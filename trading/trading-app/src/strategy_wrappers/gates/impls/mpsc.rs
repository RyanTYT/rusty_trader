use std::cell::UnsafeCell;
use std::{
    marker::PhantomPinned,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU32, Ordering},
    },
};

unsafe impl<T: Send, const CAPACITY: usize, const NUM_PRODUCERS: usize> Send
    for MpscRingBuffer<T, CAPACITY, NUM_PRODUCERS>
{
}
unsafe impl<T: Send, const CAPACITY: usize, const NUM_PRODUCERS: usize> Sync
    for MpscRingBuffer<T, CAPACITY, NUM_PRODUCERS>
{
}
unsafe impl<T: Send, const CAPACITY: usize, const NUM_PRODUCERS: usize> Send
    for MpscRingBufferProducer<T, CAPACITY, NUM_PRODUCERS>
{
}

// Align to 64 bytes to protect against 64B prefetching & 64B cache lines - on i7
#[repr(align(128))]
pub struct CacheAligned<T>(pub T);

impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Add this implementation to allow mutable borrowing
impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl CacheAligned<UnsafeCell<AtomicU32>> {
    #[inline]
    pub fn load(&self, o: Ordering) -> u32 {
        unsafe { (*self.0.get()).load(o) }
    }
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        unsafe { (*self.0.get()).compare_exchange_weak(current, new, success, failure) }
    }
}
impl<T> CacheAligned<UnsafeCell<MaybeUninit<T>>> {
    #[inline]
    pub fn write(&self, value: T) {
        unsafe { (*self.0.get()).write(value) };
    }
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        unsafe { (*self.0.get()).as_ptr() }
    }
}

pub struct MpscRingBuffer<T, const CAPACITY: usize, const NUM_PRODUCERS: usize> {
    participation_buffer: [CacheAligned<UnsafeCell<AtomicU32>>; CAPACITY],
    time_idx_buffer: [CacheAligned<UnsafeCell<AtomicU32>>; CAPACITY],
    // data_buffer: [CacheAligned<MaybeUninit<T>>; CAPACITY],
    data_buffer: [[CacheAligned<UnsafeCell<MaybeUninit<T>>>; NUM_PRODUCERS]; CAPACITY],
    last_producer_idx: AtomicI32,

    // Occupancy of the current run: bits of participating producer indices.
    // Defaults to full; `set_active_producers` narrows it for smaller runs
    // (2/3-sleeve fund runs over a NUM_PRODUCERS-capacity ring).
    active_mask: AtomicU32,

    _pin: PhantomPinned,
}

// NOTE: no `T: Clone` bound — `push` takes `T` by value, the completing
// aggregate `ptr::read`s the other producers' cells (moves), and nothing
// ever clones a payload.
impl<T, const CAPACITY: usize, const NUM_PRODUCERS: usize>
    MpscRingBuffer<T, CAPACITY, NUM_PRODUCERS>
{
    const CHECK: () = assert!(
        CAPACITY.is_power_of_two(),
        "SpmcRingBuffer CAPACITY must be a power of two"
    );

    pub fn new() -> Self {
        let () = Self::CHECK;
        Self {
            participation_buffer: [const { CacheAligned(UnsafeCell::new(AtomicU32::new(0))) };
                CAPACITY],
            time_idx_buffer: [const { CacheAligned(UnsafeCell::new(AtomicU32::new(0))) }; CAPACITY],
            data_buffer: core::array::from_fn(|_| {
                core::array::from_fn(|_| CacheAligned(UnsafeCell::new(MaybeUninit::uninit())))
            }),
            // data_buffer: [[const { CacheAligned(MaybeUninit::uninit()) }; NUM_PRODUCERS]; CAPACITY],
            last_producer_idx: AtomicI32::new(-1),
            active_mask: AtomicU32::new((1 << NUM_PRODUCERS) - 1),
            _pin: PhantomPinned,
        }
    }

    /// Declare which producer indices participate in this run. Panics on any
    /// idx >= NUM_PRODUCERS. Completion and the aggregate walk are bound by
    /// the resulting mask: producers never named here are never expected to
    /// push and their payload cells are never read (2/3-sleeve runs stay
    /// well-defined over a NUM_PRODUCERS-capacity ring).
    pub fn set_active_producers(&self, producers: &[usize]) {
        let mut mask = 0u32;
        for &idx in producers {
            assert!(
                idx < NUM_PRODUCERS,
                "producer idx {idx} out of bounds: NUM_PRODUCERS={NUM_PRODUCERS}"
            );
            mask |= 1 << idx;
        }
        self.active_mask.store(mask, Ordering::Release);
    }

    pub fn get_new_producer(
        self: &Arc<Self>,
    ) -> MpscRingBufferProducer<T, CAPACITY, NUM_PRODUCERS> {
        let mut last_producer_idx = self
            .last_producer_idx
            .load(std::sync::atomic::Ordering::Acquire);

        if last_producer_idx == NUM_PRODUCERS as i32 - 1 {
            panic!("Last producer idx used already! Too many producers for MpscRingBuffer!!");
        }

        let producer_idx: usize = {
            while let Err(new_last_producer_idx) = self.last_producer_idx.compare_exchange_weak(
                last_producer_idx,
                last_producer_idx + 1,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                if new_last_producer_idx == NUM_PRODUCERS as i32 - 1 {
                    panic!(
                        "Last producer idx used already! Too many producers for MpscRingBuffer!!"
                    );
                }
                last_producer_idx = new_last_producer_idx;
            }
            (last_producer_idx + 1) as usize
        };

        MpscRingBufferProducer {
            participation_buffer: self.participation_buffer.as_ptr()
                as *mut CacheAligned<UnsafeCell<AtomicU32>>,
            time_idx_buffer: self.time_idx_buffer.as_ptr()
                as *mut CacheAligned<UnsafeCell<AtomicU32>>,
            data_buffer: self.data_buffer.as_ptr() as *mut CacheAligned<UnsafeCell<MaybeUninit<T>>>,
            // num_producers: NUM_PRODUCERS,
            producer_idx,
            producer_mask: 1 << producer_idx,

            buf: Arc::clone(self),
        }
    }
}

pub struct MpscRingBufferProducer<T, const CAPACITY: usize, const NUM_PRODUCERS: usize> {
    participation_buffer: *mut CacheAligned<UnsafeCell<AtomicU32>>,
    time_idx_buffer: *mut CacheAligned<UnsafeCell<AtomicU32>>,
    data_buffer: *mut CacheAligned<UnsafeCell<MaybeUninit<T>>>,
    // num_producers: usize,
    producer_idx: usize,

    // bitmasks
    producer_mask: u32,

    buf: Arc<MpscRingBuffer<T, CAPACITY, NUM_PRODUCERS>>,
}

impl<T, const CAPACITY: usize, const NUM_PRODUCERS: usize>
    MpscRingBufferProducer<T, CAPACITY, NUM_PRODUCERS>
{
    pub fn get_producer_idx(&self) -> usize {
        self.producer_idx
    }

    /// Push one payload into the slot for `bucket`.
    ///
    /// `bucket` selects the slot position directly: `position = bucket & (CAPACITY - 1)`.
    /// The slot's era key is `(days_from_year_start << 16) | bucket`.
    ///
    /// Example (5-min-bar fund session): minutes-since-open are 0, 5, …, 385,
    /// so `bucket = minutes_since_open / 5` spans 0..=77 (< CAPACITY 128,
    /// injective — one flush per bucket). mso = 90 → bucket = 18. Callers
    /// must never map two different eras onto one bucket position.
    pub fn push(
        &self,
        value: T,
        bucket: u32,
        days_from_year_start: u32,
    ) -> Option<Vec<T>> {
        let participation_idx = bucket & (CAPACITY as u32 - 1);
        let participation_ptr =
            unsafe { self.participation_buffer.add(participation_idx as usize) };
        let mut participation_val =
            unsafe { (*participation_ptr).load(std::sync::atomic::Ordering::Acquire) };
        let time_ptr = unsafe { self.time_idx_buffer.add(participation_idx as usize) };
        let time_val = unsafe { (*time_ptr).load(std::sync::atomic::Ordering::Acquire) };

        // Update Time Value First
        if (days_from_year_start << 16 | bucket) != time_val {
            if participation_val != 0 {
                tracing::error!(
                    "Trying to push value with new timestamp into old slot - i.e. slot consumption is too slow - some producer is lagging!"
                );
                while let Err(_) = unsafe { &*participation_ptr }.compare_exchange_weak(
                    participation_val,
                    0,
                    std::sync::atomic::Ordering::Release,
                    std::sync::atomic::Ordering::Relaxed,
                ) {}
                participation_val = 0;
            }
            let mut actual_current_time_val = time_val;
            while let Err(actual_current_time_val_now) = unsafe { &*time_ptr }
                .compare_exchange_weak(
                    actual_current_time_val,
                    days_from_year_start << 16 | bucket,
                    std::sync::atomic::Ordering::Release,
                    std::sync::atomic::Ordering::Relaxed,
                )
            {
                actual_current_time_val = actual_current_time_val_now;
                if actual_current_time_val_now != actual_current_time_val {
                    tracing::error!(
                        "Time value of MpscRingBuffer was updated while trying to update it"
                    )
                }
            }
        }

        // Update data buffer - returning vec of T's if filled
        let new_participation_val = participation_val | self.producer_mask;
        let active_mask = self.buf.active_mask.load(Ordering::Acquire);
        if new_participation_val & active_mask == active_mask {
            let mut data = Vec::with_capacity(NUM_PRODUCERS);
            // wrapped in Optional ONLY to let compiler know
            // that value is consumed only at one point in the loop
            // - thus, unwrap() directly will NEVER panic
            let mut opt_value = Some(value);
            // walk only the ACTIVE producers' bits: cells of producers
            // never named in set_active_producers are never read.
            let mut bits = active_mask;
            while bits != 0 {
                let idx = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                if idx == self.producer_idx {
                    data.push(opt_value.take().unwrap());
                    continue;
                }
                // let data_idx = (participation_idx as usize * NUM_PRODUCERS + idx) & (CAPACITY - 1);
                let data_idx = participation_idx as usize * NUM_PRODUCERS + idx;
                let this_idx_val =
                    unsafe { std::ptr::read((*self.data_buffer.add(data_idx)).as_ptr()) };
                data.push(this_idx_val);
            }
            // Update participation_val to 0 after consumption
            let mut prev_val = participation_val;
            while let Err(new_prev_val) = unsafe { &*participation_ptr }.compare_exchange_weak(
                prev_val,
                0,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                prev_val = new_prev_val;
            }
            return Some(data);
        }
        let data_idx =
            // (participation_idx as usize * NUM_PRODUCERS + self.producer_idx) & (CAPACITY - 1);
            participation_idx as usize * NUM_PRODUCERS + self.producer_idx;
        unsafe { (*self.data_buffer.add(data_idx)).write(value) };

        // Update participation_val
        let mut participation_val = participation_val;
        while let Err(current_participation_value) = unsafe { &*participation_ptr }
            .compare_exchange_weak(
                participation_val,
                new_participation_val,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Relaxed,
            )
        {
            participation_val = current_participation_value | self.producer_mask;
        }

        None
    }
}
