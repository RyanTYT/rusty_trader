from _typeshed import Incomplete
from abc import ABCMeta, abstractmethod
from apscheduler.events import EVENT_ALL as EVENT_ALL, EVENT_ALL_JOBS_REMOVED as EVENT_ALL_JOBS_REMOVED, EVENT_EXECUTOR_ADDED as EVENT_EXECUTOR_ADDED, EVENT_EXECUTOR_REMOVED as EVENT_EXECUTOR_REMOVED, EVENT_JOBSTORE_ADDED as EVENT_JOBSTORE_ADDED, EVENT_JOBSTORE_REMOVED as EVENT_JOBSTORE_REMOVED, EVENT_JOB_ADDED as EVENT_JOB_ADDED, EVENT_JOB_MAX_INSTANCES as EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MODIFIED as EVENT_JOB_MODIFIED, EVENT_JOB_REMOVED as EVENT_JOB_REMOVED, EVENT_JOB_SUBMITTED as EVENT_JOB_SUBMITTED, EVENT_SCHEDULER_PAUSED as EVENT_SCHEDULER_PAUSED, EVENT_SCHEDULER_RESUMED as EVENT_SCHEDULER_RESUMED, EVENT_SCHEDULER_SHUTDOWN as EVENT_SCHEDULER_SHUTDOWN, EVENT_SCHEDULER_STARTED as EVENT_SCHEDULER_STARTED, JobEvent as JobEvent, JobSubmissionEvent as JobSubmissionEvent, SchedulerEvent as SchedulerEvent  # type: ignore
from apscheduler.executors.base import BaseExecutor as BaseExecutor, MaxInstancesReachedError as MaxInstancesReachedError  # type: ignore
from apscheduler.executors.pool import ThreadPoolExecutor as ThreadPoolExecutor  # type: ignore
from apscheduler.job import Job as Job  # type: ignore
from apscheduler.jobstores.base import BaseJobStore as BaseJobStore, ConflictingIdError as ConflictingIdError, JobLookupError as JobLookupError  # type: ignore
from apscheduler.jobstores.memory import MemoryJobStore as MemoryJobStore  # type: ignore
from apscheduler.schedulers import SchedulerAlreadyRunningError as SchedulerAlreadyRunningError, SchedulerNotRunningError as SchedulerNotRunningError
from apscheduler.triggers.base import BaseTrigger as BaseTrigger
from apscheduler.util import asbool as asbool, asint as asint, astimezone as astimezone, maybe_ref as maybe_ref, obj_to_ref as obj_to_ref, ref_to_obj as ref_to_obj, undefined as undefined  # type: ignore

STATE_STOPPED: int
STATE_RUNNING: int
STATE_PAUSED: int


class BaseScheduler(metaclass=ABCMeta):
    state: Incomplete
    def __init__(self, gconfig={}, **options) -> None: ...  # type: ignore
    def configure(self, gconfig={}, prefix: str = 'apscheduler.', **options) -> None: ...  # type: ignore
    def start(self, paused: bool = False) -> None: ...
    @abstractmethod
    def shutdown(self, wait: bool = True) -> None: ...
    def pause(self) -> None: ...
    def resume(self) -> None: ...
    @property
    def running(self) -> None: ...
    def add_executor(self, executor, alias: str = 'default', **executor_opts) -> None: ...  # type: ignore
    def remove_executor(self, alias, shutdown: bool = True) -> None: ...  # type: ignore
    def add_jobstore(self, jobstore, alias: str = 'default', **jobstore_opts) -> None: ...  # type: ignore
    def remove_jobstore(self, alias, shutdown: bool = True) -> None: ...  # type: ignore
    def add_listener(self, callback, mask=...) -> None: ...  # type: ignore
    def remove_listener(self, callback) -> None: ...  # type: ignore
    def add_job(self, func, trigger: Incomplete | None = None, args: Incomplete | None = None, kwargs: Incomplete | None = None, id: Incomplete | None = None, name: Incomplete | None = None, misfire_grace_time=..., coalesce=..., max_instances=..., next_run_time=..., jobstore: str = 'default', executor: str = 'default', replace_existing: bool = False, **trigger_args) -> None: ...  # type: ignore
    def scheduled_job(self, trigger, args: Incomplete | None = None, kwargs: Incomplete | None = None, id: Incomplete | None = None, name: Incomplete | None = None, misfire_grace_time=..., coalesce=..., max_instances=..., next_run_time=..., jobstore: str = 'default', executor: str = 'default', **trigger_args) -> None: ...  # type: ignore
    def modify_job(self, job_id, jobstore: Incomplete | None = None, **changes) -> None: ...  # type: ignore
    def reschedule_job(self, job_id, jobstore: Incomplete | None = None, trigger: Incomplete | None = None, **trigger_args) -> None: ...  # type: ignore
    def pause_job(self, job_id, jobstore: Incomplete | None = None) -> None: ...  # type: ignore
    def resume_job(self, job_id, jobstore: Incomplete | None = None) -> None: ...  # type: ignore
    def get_jobs(self, jobstore: Incomplete | None = None, pending: Incomplete | None = None) -> None: ...
    def get_job(self, job_id, jobstore: Incomplete | None = None): ...  # type: ignore
    def remove_job(self, job_id, jobstore: Incomplete | None = None) -> None: ...  # type: ignore
    def remove_all_jobs(self, jobstore: Incomplete | None = None) -> None: ...
    def print_jobs(self, jobstore: Incomplete | None = None, out: Incomplete | None = None) -> None: ...
    def export_jobs(self, outfile, jobstore: Incomplete | None = None) -> None: ...  # type: ignore
    def import_jobs(self, infile, jobstore: str = 'default') -> None: ...  # type: ignore
    @abstractmethod
    def wakeup(self) -> None: ...
