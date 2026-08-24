/// Downsizes the portfolio of positions proportionately - s.t. proportion of positions
/// relative to one another stays roughly the same (minimal changes)
/// NOTE: cannot be used when direction of positions are non-uniform
///
/// Proportionally reduce integer holdings to fit total exposure <= T using exact DP.
/// Preserves signs, never increases |h_i|, and returns the largest integer portfolio
/// whose exposure is <= T (maximizes exposure subject to the constraint).
///
/// quantities: signed integer holdings (i64)
/// prices: per-unit exposures (f64) -- must be > 0
/// target_exposure: target total exposure (f64)
///
/// Returns (new_holdings, total_exposure).
pub fn proportional_integer_reduce(
    quantities: &Vec<i64>,
    prices: &Vec<f64>,
    target_exposure: f64,
) -> (Vec<i64>, f64) {
    assert_eq!(
        prices.len(),
        quantities.len(),
        "h and u must have same length"
    );
    let eps = 1e-12_f64;

    let curr_exposure = quantities
        .iter()
        .zip(prices.iter())
        .map(|(qty, price)| (qty.abs() as f64) * price)
        .sum();
    if curr_exposure <= target_exposure + eps {
        return (quantities.clone(), curr_exposure); // already below target
    }

    let scale_factor = target_exposure / curr_exposure;

    // 1) Scale to target exposure directly first
    let integer_scaled_qtys = quantities
        .iter()
        .map(|qty| {
            let new_qty = ((*qty as f64) * scale_factor).trunc() as i64;
            if new_qty.abs() > qty.abs() {
                return *qty;
            }
            new_qty
        })
        .collect::<Vec<i64>>();
    let new_exposure = integer_scaled_qtys
        .iter()
        .zip(prices.iter())
        .map(|(qty, price)| (qty.abs() as f64) * price)
        .sum();

    if target_exposure - new_exposure <= eps {
        return (integer_scaled_qtys, new_exposure);
    }

    // 2) build candidates: each candidate = one unit of a position (pos index, cost u[pos])
    //    but never allow adding more than original |h_i|
    let mut candidates = quantities
        .iter()
        .zip(integer_scaled_qtys.iter())
        .zip(prices.iter())
        .enumerate()
        .flat_map(|(idx, ((orig_qty, new_qty), price))| {
            let max_add = (orig_qty.abs() - new_qty.abs()) as usize;
            if *price < eps {
                return vec![];
            }
            vec![(idx, *price); max_add]
        })
        .collect::<Vec<(usize, f64)>>();

    if candidates.is_empty() {
        // Nothing else we can add
        return (integer_scaled_qtys, new_exposure);
    }

    // ================================ GREEDY =======================================
    // fallback to greedy fill by real cost
    let mut caps = target_exposure - new_exposure;
    let mut final_qtys = integer_scaled_qtys;
    // sort candidates ascending by cost to fill smaller prices first
    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    for (pos, cost) in candidates {
        if cost <= caps + eps {
            if quantities[pos] >= 0 {
                final_qtys[pos] += 1;
            } else {
                final_qtys[pos] -= 1;
            }
            caps -= cost;
            continue;
        }
        break;
    }
    let final_exposure = final_qtys
        .iter()
        .zip(prices.iter())
        .map(|(qty, price)| (qty.abs() as f64) * price)
        .sum();
    return (final_qtys, final_exposure);
    // ================================ GREEDY =======================================
}
