"""`stratlab` command-line interface.

Bash entry points for the research toolkit (all exit 0 on success, 1 on
failure/error so an LLM can branch on them):

    stratlab verify                       # causality + streaming/vec parity (20 checks)
    stratlab backtest --strategy s.py:C --universe raw_bars.parquet ... --out r.json
    stratlab resample --universe raw_bars.parquet --timeframe 15m --out bars_15m.parquet
    stratlab study new <slug> --title "..."       # scaffold a labelled run.py
    stratlab study validate <slug>                # gate the labels
    stratlab parity [--golden <path>]             # metric parity vs the Rust golden
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys

from stratlab.cost import CommissionModel
from stratlab.engine import EngineConfig, run_thorough
from stratlab.io import Timeframe, Universe


def _write_json(path: str, obj) -> None:
    with open(path, "w") as f:
        json.dump(_sanitize(obj), f, indent=2, default=str)
    print(f"wrote {path}", file=sys.stderr)


def _sanitize(obj):
    """Convert non-JSON floats (nan/inf) to strings so strict parsers accept output."""
    if isinstance(obj, float):
        if math.isfinite(obj):
            return obj
        return "nan" if math.isnan(obj) else ("inf" if obj > 0 else "-inf")
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def _load_universe(path: str) -> Universe:
    return Universe.from_parquet(path)


def _load_strategy(spec: str):
    if ":" in spec:
        path, cls = spec.rsplit(":", 1)
    else:
        path, cls = spec, "Strategy"
    spec_obj = importlib.util.spec_from_file_location("_stratlab_user_strategy", path)
    if spec_obj is None or spec_obj.loader is None:
        raise ValueError(f"cannot load strategy file: {path}")
    mod = importlib.util.module_from_spec(spec_obj)
    spec_obj.loader.exec_module(mod)
    return getattr(mod, cls)()


# ────────────────────────────────────────────────────────────────────────────
# Subcommand handlers
# ────────────────────────────────────────────────────────────────────────────
def _cmd_verify(args) -> int:
    from stratlab.primitives.verify import run_all

    results = run_all()
    n_fail = 0
    for name, ok in results.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
        n_fail += 0 if ok else 1
    print(f"\n{len(results) - n_fail}/{len(results)} passed")
    return 1 if n_fail else 0


def _cmd_backtest(args) -> int:
    uni = _load_universe(args.universe)
    strategy = _load_strategy(args.strategy)
    cfg = EngineConfig(
        starting_equity=args.start_equity,
        slippage_bps=args.slippage_bps,
        commission_model=CommissionModel.from_str(args.commission),
    )
    res = run_thorough(uni, strategy, cfg)
    r = res.results
    out = {"schema_version": "1.0", "mode": "thorough", "results": r.to_dict()}
    if args.full_stats:
        from stratlab.metrics.dynamic import format_forward, forward_bps
        from stratlab.metrics.full import close_trades, full_report

        report = full_report(uni, res.state.fills, equity_curve=res.results.equity_curve)
        trades = close_trades(res.state.fills)
        entries = [(t.entry_time, t.symbol, t.side) for t in trades]
        out["full_stats"] = {
            "trade_summary": report["trade_summary"],
            "portfolio_risk": report["portfolio_risk"],
        }
        out["dynamic"] = forward_bps(entries, uni)
        print(format_forward(out["dynamic"]), file=sys.stderr)
    _write_json(args.out, out)
    print(
        f"trades={r.num_trades} closed={r.num_closed_trades} win={r.win_rate_pct:.2f}% "
        f"sharpe={r.sharpe:.4f} sortino={r.sortino:.4f} max_dd={r.max_drawdown_pct:.2f}% "
        f"final_equity={r.final_equity:.2f}"
    )
    return 0


def _cmd_resample(args) -> int:
    uni = _load_universe(args.universe)
    tf = Timeframe.parse(args.timeframe)
    coarse = uni.resample(tf)
    coarse.long.to_parquet(args.out, index=False)
    print(f"{tf} -> {args.out} ({coarse.summary()})")
    return 0


def _cmd_study(args) -> int:
    if args.study_command == "new":
        from stratlab.study import scaffold
        path = scaffold(args.slug, args.title)
        print(path)
        return 0
    if args.study_command == "validate":
        from stratlab.study import markdown_from_runpy, validate
        with open(args.path) as f:
            src = f.read()
        problems = validate(markdown_from_runpy(src))
        if problems:
            for p in problems:
                print(f"FAIL  {p}")
            return 1
        print("PASS  study labels valid")
        return 0
    if args.study_command == "render":
        from stratlab.study.render import render_runpy
        out = render_runpy(args.path, args.out)
        print(out)
        return 0
    return 1


def _cmd_parity(args) -> int:
    from stratlab.parity import RustGolden, default_golden_path, metric_parity_problems

    path = args.golden or default_golden_path()
    golden = RustGolden.load(path)
    problems = metric_parity_problems(golden, rtol=1e-6, atol=1e-6)
    if problems:
        for p in problems:
            print(f"FAIL  {p}")
        return 1
    print(f"PASS  metric parity ({golden.sharpe:.4f} sharpe, {golden.max_drawdown_pct:.2f}% maxdd)")
    return 0


# ────────────────────────────────────────────────────────────────────────────
# Parser
# ────────────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="stratlab", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("verify", help="run the causality + streaming/vectorized parity harness")

    bt = sub.add_parser("backtest", help="run a strategy through the thorough engine")
    bt.add_argument("--strategy", required=True, help="path.py[:ClassName] with on_row(bars, ctx)")
    bt.add_argument("--universe", required=True, help="raw_bars.parquet")
    bt.add_argument("--start-equity", type=float, default=100_000.0)
    bt.add_argument("--slippage-bps", type=float, default=0.0)
    bt.add_argument("--commission", choices=["fixed", "tiered"], default="tiered")
    bt.add_argument("--out", default="backtest_result.json")
    bt.add_argument("--full-stats", action="store_true",
                    help="also emit trade_summary, portfolio_risk, and dynamic forward-bps curves")

    rs = sub.add_parser("resample", help="aggregate raw_bars to a coarser Timeframe")
    rs.add_argument("--universe", required=True)
    rs.add_argument("--timeframe", required=True, help="15m | 30m | 1h | 1d | 1w | 1mo | 1y")
    rs.add_argument("--out", required=True)

    st = sub.add_parser("study", help="scaffold/validate a research study")
    st_sub = st.add_subparsers(dest="study_command", required=True)
    st_new = st_sub.add_parser("new", help="scaffold a labelled study")
    st_new.add_argument("slug")
    st_new.add_argument("--title", default=None)
    st_val = st_sub.add_parser("validate", help="gate the study's labels")
    st_val.add_argument("path", help="path to run.py")
    st_render = st_sub.add_parser("render", help="render run.py to a .ipynb notebook")
    st_render.add_argument("path", help="path to run.py")
    st_render.add_argument("--out", default=None, help="output .ipynb (default: run.ipynb)")

    pa = sub.add_parser("parity", help="metric parity vs the Rust golden")
    pa.add_argument("--golden", default=None, help="path to backtest_results_*.json")

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "verify":
            return _cmd_verify(args)
        if args.command == "backtest":
            return _cmd_backtest(args)
        if args.command == "resample":
            return _cmd_resample(args)
        if args.command == "study":
            return _cmd_study(args)
        if args.command == "parity":
            return _cmd_parity(args)
    except Exception as e:  # noqa: BLE001 — CLI boundary: report and exit non-zero
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())