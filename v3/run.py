"""逐个扫描 full/empty 共享 ID 容量，保存动态指令、事件和报告。"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from simulator import HERE, simulate, v2


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def same_timing(a, b):
    return len(a) == len(b) and all((x["enqueue"], x["start"], x["end"]) ==
                                  (y["enqueue"], y["start"], y["end"]) for x, y in zip(a, b))


def row_for(run, baseline):
    s, b = run["metrics"]["steady"], baseline["metrics"]["steady"]
    return {"buffers": run["buffer_count"], "ids": run["barrier_count"], "policy": run["config"]["policy"],
            "status": run["status"], "completed_iterations": run["completed_iterations"],
            "time": run["time"], "makespan": run["makespan"], "ii": s["ii"] if s else None,
            "baseline_ii": b["ii"] if b else None,
            "efficiency": s["bound_efficiency"] if s else None,
            "completion_stall_time": run["metrics"]["completion_stall_time"],
            "peak_ids": run["barrier_stats"]["peak_live"],
            "peak_full_ids": run["barrier_stats"]["peak_full_live"],
            "same_as_v2": run["status"] == "complete" and same_timing(run["records"], baseline["records"]),
            "steady": run["steady"],
            "meets_baseline": bool(run["status"] == "complete" and s and b and s["ii"] <= b["ii"])}


def peak_witness(run):
    live = {}
    best = {"count": 0, "event_seq": None, "time": None, "full": 0, "empty": 0, "live": []}
    for e in run["barrier_events"]:
        if e["event"] == "allocate":
            live[e["slot"]] = e
        elif e["event"] == "free":
            del live[e["slot"]]
        if len(live) > best["count"]:
            best = {"count": len(live), "event_seq": e["seq"], "time": e["time"],
                    "full": sum(x["kind"] == "full" for x in live.values()),
                    "empty": sum(x["kind"] == "empty" for x in live.values()),
                    "live": [{k: x[k] for k in ("slot", "generation", "kind", "owner", "mask")} for x in live.values()]}
    return best


def mask(refs):
    value = 0
    for r in refs:
        value |= r["mask"]
    return value


def export_program(run, path):
    bits = run["barrier_count"]
    def bitstring(refs):
        return "0b" + format(mask(refs), f"0{bits}b")
    lines = [f"# 实际运行绑定记录：{run['buffer_count']} buffers / {bits} one-hot IDs / {run['config']['policy']}",
             "# 每条ID独占一位；多条WAIT为OR mask。ID在完成时动态分配，以下是执行轨迹，不是预先固定的静态指令编码。",
             "# WAIT在真正开始时消费并回收；GET_AND_SET在成功完成时分配并置位。"]
    for r in run["records"]:
        if r["start"] is None:
            lines.append(f"i{r['iteration']}/{r['op']} : 尚未开始")
            continue
        waits = []
        sets = []
        if r["wait_full"]:
            waits.append("WAIT_FULL " + bitstring(r["wait_full"]))
        if r["wait_empty"]:
            waits.append("WAIT_EMPTY " + bitstring(r["wait_empty"]))
        if r["set_full"]:
            sets.append("GET_AND_SET_FULL " + bitstring(r["set_full"]))
        if r["set_empty"]:
            sets.append("GET_AND_SET_EMPTY " + bitstring(r["set_empty"]))
        lines.append(f"i{r['iteration']}: [{'; '.join(waits)}, {r['op']}, {'; '.join(sets) or '未发布'}] "
                     f"start={r['start']} compute_end={r['compute_end']} end={r['end']}")
        for name in ("wait_full", "wait_empty", "set_full", "set_empty"):
            if r[name]:
                lines.append("  " + name + ": " + ", ".join(f"ID{x['slot']}:g{x['generation']} owner={x['owner']}" for x in r[name]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def safe_html_data(value):
    # JS Number 无法精确保存任意宽one-hot整数；HTML把mask存hex并用BigInt读取。
    if isinstance(value, dict):
        return {k: hex(v) if k == "mask" and isinstance(v, int) else safe_html_data(v) for k, v in value.items()}
    if isinstance(value, list):
        return [safe_html_data(v) for v in value]
    return value


def render_report(output):
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    runs = {key: json.loads((output / f"trace_{key}.json").read_text(encoding="utf-8")) for key in summary["display_runs"]}
    for r in runs.values():
        r.pop("memory_events", None)
    packed = safe_html_data({**summary, "runs": runs})
    data = json.dumps(packed, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    template = (HERE / "report_template.html").read_text(encoding="utf-8")
    (output / "report.html").write_text(template.replace("__DATA__", data), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "config.json")
    parser.add_argument("--output", type=Path, default=HERE / "out")
    parser.add_argument("--buffers", type=int, nargs="+", default=[9, 11])
    parser.add_argument("--max-ids", type=int, help="默认使用2×最大buffer数量的足够容量上界")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--long-iterations", type=int, default=300)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.render_only:
        render_report(args.output)
        return
    config = json.loads(args.config.read_text(encoding="utf-8"))
    config["iterations"] = args.iterations
    for n in args.buffers:
        v2.v1.positive_int(n, "buffers")
    v2.v1.positive_int(args.long_iterations, "long_iterations")
    maximum = args.max_ids or max(args.buffers) * max(1, max(map(len, v2.consumers_for(v2.v1.instruction_records({**config, "iterations": 1})).values())))
    v2.v1.positive_int(maximum, "max_ids")
    rows, minima, display, long_checks, peaks = [], [], {}, [], {}
    for buffers in sorted(set(args.buffers)):
        for policy in ("ooo", "fifo"):
            cfg = {**config, "policy": policy}
            baseline = v2.simulate(cfg, buffers)
            candidates = []
            last_failed = None
            first_pass = None
            for count in range(1, maximum + 1):
                run = simulate(cfg, buffers, count)
                row = row_for(run, baseline)
                rows.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)
                if row["meets_baseline"]:
                    candidates.append(count)
                    if first_pass is None:
                        first_pass = run
                elif first_pass is None:
                    last_failed = run
            minimum = min(candidates) if candidates else None
            minima.append({"buffers": buffers, "policy": policy, "minimum_ids": minimum,
                           "baseline_ii": baseline["metrics"]["steady"]["ii"] if baseline["metrics"]["steady"] else None})
            if first_pass:
                key = f"b{buffers}_m{minimum}_{policy}"
                display[key] = first_pass
                peaks[key] = peak_witness(first_pass)
                if policy == "ooo" and last_failed:
                    display[f"b{buffers}_m{last_failed['barrier_count']}_{policy}"] = last_failed
                for count in sorted({max(1, minimum - 1), minimum}):
                    cfg_long = {**cfg, "iterations": max(args.long_iterations, args.iterations)}
                    long_run = simulate(cfg_long, buffers, count)
                    s = long_run["metrics"]["steady"]
                    short = next(r for r in rows if (r["buffers"], r["ids"], r["policy"]) == (buffers, count, policy))
                    if long_run["status"] != short["status"] or (s["ii"] if s else None) != short["ii"]:
                        raise AssertionError("长运行未复现容量阈值")
                    if count == minimum:
                        base_long = v2.simulate(cfg_long, buffers)
                        assert same_timing(long_run["records"], base_long["records"]), "达标ID数量没有复现v2逐条时序"
                    long_checks.append({"buffers": buffers, "ids": count, "policy": policy,
                                        "iterations": cfg_long["iterations"], "status": long_run["status"],
                                        "makespan": long_run["makespan"], "time": long_run["time"],
                                        "completed_iterations": long_run["completed_iterations"],
                                        "ii": s["ii"] if s else None, "consistent": True})
    if not display:
        write_json(args.output / "sweep.json", rows)
        raise RuntimeError("扫描范围没有发现达到基线的配置；已保存结果，请增加max-ids或更换buffer数量")
    manifest = json.loads((HERE / "baseline_manifest.json").read_text(encoding="utf-8"))
    unchanged = {f: hashlib.sha256((HERE.parent / f).read_bytes()).hexdigest() == h for f, h in manifest["files"].items()}
    assert all(unchanged.values()), "v1/v2基线文件发生变化"
    default = next(k for k in display if k.endswith("ooo") and display[k]["status"] == "complete")
    summary = {"config": config, "dependency_edges_per_iteration": sum(len(o["deps"]) for o in config["operations"]),
               "scan_max_ids": maximum, "sweep": rows, "minima": minima,
               "default_run": default, "display_runs": list(display), "peaks": peaks,
               "long_checks": long_checks, "baseline_files_unchanged": unchanged,
               "scope": "当前动态分配、整批原子发布、初始empty及最老就绪优先规则下的容量阈值"}
    write_json(args.output / "summary.json", summary)
    write_json(args.output / "sweep.json", rows)
    write_json(args.output / "effective_config.json", config)
    for key, run in display.items():
        write_json(args.output / f"trace_{key}.json", run)
        if run["config"]["policy"] == "ooo":
            short_key = key.removesuffix("_ooo")
            export_program(run, args.output / f"instructions_{short_key}.txt")
            with (args.output / f"events_{short_key}.jsonl").open("w", encoding="utf-8") as f:
                for event in run["barrier_events"]:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
    render_report(args.output)
    print(json.dumps({"output": str(args.output), "minima": minima, "scan_cases": len(rows),
                      "long_checks": len(long_checks), "verification": "PASS"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
