"""WSL 中执行 v2 buffer 扫描、交叉核验与 HTML 报告生成。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from simulator import HERE, fixed_trace_requirement, simulate, v1


def compact(result):
    return {k: v for k, v in result.items() if k not in ("records", "segments", "leases", "memory_events")}


def timing_equal(a, b):
    return all((x["enqueue"], x["start"], x["end"]) == (y["enqueue"], y["start"], y["end"])
               for x, y in zip(a, b)) and len(a) == len(b)


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "config.json")
    parser.add_argument("--output", type=Path, default=HERE / "out")
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--buffers", type=int, help="展示指定容量；仍扫描全部更小容量")
    parser.add_argument("--long-iterations", type=int, default=300)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.iterations is not None:
        config["iterations"] = args.iterations
    if config["policy"] != "ooo":
        raise ValueError("最小 buffer 搜索以 ooo 为主策略；FIFO 会自动做对照")
    if args.buffers is not None:
        v1.positive_int(args.buffers, "buffers")
    v1.positive_int(args.long_iterations, "long_iterations")
    output = args.output
    output.mkdir(parents=True, exist_ok=True)

    baseline = v1.simulate(config)
    target = baseline["metrics"]["steady"]["ii"] if baseline["metrics"]["steady"] else baseline["metrics"]["ii_lower_bound"]
    fixed = fixed_trace_requirement(baseline["records"], config.get("count_source_inputs", True))
    upper = fixed["minimum_for_fixed_trace"]
    all_runs = {}
    scan = []
    for count in range(1, max(upper + 1, args.buffers or 1) + 1):
        run = simulate(config, count)
        steady = run["metrics"]["steady"]
        qualifies = run["status"] == "complete" and steady is not None and steady["ii"] <= target
        exact = run["status"] == "complete" and timing_equal(run["records"], baseline["records"])
        row = {**compact(run), "meets_target": qualifies, "same_as_v1_timing": exact}
        scan.append(row)
        all_runs[count] = run
        print(json.dumps({"buffers": count, "status": run["status"], "completed": run["completed_iterations"],
                          "makespan": run["makespan"], "ii": steady["ii"] if steady else None,
                          "peak": run["peak_buffers"], "target_met": qualifies, "same_as_v1": exact}, ensure_ascii=False), flush=True)
    passing = [row["buffer_count"] for row in scan if row["meets_target"]]
    if not passing:
        save_json(output / "sweep.json", scan)
        raise RuntimeError("扫描范围内没有确认达到目标稳态，已保存真实结果")
    minimum = min(passing)
    chosen = args.buffers or minimum
    selected = all_runs[chosen]
    exact = all_runs[upper]
    if not timing_equal(exact["records"], baseline["records"]):
        raise AssertionError("固定时序峰值容量没有复现 v1，需审计边界")
    fifo = simulate({**config, "policy": "fifo"}, chosen)
    long_n = max(config["iterations"], args.long_iterations)
    long_runs = []
    for count in sorted({max(1, minimum - 1), minimum, upper}):
        result = simulate({**config, "iterations": long_n}, count)
        short = all_runs[count]
        a, b = short["metrics"]["steady"], result["metrics"]["steady"]
        consistent = bool(a and b and a["ii"] == b["ii"] and result["status"] == short["status"])
        if not consistent:
            raise AssertionError(f"容量 {count} 在长运行中没有复现稳态")
        long_runs.append({"buffers": count, "iterations": long_n, "status": result["status"],
                          "makespan": result["makespan"], "steady": result["steady"],
                          "ii": b["ii"], "consistent": consistent})

    # 固定 v1 时序的最小数来自同时存活区间峰值，而非只试几个容量。
    steady_fixed = None
    if baseline["steady"]:
        steady_fixed = fixed_trace_requirement(baseline["records"], config.get("count_source_inputs", True),
                                              baseline["steady"]["measurement_start"],
                                              baseline["steady"]["measurement_end"])
    report = {"target_ii": target, "minimum_under_current_policy": minimum,
              "minimum_for_exact_v1_timing": upper, "selected_count": chosen,
              "fixed_trace_proof": {k: v for k, v in fixed.items() if k != "intervals"},
              "fixed_steady_requirement": steady_fixed["minimum_for_fixed_trace"] if steady_fixed else None,
              "scope": "逐个容量扫描的当前最老就绪优先策略最小值，不是所有可能调度算法的全局最优证明",
              "baseline_metrics": baseline["metrics"], "sweep": scan, "long_run_checks": long_runs}
    save_json(output / "summary.json", report)
    save_json(output / "sweep.json", scan)
    save_json(output / "effective_config.json", config)
    for name, run in [("ooo", selected), ("fifo", fifo), ("exact_v1", exact)]:
        save_json(output / f"trace_{name}.json", run)
    with (output / "buffer_lifetimes.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=("slot", "generation", "owner", "kind", "start", "end", "consumers"))
        writer.writeheader()
        writer.writerows({**l, "consumers": " ".join(map(str, l["consumers"]))} for l in selected["leases"])
    with (output / "memory_events.jsonl").open("w", encoding="utf-8") as f:
        for event in selected["memory_events"]:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    verification = {"schedule_and_memory_invariants": "PASS", "scanned_counts": len(scan),
                    "long_run_checks": long_runs, "baseline_files_unchanged": {}}
    manifest = json.loads((HERE / "baseline_manifest.json").read_text())
    for filename, expected in manifest["files"].items():
        actual = hashlib.sha256((HERE.parent / filename).read_bytes()).hexdigest()
        verification["baseline_files_unchanged"][filename] = actual == expected
    if not all(verification["baseline_files_unchanged"].values()):
        raise AssertionError("v1 基线文件发生变化，需重新核对")
    save_json(output / "verification.json", verification)
    packed = {**report, "runs": {name: {k: v for k, v in run.items() if k != "memory_events"}
                                  for name, run in [("ooo", selected), ("fifo", fifo), ("exact_v1", exact)]}}
    template = (HERE / "report_template.html").read_text(encoding="utf-8")
    inline = json.dumps(packed, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    (output / "report.html").write_text(template.replace("__DATA__", inline), encoding="utf-8")
    print(json.dumps({"output": str(output), "minimum_for_throughput": minimum,
                      "minimum_for_exact_v1": upper, "target_ii": target, "verification": "PASS"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
