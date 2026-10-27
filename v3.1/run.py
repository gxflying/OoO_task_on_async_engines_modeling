"""二维资源扫描、Pareto 边界、长运行及独立 HTML 报告。"""
import argparse
import csv
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
from pathlib import Path

from simulator import HERE, simulate, v1


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def summarize(r):
    s = r["metrics"]["steady"]
    d = r["deadlock"]
    return {"buffers": r["buffer_count"], "barriers": r["barrier_count"], "policy": r["config"]["policy"],
            "status": r["status"], "makespan": r["makespan"], "time": r["time"],
            "completed_iterations": r["completed_iterations"], "ii": s["ii"] if s else None,
            "efficiency": s["efficiency"] if s else None,
            "buffer_peak": r["buffer_peak"], "barrier_peak": r["barrier_peak"],
            "steady": r["steady"], "frontend_wait": s["frontend_wait"] if s else None,
            "deadlock": None if not d else {"frontend": d["frontend"],
                                           "free_buffers": d["buffer_pool"]["free_bitmap"].bit_count(),
                                           "free_barriers": d["barrier_pool"]["free_bitmap"].bit_count(),
                                           "pending_reclaimers": d["reclaimers"]}}


def case(task):
    config, b, m, policy = task
    return summarize(simulate({**config, "policy": policy}, b, m))


def frontier(rows, target):
    good = [x for x in rows if x["status"] == "complete" and x["ii"] is not None and x["ii"] <= target + 1e-9]
    return sorted([x for x in good if not any(y["buffers"] <= x["buffers"] and y["barriers"] <= x["barriers"]
                                             and (y["buffers"], y["barriers"]) != (x["buffers"], x["barriers"]) for y in good)],
                  key=lambda x: (x["buffers"], x["barriers"]))


def safe_data(value):
    if isinstance(value, dict):
        return {k: hex(v) if k in ("mask", "free_bitmap", "flag_bitmap") and isinstance(v, int)
                else safe_data(v) for k, v in value.items()}
    if isinstance(value, list):
        return [safe_data(v) for v in value]
    return value


def render(output):
    data = json.loads((output / "summary.json").read_text())
    data["runs"] = {key: json.loads((output / f"trace_{key}.json").read_text()) for key in data["display_runs"]}
    for run in data["runs"].values():
        run.pop("api_events", None)
        run.pop("buffer_events", None)
        run.pop("barrier_events", None)
    packed = json.dumps(safe_data(data), ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    (output / "report.html").write_text((HERE / "report_template.html").read_text().replace("__DATA__", packed), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "config.json")
    parser.add_argument("--output", type=Path, default=HERE / "out")
    parser.add_argument("--max-buffers", type=int, default=24)
    parser.add_argument("--max-barriers", type=int, default=24)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--long-iterations", type=int, default=300)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.render_only:
        render(args.output)
        d = json.loads((args.output / "summary.json").read_text())
        print(json.dumps({"mode": "render_only", "output": str(args.output), "cases": len(d["sweep"]),
                          "long_checks": len(d["long_checks"]), "report": "PASS"}, ensure_ascii=False))
        return
    for name in ("max_buffers", "max_barriers", "iterations", "long_iterations", "workers"):
        v1.positive_int(getattr(args, name), name)
    config = json.loads(args.config.read_text())
    config["iterations"] = args.iterations
    references = {}
    for policy in ("ooo", "fifo"):
        run = simulate({**config, "policy": policy}, 128, 128)
        base = v1.simulate({**config, "policy": policy})
        assert [(r["start"], r["end"]) for r in run["records"]] == [(r["start"], r["end"]) for r in base["records"]]
        references[policy] = summarize(run)
    tasks = [(config, b, m, policy) for policy in ("ooo", "fifo") for b in range(1, args.max_buffers + 1)
             for m in range(1, args.max_barriers + 1)]
    rows = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for index, row in enumerate(executor.map(case, tasks, chunksize=2), 1):
            rows.append(row)
            if index % args.max_barriers == 0:
                print(json.dumps({"completed_cases": index, "total_cases": len(tasks), "policy": row["policy"],
                                  "buffer_row_finished": row["buffers"]}), flush=True)
    save(args.output / "sweep.json", rows)
    fields = ("buffers", "barriers", "policy", "status", "completed_iterations", "makespan", "ii",
              "efficiency", "buffer_peak", "barrier_peak")
    with (args.output / "performance.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    targets = {p: references[p]["ii"] for p in references}
    pareto = {p: frontier([r for r in rows if r["policy"] == p], targets[p]) for p in targets}
    if not pareto["ooo"]:
        raise RuntimeError("扫描范围未发现达标配置；真实结果已保存在sweep.json，请扩大范围")
    chosen = pareto["ooo"][0]
    selected = {(r["buffers"], r["barriers"], r["policy"]) for group in pareto.values() for r in group}
    selected.add((chosen["buffers"], chosen["barriers"], "fifo"))
    for b, m, p in list(selected):
        if b > 1:
            selected.add((b - 1, m, p))
        if m > 1:
            selected.add((b, m - 1, p))
    # 至少保存一条真实资源死锁，清楚展示前端停在何处。
    bad = next((r for r in rows if r["status"] == "deadlock" and r["time"] > 0), None)
    if bad:
        selected.add((bad["buffers"], bad["barriers"], bad["policy"]))
    long_checks, display = [], {}
    for b, m, policy in sorted(selected):
        cfg = {**config, "policy": policy}
        run = simulate(cfg, b, m)
        key = f"b{b}_m{m}_{policy}"
        display[key] = run
        save(args.output / f"trace_{key}.json", run)
        long = simulate({**cfg, "iterations": max(args.long_iterations, args.iterations)}, b, m)
        s, l = summarize(run), summarize(long)
        if s["status"] != l["status"] or s["ii"] != l["ii"]:
            raise AssertionError(f"{key}: 长运行状态或稳态不一致")
        if s["status"] == "deadlock":
            assert s["time"] == l["time"] and s["completed_iterations"] == l["completed_iterations"]
        long_checks.append({"key": key, "iterations": max(args.long_iterations, args.iterations),
                            "status": l["status"], "makespan": l["makespan"], "ii": l["ii"], "consistent": True})
    manifest = json.loads((HERE / "baseline_manifest.json").read_text())
    unchanged = {f: hashlib.sha256((HERE.parent / f).read_bytes()).hexdigest() == h for f, h in manifest["files"].items()}
    assert all(unchanged.values()), "旧版本基线或用户未提交文档发生变化"
    default = f"b{chosen['buffers']}_m{chosen['barriers']}_ooo"
    summary = {"config": config, "references": references, "targets": targets, "sweep": rows,
               "max_buffers": args.max_buffers, "max_barriers": args.max_barriers, "pareto": pareto,
               "default_run": default, "display_runs": list(display), "long_checks": long_checks,
               "baseline_files_unchanged": unchanged,
               "scope": "当前单前端顺序阻塞申请、零耗时回调/回收控制、最老就绪优先策略；最小组合限于已扫描范围"}
    save(args.output / "summary.json", summary)
    save(args.output / "effective_config.json", config)
    run = display[default]
    for pool in ("buffer", "barrier"):
        with (args.output / f"{pool}_events.jsonl").open("w", encoding="utf-8") as f:
            for event in run[f"{pool}_events"]:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
    save(args.output / "verification.json", {"cases": len(rows), "long_checks": long_checks,
                                            "baseline_files_unchanged": unchanged, "invariants": "PASS"})
    render(args.output)
    print(json.dumps({"output": str(args.output), "cases": len(rows),
                      "pareto": {p: [(r["buffers"], r["barriers"], r["ii"]) for r in group] for p, group in pareto.items()},
                      "long_checks": len(long_checks), "verification": "PASS"}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
