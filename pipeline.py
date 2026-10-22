"""四执行单元、有限乱序队列的逐周期模拟器；仅使用 Python 标准库。"""
from __future__ import annotations

import argparse
import base64
import copy
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

ENGINES = ("rdma", "wdma", "tensor", "vector")
HERE = Path(__file__).resolve().parent


def positive_int(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} 必须是正整数")


def validate_config(config):
    for name in ("iterations", "max_cycles"):
        positive_int(config[name], name)
    depth = config["queue_depth"]
    depths = {e: depth for e in ENGINES} if type(depth) is int else depth
    if not isinstance(depths, dict) or set(depths) != set(ENGINES):
        raise ValueError("queue_depth 必须是正整数或包含全部四个单元的对象")
    for e in ENGINES:
        positive_int(depths[e], f"queue_depth.{e}")
    if config["policy"] not in ("ooo", "fifo"):
        raise ValueError("policy 必须是 ooo 或 fifo")
    ops = config["operations"]
    if not isinstance(ops, list) or not ops:
        raise ValueError("operations 不可为空")
    ids = [op["id"] for op in ops]
    if any(not isinstance(x, str) or not x for x in ids) or len(set(ids)) != len(ids):
        raise ValueError("指令 id 必须是唯一的非空字符串")
    for op in ops:
        if op["engine"] not in ENGINES:
            raise ValueError(f"未知执行单元: {op['engine']}")
        positive_int(op["duration"], f"{op['id']}.duration")
        if not isinstance(op["deps"], list) or any(d not in ids for d in op["deps"]):
            raise ValueError(f"{op['id']} 存在未知依赖")
    visited, visiting = set(), set()
    by_id = {op["id"]: op for op in ops}

    def visit(name):
        if name in visiting:
            raise ValueError("依赖图存在环")
        if name in visited:
            return
        visiting.add(name)
        for dep in by_id[name]["deps"]:
            visit(dep)
        visiting.remove(name)
        visited.add(name)

    for name in ids:
        visit(name)
    return depths


def instruction_records(config):
    """每轮有独立的完成 token；配置中的数组顺序就是前端程序序。"""
    records = []
    nops = len(config["operations"])
    offsets = {op["id"]: i for i, op in enumerate(config["operations"])}
    for iteration in range(config["iterations"]):
        for index, op in enumerate(config["operations"]):
            records.append({
                "seq": len(records), "iteration": iteration, "op": op["id"],
                "engine": op["engine"], "duration": op["duration"],
                "label": op.get("label", op["id"]),
                "deps": [iteration * nops + offsets[d] for d in op["deps"]],
                "enqueue": None, "start": None, "end": None,
                "bypassed": [],
            })
    return records


def normalized_state(t, cursor, queues, running, done, records, nops):
    """比较完整调度状态，剔除绝对时间与整体轮次平移。

    frontier 之前的轮次已无在途指令，且模型没有跨轮依赖，所以可丢弃
    那些完成 token。未来未入队程序由 cursor 唯一确定。
    """
    active = [cursor] + [s for q in queues.values() for s in q]
    active += [s for s in running.values() if s is not None]
    base = min(active) // nops
    shift = base * nops
    return base, (
        cursor - shift,
        tuple(tuple(s - shift for s in queues[e]) for e in ENGINES),
        tuple(None if running[e] is None else
              (running[e] - shift, records[running[e]]["end"] - t)
              for e in ENGINES),
        tuple(sorted(s - shift for s in done if s >= shift)),
    )


def simulate(config):
    config = copy.deepcopy(config)
    depths = validate_config(config)
    records = instruction_records(config)
    nops, total = len(config["operations"]), len(records)
    queues = {e: [] for e in ENGINES}
    running = dict.fromkeys(ENGINES)
    done = set()
    cursor = 0
    cycles = []
    seen = {}
    stable = None
    iteration_done = []
    done_counts = Counter()

    for t in range(config["max_cycles"] + 1):
        # 边界 t：完成与依赖可见 -> 零耗时入队/发射直到无进展。
        # 发射立即释放队列槽；执行占用 [start,end)，入队不等待执行完成。
        for e in ENGINES:
            seq = running[e]
            if seq is not None and records[seq]["end"] == t:
                done.add(seq)
                done_counts[records[seq]["iteration"]] += 1
                if done_counts[records[seq]["iteration"]] == nops:
                    iteration_done.append({"iteration": records[seq]["iteration"], "time": t})
                running[e] = None
        if len(done) == total:
            break
        if t == config["max_cycles"]:
            raise RuntimeError(f"达到 max_cycles={t}，未完成；请检查死锁或增加上限")

        issued = []
        dispatched = []
        while True:
            blocked_engine = None
            while cursor < total:
                op = records[cursor]
                e = op["engine"]
                if len(queues[e]) >= depths[e]:
                    blocked_engine = e
                    break
                op["enqueue"] = t
                queues[e].append(cursor)
                dispatched.append(cursor)
                cursor += 1

            progress = False
            for e in ENGINES:
                if running[e] is not None:
                    continue
                candidates = queues[e] if config["policy"] == "ooo" else queues[e][:1]
                chosen = next((s for s in candidates if all(d in done for d in records[s]["deps"])), None)
                if chosen is None:
                    continue
                pos = queues[e].index(chosen)
                op = records[chosen]
                op["bypassed"] = queues[e][:pos]
                op["start"], op["end"] = t, t + op["duration"]
                running[e] = chosen
                queues[e].pop(pos)
                issued.append(chosen)
                progress = True
            if not progress:
                break
        reasons = {e: "busy" if running[e] is not None else "dependency" if queues[e] else "empty"
                   for e in ENGINES}

        cycles.append({
            "time": t, "running": dict(running),
            "queues": {e: list(queues[e]) for e in ENGINES},
            "idle_reason": reasons, "issued": issued, "dispatched": dispatched,
            "blocked_engine": blocked_engine, "cursor": cursor,
        })

        if stable is None and cursor < total:
            base, key = normalized_state(t, cursor, queues, running, done, records, nops)
            history = seen.setdefault(key, [])
            history.append((t, base))
            if len(history) >= 3:
                a, b, c = history[-3:]
                if b[0] - a[0] == c[0] - b[0] and b[1] - a[1] == c[1] - b[1] > 0:
                    stable = {
                        "start": a[0], "end": c[0], "confirmed_at": c[0],
                        "period": b[0] - a[0], "iterations_per_period": b[1] - a[1],
                        "observed_periods": 2,
                        "criterion": "完整调度状态经轮次平移后连续三次相同；确认两个周期",
                    }
        if not any(s is not None for s in running.values()) and not dispatched:
            blocked = [{"seq": s, "missing": [d for d in records[s]["deps"] if d not in done]}
                       for q in queues.values() for s in q]
            raise RuntimeError(f"t={t} 死锁：{blocked}")
    else:
        raise RuntimeError("模拟没有正常完成")

    if stable:
        # 从第一个重复状态取尽可能多的完整周期，排除前端耗尽及尾部排空。
        drain = next((c["time"] for c in cycles if c["cursor"] == total), t)
        count = (drain - stable["start"]) // stable["period"]
        stable["measurement_start"] = stable["start"]
        stable["measurement_end"] = stable["start"] + count * stable["period"]
        stable["measurement_periods"] = count
    result = {"config": config, "depths": depths, "records": records, "cycles": cycles,
              "iteration_completions": iteration_done, "makespan": t, "steady": stable}
    verify_schedule(result)
    result["metrics"] = measure(result)
    return result


def verify_schedule(result):
    """独立核验实际轨迹：时长、依赖、资源独占、队列容量与生命周期。"""
    records, cycles = result["records"], result["cycles"]
    for r in records:
        if r["enqueue"] is None or r["start"] is None or r["end"] is None:
            raise AssertionError("存在未完成指令")
        if r["start"] < r["enqueue"] or r["end"] - r["start"] != r["duration"]:
            raise AssertionError("时序或执行时长错误")
        if any(records[d]["end"] > r["start"] for d in r["deps"]):
            raise AssertionError("依赖尚未完成便发射")
        if any(records[d]["iteration"] != r["iteration"] for d in r["deps"]):
            raise AssertionError("错误的跨轮依赖")
    for e in ENGINES:
        lane = sorted((r for r in records if r["engine"] == e), key=lambda r: r["start"])
        if any(a["end"] > b["start"] for a, b in zip(lane, lane[1:])):
            raise AssertionError(f"{e} 执行单元冲突")
    enqueued, issued = [], []
    for c in cycles:
        enqueued.extend(c["dispatched"])
        issued.extend(c["issued"])
        blocked_engine = c["blocked_engine"]
        if blocked_engine is not None:
            if c["cursor"] >= len(records) or records[c["cursor"]]["engine"] != blocked_engine:
                raise AssertionError("前端反压目标错误")
            if len(c["queues"][blocked_engine]) != result["depths"][blocked_engine]:
                raise AssertionError("队列未满便反压前端")
        for e in ENGINES:
            q = c["queues"][e]
            if len(q) > result["depths"][e]:
                raise AssertionError("队列溢出")
            for seq in q:
                r = records[seq]
                if r["engine"] != e or not r["enqueue"] <= c["time"] < r["start"]:
                    raise AssertionError("队列生命周期错误")
            active = c["running"][e]
            if active is not None:
                r = records[active]
                if r["engine"] != e or not r["start"] <= c["time"] < r["end"] or active in q:
                    raise AssertionError("执行生命周期错误")
    if enqueued != list(range(len(records))) or sorted(issued) != list(range(len(records))):
        raise AssertionError("指令漏发、重复发射或前端乱序")


def window_metrics(result, start, end):
    duration = end - start
    if duration <= 0:
        raise ValueError("统计窗口必须为正")
    rows = result["cycles"][start:end]
    units = {}
    for e in ENGINES:
        occupied = sum(max(0, min(end, r["end"]) - max(start, r["start"]))
                       for r in result["records"] if r["engine"] == e)
        units[e] = {
            "busy": occupied, "utilization": occupied / duration,
            "idle_dependency": sum(c["idle_reason"][e] == "dependency" for c in rows),
            "idle_empty": sum(c["idle_reason"][e] == "empty" for c in rows),
            "queue_max": max(len(c["queues"][e]) for c in rows),
            "queue_mean": sum(len(c["queues"][e]) for c in rows) / duration,
        }
    completions = sum(start < c["time"] <= end for c in result["iteration_completions"])
    return {"start": start, "end": end, "duration": duration, "units": units,
            "completed_iterations": completions, "throughput": completions / duration,
            "mean_utilization": sum(u["utilization"] for u in units.values()) / len(ENGINES),
            "frontend_stall_cycles": sum(c["blocked_engine"] is not None for c in rows)}


def measure(result):
    config = result["config"]
    work = {e: sum(op["duration"] for op in config["operations"] if op["engine"] == e)
            for e in ENGINES}
    lower_bound = max(work.values())
    steady = result["steady"]
    metrics = {"work_per_iteration": work, "ii_lower_bound": lower_bound,
               "full": window_metrics(result, 0, result["makespan"]),
               "ooo_issues": sum(bool(r["bypassed"]) for r in result["records"]),
               "steady": None}
    if steady:
        m = window_metrics(result, steady["measurement_start"], steady["measurement_end"])
        m["ii"] = steady["period"] / steady["iterations_per_period"]
        m["bound_efficiency"] = lower_bound / m["ii"]
        metrics["steady"] = m
    return metrics


def summary(result):
    return {k: v for k, v in result.items() if k not in ("records", "cycles")}


def write_outputs(result, comparisons, output, alternate=None):
    output.mkdir(parents=True, exist_ok=True)
    other_policy = "fifo" if result["config"]["policy"] == "ooo" else "ooo"
    other_config = {**result["config"], "policy": other_policy}
    if alternate is None:
        alternate = simulate(other_config)
    if alternate["config"] != other_config:
        raise ValueError("两个执行时间线必须使用相同配置，仅发射策略不同")
    digest = hashlib.sha256(json.dumps(result["config"], sort_keys=True).encode()).hexdigest()
    data = summary(result)
    data["config_sha256"] = digest
    data["comparisons"] = comparisons
    (output / "summary.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "effective_config.json").write_text(json.dumps(result["config"], ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "trace.json").write_text(json.dumps({"instructions": result["records"], "cycles": result["cycles"]},
                                                  ensure_ascii=False), encoding="utf-8")
    fields = ("seq", "iteration", "op", "label", "engine", "duration", "enqueue", "start", "end", "deps", "bypassed")
    with (output / "instructions.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows({**r, "deps": " ".join(map(str, r["deps"])),
                         "bypassed": " ".join(map(str, r["bypassed"]))} for r in result["records"])
    packed = {**data, "records": result["records"], "cycles": result["cycles"]}
    other_data = summary(alternate)
    other_data["config_sha256"] = hashlib.sha256(json.dumps(other_config, sort_keys=True).encode()).hexdigest()
    packed["policy_runs"] = {other_policy: {**other_data, "records": alternate["records"], "cycles": alternate["cycles"]}}
    (output / f"summary_{other_policy}.json").write_text(
        json.dumps(other_data, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / f"trace_{other_policy}.json").write_text(
        json.dumps({"instructions": alternate["records"], "cycles": alternate["cycles"]}, ensure_ascii=False),
        encoding="utf-8")
    if result["config"].get("timing_source"):
        measurement_image = HERE / "measurement" / "measured_blocks_4x.png"
        if measurement_image.exists():
            packed["measurement_image"] = "data:image/png;base64," + base64.b64encode(measurement_image.read_bytes()).decode()
    template = (HERE / "report_template.html").read_text(encoding="utf-8")
    inline = json.dumps(packed, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    (output / "report.html").write_text(template.replace("__REPORT_DATA__", inline), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "config.json")
    parser.add_argument("--output", type=Path, default=HERE / "out")
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--queue-depth", type=int)
    parser.add_argument("--policy", choices=("ooo", "fifo"))
    parser.add_argument("--compare", action="store_true", help="运行深度 1～5 的 OoO/FIFO 完整对照")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    for field in ("iterations", "queue_depth", "policy"):
        value = getattr(args, field)
        if value is not None:
            config[field] = value
    result = simulate(config)
    comparisons = []
    alternate = None
    if args.compare:
        variants = [(policy, depth) for depth in range(1, 6) for policy in ("ooo", "fifo")]
        for policy, depth in variants:
            variant = {**config, "policy": policy, "queue_depth": depth}
            run = result if variant == config else simulate(variant)
            if depth == config["queue_depth"] and policy != config["policy"]:
                alternate = run
            comparisons.append({"policy": policy, "queue_depth": depth, "makespan": run["makespan"],
                                "steady": run["steady"], "metrics": run["metrics"]})
    write_outputs(result, comparisons, args.output, alternate)
    if comparisons:
        (args.output / "depth_1_5_comparison.json").write_text(
            json.dumps({"base_config": config, "runs": comparisons}, ensure_ascii=False, indent=2),
            encoding="utf-8")
    print(json.dumps({"output": str(args.output.resolve()), "instructions": len(result["records"]),
                      "makespan": result["makespan"], "steady": result["steady"],
                      "metrics": result["metrics"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
