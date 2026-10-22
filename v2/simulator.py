"""v2：等大小共享 buffer 的分配、读取、释放纳入真实发射条件。"""
from __future__ import annotations

import copy
import importlib.util
from collections import Counter
from pathlib import Path

from memory import BufferPool

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("pipeline_v1", HERE.parent / "pipeline.py")
v1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v1)
ENGINES = v1.ENGINES


def consumers_for(records):
    children = {r["seq"]: set() for r in records}
    for r in records:
        for dep in r["deps"]:
            children[dep].add(r["seq"])
    return children


def state_key(time, cursor, queues, running, done, records, pool, nops):
    active = [cursor] + [s for q in queues.values() for s in q]
    active += [s for s in running.values() if s is not None]
    active += [l.owner for l in pool.live.values()]
    base = min(active) // nops
    shift = base * nops
    # 所有 buffer 同容量且无 bank 差异，因此物理编号/无限代数可规范化掉。
    # producer、pending consumer、active reader 和 ready 状态都必须参与比较。
    memory = tuple(sorted((l.owner - shift, l.kind, l.ready,
                           tuple(sorted(c - shift for c in l.remaining)),
                           tuple(sorted(c - shift for c in l.readers))) for l in pool.live.values()))
    return base, (cursor - shift,
                  tuple(tuple(s - shift for s in queues[e]) for e in ENGINES),
                  tuple(None if running[e] is None else
                        (running[e] - shift, records[running[e]]["end"] - time) for e in ENGINES),
                  tuple(sorted(s - shift for s in done if s >= shift)), memory)


def simulate(config, buffer_count):
    config = copy.deepcopy(config)
    depths = v1.validate_config(config)
    count_source = config.get("count_source_inputs", True)
    if type(count_source) is not bool:
        raise ValueError("count_source_inputs 必须为 bool")
    if any(len(set(o["deps"])) != len(o["deps"]) for o in config["operations"]):
        raise ValueError("同一输入依赖不可重复")
    records = v1.instruction_records(config)
    for r in records:
        r.update(input_handles=[], output_handle=None)
    children = consumers_for(records)
    nops, total = len(config["operations"]), len(records)
    queues = {e: [] for e in ENGINES}
    running = dict.fromkeys(ENGINES)
    done, outputs, inputs = set(), {}, {}
    done_counts = Counter()
    completions, segments, seen = [], [], {}
    pool = BufferPool(buffer_count)
    cursor = time = 0
    stable = None
    status, deadlock = "complete", None
    feed_end = None

    while True:
        # 所有完成事件先处理，读者结束前绝不释放；同一边界可回收再分配。
        for e in ENGINES:
            seq = running[e]
            if seq is None or records[seq]["end"] != time:
                continue
            for handle in inputs[seq]:
                pool.finish_read(handle, seq, time)
            pool.finish_write(outputs[seq], time)
            done.add(seq)
            done_counts[records[seq]["iteration"]] += 1
            if done_counts[records[seq]["iteration"]] == nops:
                completions.append({"iteration": records[seq]["iteration"], "time": time})
            running[e] = None
        if len(done) == total:
            if pool.live:
                raise AssertionError("程序完成后仍泄漏 buffer")
            break
        if time >= config["max_cycles"]:
            status = "timeout"
            break

        issued, dispatched = [], []
        while True:
            blocked_engine = None
            while cursor < total:
                e = records[cursor]["engine"]
                if len(queues[e]) == depths[e]:
                    blocked_engine = e
                    break
                records[cursor]["enqueue"] = time
                queues[e].append(cursor)
                dispatched.append(cursor)
                cursor += 1
            if cursor == total and feed_end is None:
                feed_end = time

            progress = False
            for e in ENGINES:
                if running[e] is not None:
                    continue
                candidates = queues[e] if config["policy"] == "ooo" else queues[e][:1]
                chosen = None
                for seq in candidates:
                    r = records[seq]
                    need = 1 + int(count_source and not r["deps"])
                    if all(d in done for d in r["deps"]) and pool.free_count >= need:
                        chosen = seq
                        break
                if chosen is None:
                    continue
                r = records[chosen]
                requests = [(chosen, "output", children[chosen])]
                if count_source and not r["deps"]:
                    requests.append((chosen, "source_input", {chosen}))
                allocated = pool.allocate_many(requests, time)
                assert allocated is not None
                outputs[chosen] = allocated[0]
                inputs[chosen] = [outputs[d] for d in r["deps"]] + allocated[1:]
                if allocated[0] in inputs[chosen]:
                    raise AssertionError("输出覆盖正在读取的输入")
                for handle in inputs[chosen]:
                    pool.begin_read(handle, chosen, time)
                r["input_handles"] = [[h.slot, h.generation] for h in inputs[chosen]]
                r["output_handle"] = [allocated[0].slot, allocated[0].generation]
                pos = queues[e].index(chosen)
                r["bypassed"] = queues[e][:pos]
                r["start"], r["end"] = time, time + r["duration"]
                queues[e].pop(pos)
                running[e] = chosen
                issued.append(chosen)
                progress = True
            if not progress:
                break

        reasons = {}
        for e in ENGINES:
            candidates = queues[e] if config["policy"] == "ooo" else queues[e][:1]
            ready = [s for s in candidates if all(d in done for d in records[s]["deps"])]
            reasons[e] = ("busy" if running[e] is not None else "empty" if not queues[e]
                          else "buffer" if ready else "dependency")

        if stable is None and cursor < total:
            base, key = state_key(time, cursor, queues, running, done, records, pool, nops)
            history = seen.setdefault(key, [])
            history.append((time, base))
            if len(history) >= 3:
                a, b, c = history[-3:]
                if b[0] - a[0] == c[0] - b[0] and b[1] - a[1] == c[1] - b[1] > 0:
                    stable = {"start": a[0], "confirmed_at": c[0], "period": b[0] - a[0],
                              "iterations_per_period": b[1] - a[1],
                              "criterion": "调度及 buffer 所有权/读者完整状态，经轮次和平等槽位重命名连续三次相同"}

        ends = [records[s]["end"] for s in running.values() if s is not None]
        if not ends:
            status = "deadlock"
            deadlock = {"time": time, "cursor": cursor, "live_buffers": pool.snapshot(),
                        "waiting": [{"seq": s, "missing_deps": [d for d in records[s]["deps"] if d not in done],
                                     "new_buffers_required": 1 + int(count_source and not records[s]["deps"])}
                                    for q in queues.values() for s in q]}
            break
        next_time = min(min(ends), config["max_cycles"])
        segments.append({"start": time, "end": next_time, "cursor": cursor,
                         "running": dict(running), "queues": {e: list(queues[e]) for e in ENGINES},
                         "idle_reason": reasons, "blocked_engine": blocked_engine,
                         "buffers": pool.snapshot(), "issued": issued, "dispatched": dispatched})
        time = next_time

    if stable:
        bound = feed_end if feed_end is not None else time
        periods = (bound - stable["start"]) // stable["period"]
        stable.update(measurement_start=stable["start"],
                      measurement_end=stable["start"] + periods * stable["period"],
                      measurement_periods=periods)
    result = {"config": config, "buffer_count": buffer_count, "depths": depths, "status": status,
              "time": time, "makespan": time if status == "complete" else None,
              "completed_iterations": len(completions), "completed_instructions": len(done),
              "records": records, "segments": segments, "leases": pool.export_leases(),
              "memory_events": pool.events, "peak_buffers": pool.peak,
              "iteration_completions": completions, "steady": stable, "deadlock": deadlock}
    verify(result)
    result["metrics"] = metrics(result)
    return result


def overlap(start, end, a, b):
    return max(0, min(end, b) - max(start, a))


def window(result, start, end):
    duration = end - start
    units = {}
    for e in ENGINES:
        counts = {reason: sum(overlap(start, end, s["start"], s["end"])
                              for s in result["segments"] if s["idle_reason"][e] == reason)
                  for reason in ("busy", "empty", "dependency", "buffer")}
        units[e] = {**counts, "utilization": counts["busy"] / duration}
    completed = sum(start < c["time"] <= end for c in result["iteration_completions"])
    return {"start": start, "end": end, "duration": duration, "units": units,
            "completed_iterations": completed, "throughput": completed / duration,
            "mean_live_buffers": sum(overlap(start, end, s["start"], s["end"]) * len(s["buffers"])
                                     for s in result["segments"]) / duration}


def metrics(result):
    work = {e: sum(o["duration"] for o in result["config"]["operations"] if o["engine"] == e) for e in ENGINES}
    data = {"work_per_iteration": work, "ii_lower_bound": max(work.values()), "steady": None,
            "full": window(result, 0, result["time"]) if result["time"] else None,
            "ooo_issues": sum(bool(r["bypassed"]) for r in result["records"])}
    s = result["steady"]
    if s and result["status"] == "complete" and s["measurement_periods"] > 0:
        m = window(result, s["measurement_start"], s["measurement_end"])
        m["ii"] = s["period"] / s["iterations_per_period"]
        m["bound_efficiency"] = data["ii_lower_bound"] / m["ii"]
        data["steady"] = m
    return data


def verify(result):
    """独立从指令/lease 时间区间检查生命期和版本，不依赖池内部计数。"""
    records = result["records"]
    children = consumers_for(records)
    leases = result["leases"]
    by_handle = {(l["slot"], l["generation"]): l for l in leases}
    for e in ENGINES:
        issued = sorted((r for r in records if r["engine"] == e and r["start"] is not None), key=lambda r: r["start"])
        if any(a["end"] > b["start"] for a, b in zip(issued, issued[1:])):
            raise AssertionError("执行单元重叠")
    for r in records:
        if r["start"] is None:
            continue
        assert r["enqueue"] <= r["start"] and r["end"] - r["start"] == r["duration"]
        assert all(records[d]["end"] is not None and records[d]["end"] <= r["start"] for d in r["deps"])
        out = by_handle[tuple(r["output_handle"])]
        assert out["owner"] == r["seq"] and out["kind"] == "output" and out["start"] == r["start"]
        actual_inputs = [by_handle[tuple(h)] for h in r["input_handles"]]
        assert {l["owner"] for l in actual_inputs if l["kind"] == "output"} == set(r["deps"])
        expected_source = int(result["config"].get("count_source_inputs", True) and not r["deps"])
        assert sum(l["kind"] == "source_input" for l in actual_inputs) == expected_source
        for l in actual_inputs:
            assert l["slot"] != out["slot"], "输入输出同址覆盖"
            assert l["start"] <= r["start"] and (l["end"] is None or l["end"] >= r["end"]), "输入过早释放"
    for lease in leases:
        owner = records[lease["owner"]]
        expected_readers = children[lease["owner"]] if lease["kind"] == "output" else {lease["owner"]}
        assert set(lease["consumers"]) == expected_readers
        relevant = [records[c]["end"] for c in expected_readers] or [owner["end"]]
        if lease["end"] is not None:
            assert all(t is not None for t in relevant)
            assert lease["end"] == max(relevant), "不符合最后消费者完成时释放"
        elif result["status"] == "complete":
            raise AssertionError("完成后 buffer 泄漏")
    for slot in range(result["buffer_count"]):
        history = sorted((l for l in leases if l["slot"] == slot), key=lambda l: l["generation"])
        for a, b in zip(history, history[1:]):
            assert b["generation"] == a["generation"] + 1
            assert a["end"] is not None and a["end"] <= b["start"], "物理 buffer 覆盖未结束生命周期"
    for segment in result["segments"]:
        assert len(segment["buffers"]) <= result["buffer_count"]
        assert len({l["slot"] for l in segment["buffers"]}) == len(segment["buffers"])
        for e in ENGINES:
            assert len(segment["queues"][e]) <= result["depths"][e]
    if result["status"] == "complete":
        assert all(r["end"] is not None for r in records)
        assert result["completed_instructions"] == len(records)


def fixed_trace_requirement(records, count_source=True, window_start=None, window_end=None):
    """固定时序的区间重叠峰值；等大小共享 buffer 的精确着色下界。"""
    children = consumers_for(records)
    intervals = [{"owner": r["seq"], "kind": "output", "start": r["start"],
                  "end": max([records[c]["end"] for c in children[r["seq"]]] or [r["end"]])}
                 for r in records]
    if count_source:
        intervals += [{"owner": r["seq"], "kind": "source_input", "start": r["start"], "end": r["end"]}
                      for r in records if not r["deps"]]
    points = sorted({x["start"] for x in intervals})
    if window_start is not None:
        points = [p for p in points if window_start <= p < window_end] + [window_start]
    peak, witness_time = max((sum(x["start"] <= t < x["end"] for x in intervals), t) for t in points)
    return {"minimum_for_fixed_trace": peak, "witness_time": witness_time,
            "witness": [x for x in intervals if x["start"] <= witness_time < x["end"]],
            "intervals": intervals}
