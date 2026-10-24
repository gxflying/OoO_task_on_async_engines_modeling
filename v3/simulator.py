"""v3：完成时 get_and_set、动态 full/empty one-hot ID 的闭合模拟。"""
from __future__ import annotations

import copy
import importlib.util
import sys
from collections import Counter
from pathlib import Path

from barrier import BarrierPool

HERE = Path(__file__).resolve().parent
sys.path.append(str(HERE.parent / "v2"))
from memory import BufferPool

spec = importlib.util.spec_from_file_location("v3_v2_baseline", HERE.parent / "v2" / "simulator.py")
v2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2)
v1 = v2.v1
ENGINES = v1.ENGINES


def serial_handle(pool, handle):
    lease = pool.get(handle)
    return {"slot": handle.slot, "generation": handle.generation,
            "mask": handle.mask, "owner": copy.deepcopy(lease.owner)}


def canonical_state(t, cursor, queues, running, done, records, mem, ids, nops):
    active = [cursor] + [s for q in queues.values() for s in q]
    active += [s for s in running.values() if s is not None]
    active += [l.owner for l in mem.live.values()]
    base = min(active) // nops
    shift = base * nops
    memory = tuple(sorted((l.owner - shift, l.kind, l.ready,
                           tuple(sorted(c - shift for c in l.remaining)),
                           tuple(sorted(c - shift for c in l.readers))) for l in mem.live.values()))
    # 全部槽等价且无 bank 差异，ID/物理槽可重命名；full 的逻辑边不可丢弃。
    full = tuple(sorted((l.owner[0] - shift, l.owner[1] - shift, l.ready)
                        for l in ids.live.values() if l.kind == "full"))
    empty = tuple(sorted(l.ready for l in ids.live.values() if l.kind == "empty"))
    return base, (cursor - shift, tuple(tuple(s - shift for s in queues[e]) for e in ENGINES),
                  tuple(None if running[e] is None else
                        (running[e] - shift, max(0, records[running[e]]["compute_end"] - t)) for e in ENGINES),
                  tuple(sorted(s - shift for s in done if s >= shift)), memory, full, empty)


def simulate(config, buffer_count, barrier_count):
    config = copy.deepcopy(config)
    depths = v1.validate_config(config)
    count_source = config.get("count_source_inputs", True)
    if type(count_source) is not bool:
        raise ValueError("count_source_inputs 必须为 bool")
    if any(len(set(o["deps"])) != len(o["deps"]) for o in config["operations"]):
        raise ValueError("同一依赖边不可重复")
    records = v1.instruction_records(config)
    for r in records:
        r.update(compute_end=None, input_handles=[], output_handle=None,
                 wait_full=[], wait_empty=[], set_full=[], set_empty=[])
    children = v2.consumers_for(records)
    total, nops = len(records), len(config["operations"])
    mem, ids = BufferPool(buffer_count), BarrierPool(barrier_count)
    queues = {e: [] for e in ENGINES}
    running = dict.fromkeys(ENGINES)
    outputs, inputs, full_edges, empty_slots = {}, {}, {}, {}
    done, counts = set(), Counter()
    segments, completions, seen = [], [], {}
    cursor = time = 0
    feed_end = stable = deadlock = None
    status = "complete"

    initial = ids.allocate_many([{"kind": "empty", "owner": [slot, 0]} for slot in range(buffer_count)], 0)
    if initial is None:
        status = "initialization_blocked"
        deadlock = {"time": 0, "reason": "初始空闲 buffer 各需一个已 SET 的 empty ID",
                    "required": buffer_count, "available": barrier_count}
    else:
        for slot, handle in enumerate(initial):
            ids.signal(handle, 0)
            empty_slots[slot] = handle

    while status == "complete" and len(done) < total:
        issued, dispatched, published = [], [], []
        while True:
            progress = False
            # compute_done 不释放任何资源。先原子得到全部 full+empty，才能完成。
            for e in ENGINES:
                seq = running[e]
                if seq is None or records[seq]["compute_end"] > time:
                    continue
                r = records[seq]
                releases = [h for h in inputs[seq] if mem.get(h).remaining == {seq}]
                if not children[seq]:
                    releases.append(outputs[seq])
                requests = [{"kind": "full", "owner": [seq, c]} for c in sorted(children[seq])]
                requests += [{"kind": "empty", "owner": [h.slot, h.generation]} for h in releases]
                handles = ids.allocate_many(requests, time)
                if handles is None:
                    continue
                full_count = len(children[seq])
                r["set_full"] = [serial_handle(ids, h) for h in handles[:full_count]]
                r["set_empty"] = [serial_handle(ids, h) for h in handles[full_count:]]
                for h in inputs[seq]:
                    mem.finish_read(h, seq, time)
                mem.finish_write(outputs[seq], time)
                for child, h in zip(sorted(children[seq]), handles[:full_count]):
                    full_edges[(seq, child)] = h
                for old_buffer, h in zip(releases, handles[full_count:]):
                    assert old_buffer.slot not in mem.live
                    assert old_buffer.slot not in empty_slots
                    empty_slots[old_buffer.slot] = h
                for h in handles:
                    ids.signal(h, time)
                r["end"] = time
                done.add(seq)
                counts[r["iteration"]] += 1
                if counts[r["iteration"]] == nops:
                    completions.append({"iteration": r["iteration"], "time": time})
                running[e] = None
                published.append(seq)
                progress = True

            if time >= config["max_cycles"]:
                break

            blocked_engine = None
            while cursor < total:
                e = records[cursor]["engine"]
                if len(queues[e]) >= depths[e]:
                    blocked_engine = e
                    break
                records[cursor]["enqueue"] = time
                queues[e].append(cursor)
                dispatched.append(cursor)
                cursor += 1
            if cursor == total and feed_end is None:
                feed_end = time

            for e in ENGINES:
                if running[e] is not None:
                    continue
                candidates = queues[e] if config["policy"] == "ooo" else queues[e][:1]
                chosen = None
                for seq in candidates:
                    r = records[seq]
                    refs = [full_edges.get((d, seq)) for d in r["deps"]]
                    need = 1 + int(count_source and not r["deps"])
                    if any(h is None for h in refs) or not ids.wait_all(refs) or mem.free_count < need:
                        continue
                    free_slots = [s for s in range(buffer_count) if s not in mem.live][:need]
                    empties = [empty_slots[s] for s in free_slots]
                    if ids.wait_all(empties):
                        chosen = seq
                        break
                if chosen is None:
                    continue
                r = records[chosen]
                r["wait_full"] = [serial_handle(ids, h) for h in refs]
                r["wait_empty"] = [serial_handle(ids, h) for h in empties]
                # 仅在所有条件齐备且现在就能开始时消费 WAIT，不提前消费 fan-in 一半。
                for dep, h in zip(r["deps"], refs):
                    ids.consume_and_release(h, time)
                    del full_edges[(dep, chosen)]
                for slot, h in zip(free_slots, empties):
                    ids.consume_and_release(h, time)
                    del empty_slots[slot]
                requests = [(chosen, "output", children[chosen])]
                if count_source and not r["deps"]:
                    requests.append((chosen, "source_input", {chosen}))
                allocated = mem.allocate_many(requests, time)
                assert allocated is not None and [h.slot for h in allocated] == free_slots
                outputs[chosen] = allocated[0]
                inputs[chosen] = [outputs[d] for d in r["deps"]] + allocated[1:]
                for h in inputs[chosen]:
                    mem.begin_read(h, chosen, time)
                r["input_handles"] = [[h.slot, h.generation] for h in inputs[chosen]]
                r["output_handle"] = [allocated[0].slot, allocated[0].generation]
                r["bypassed"] = queues[e][:queues[e].index(chosen)]
                r["start"], r["compute_end"] = time, time + r["duration"]
                queues[e].remove(chosen)
                running[e] = chosen
                issued.append(chosen)
                progress = True
            if not progress:
                break

        ids.check_invariants()
        assert set(empty_slots) == set(range(buffer_count)) - set(mem.live)
        assert all(ids.is_ready(h) for h in empty_slots.values())
        if len(done) == total:
            assert not mem.live and not full_edges and len(ids.live) == buffer_count
            break
        if time >= config["max_cycles"]:
            status = "timeout"
            break

        reasons = {}
        for e in ENGINES:
            seq = running[e]
            if seq is not None:
                reasons[e] = "compute" if records[seq]["compute_end"] > time else "publish_barrier"
                continue
            candidates = queues[e] if config["policy"] == "ooo" else queues[e][:1]
            ready = [s for s in candidates if all((d, s) in full_edges for d in records[s]["deps"])]
            reasons[e] = "empty" if not queues[e] else "buffer" if ready else "wait_full"

        if stable is None and cursor < total:
            base, key = canonical_state(time, cursor, queues, running, done, records, mem, ids, nops)
            history = seen.setdefault(key, [])
            history.append((time, base))
            if len(history) >= 3:
                a, b, c = history[-3:]
                if b[0] - a[0] == c[0] - b[0] and b[1] - a[1] == c[1] - b[1] > 0:
                    stable = {"start": a[0], "confirmed_at": c[0], "period": b[0] - a[0],
                              "iterations_per_period": b[1] - a[1],
                              "criterion": "含full逻辑边、empty、阻塞完成和buffer读者的归一化完整状态连续三次重复"}
        future = [records[s]["compute_end"] for s in running.values()
                  if s is not None and records[s]["compute_end"] > time]
        if not future:
            status = "deadlock"
            blocked = []
            for e, seq in running.items():
                if seq is None:
                    continue
                releases = sum(mem.get(h).remaining == {seq} for h in inputs[seq]) + int(not children[seq])
                blocked.append({"engine": e, "seq": seq, "compute_end": records[seq]["compute_end"],
                                "full_required": len(children[seq]), "empty_required": releases,
                                "free_ids": ids.free_count})
            deadlock = {"time": time, "cursor": cursor, "blocked_completions": blocked,
                        "barriers": ids.snapshot(), "buffers": mem.snapshot(),
                        "queues": copy.deepcopy(queues)}
            break
        next_time = min(min(future), config["max_cycles"])
        segments.append({"start": time, "end": next_time, "cursor": cursor,
                         "running": dict(running), "queues": copy.deepcopy(queues),
                         "state": reasons, "blocked_engine": blocked_engine,
                         "buffers": mem.snapshot(), "barriers": ids.snapshot(),
                         "issued": issued, "dispatched": dispatched, "published": published})
        time = next_time

    if stable:
        stop = feed_end if feed_end is not None else time
        periods = (stop - stable["start"]) // stable["period"]
        stable.update(measurement_start=stable["start"], measurement_end=stable["start"] + periods * stable["period"],
                      measurement_periods=periods)
    result = {"config": config, "buffer_count": buffer_count, "barrier_count": barrier_count,
              "depths": depths, "status": status, "time": time,
              "makespan": time if status == "complete" else None,
              "completed_iterations": len(completions), "completed_instructions": len(done),
              "records": records, "segments": segments, "leases": mem.export_leases(),
              "memory_events": mem.events, "barrier_leases": ids.export_leases(),
              "barrier_events": ids.events, "barrier_stats": ids.stats,
              "final_barriers": ids.snapshot(), "final_buffers": mem.snapshot(),
              "iteration_completions": completions, "steady": stable, "deadlock": deadlock,
              "peak_buffers": mem.peak}
    result["metrics"] = metrics(result)
    verify(result)
    return result


def window(result, start, end):
    duration = end - start
    units = {}
    for e in ENGINES:
        counts = {reason: sum(v2.overlap(start, end, s["start"], s["end"])
                              for s in result["segments"] if s["state"][e] == reason)
                  for reason in ("compute", "publish_barrier", "empty", "buffer", "wait_full")}
        units[e] = {**counts, "compute_utilization": counts["compute"] / duration,
                    "occupied_utilization": (counts["compute"] + counts["publish_barrier"]) / duration}
    completions = sum(start < c["time"] <= end for c in result["iteration_completions"])
    return {"start": start, "end": end, "duration": duration, "units": units,
            "completed_iterations": completions, "throughput": completions / duration,
            "mean_live_ids": sum(v2.overlap(start, end, s["start"], s["end"]) * len(s["barriers"])
                                 for s in result["segments"]) / duration}


def metrics(result):
    work = {e: sum(o["duration"] for o in result["config"]["operations"] if o["engine"] == e) for e in ENGINES}
    m = {"work_per_iteration": work, "ii_lower_bound": max(work.values()), "steady": None,
         "full": window(result, 0, result["time"]) if result["time"] else None,
         "completion_stall_time": sum(max(0, (r["end"] if r["end"] is not None else result["time"]) - r["compute_end"])
                                      for r in result["records"] if r["compute_end"] is not None)}
    s = result["steady"]
    if s and result["status"] == "complete" and s["measurement_periods"] > 0:
        w = window(result, s["measurement_start"], s["measurement_end"])
        w["ii"] = s["period"] / s["iterations_per_period"]
        w["bound_efficiency"] = m["ii_lower_bound"] / w["ii"]
        m["steady"] = w
    return m


def verify(result):
    records = result["records"]
    children = v2.consumers_for(records)
    bl = {(l["slot"], l["generation"]): l for l in result["barrier_leases"]}
    ml = {(l["slot"], l["generation"]): l for l in result["leases"]}
    for e in ENGINES:
        lane = sorted((r for r in records if r["engine"] == e and r["start"] is not None), key=lambda r: r["start"])
        assert all(a["end"] is not None and a["end"] <= b["start"] for a, b in zip(lane, lane[1:])), "engine 重叠或阻塞期间提前发射"
    for r in records:
        if r["start"] is None:
            continue
        assert r["enqueue"] <= r["start"] and r["compute_end"] - r["start"] == r["duration"]
        if r["end"] is not None:
            assert r["end"] >= r["compute_end"]
            assert {tuple(x["owner"]) for x in r["set_full"]} == {(r["seq"], c) for c in children[r["seq"]]}
        assert {tuple(x["owner"]) for x in r["wait_full"]} == {(d, r["seq"]) for d in r["deps"]}
        wait_refs = r["wait_full"] + r["wait_empty"]
        assert len({x["slot"] for x in wait_refs}) == len(wait_refs)
        for field, expected_kind in (("wait_full", "full"), ("wait_empty", "empty"),
                                     ("set_full", "full"), ("set_empty", "empty")):
            for ref in r[field]:
                l = bl[(ref["slot"], ref["generation"])]
                assert l["kind"] == expected_kind and l["owner"] == ref["owner"] and l["mask"] == ref["mask"], "barrier 引用类型、owner或mask错配"
                if field.startswith("wait"):
                    assert l["set_time"] <= r["start"] and l["end"] == r["start"]
                else:
                    assert l["start"] == l["set_time"] == r["end"]
        out = ml[tuple(r["output_handle"])]
        assert out["owner"] == r["seq"] and out["start"] == r["start"]
        allocations = [out] + [ml[tuple(h)] for h in r["input_handles"] if ml[tuple(h)]["kind"] == "source_input"]
        assert all(l["owner"] == r["seq"] and l["start"] == r["start"] for l in allocations)
        assert {tuple(x["owner"]) for x in r["wait_empty"]} == {(l["slot"], l["generation"] - 1) for l in allocations}, "WAIT empty 未对应本次新分配buffer的上一代"
        expected_source = int(result["config"].get("count_source_inputs", True) and not r["deps"])
        assert len(allocations) == 1 + expected_source
        assert {ml[tuple(h)]["owner"] for h in r["input_handles"] if ml[tuple(h)]["kind"] == "output"} == set(r["deps"])
        for handle in r["input_handles"]:
            l = ml[tuple(handle)]
            assert l["slot"] != out["slot"]
            assert l["end"] is None or r["end"] is not None and l["end"] >= r["end"]
        assert all(records[d]["end"] is not None and records[d]["end"] <= r["start"] for d in r["deps"])
    empty_by_owner = {}
    for lease in result["barrier_leases"]:
        if lease["kind"] == "empty":
            owner = tuple(lease["owner"])
            assert owner not in empty_by_owner
            empty_by_owner[owner] = lease
        elif lease["kind"] == "full":
            p, c = lease["owner"]
            assert p in records[c]["deps"]
            assert lease["start"] == lease["set_time"] == records[p]["end"]
            assert lease["end"] == records[c]["start"]
    for l in result["leases"]:
        owner = records[l["owner"]]
        consumers = children[l["owner"]] if l["kind"] == "output" else {l["owner"]}
        assert set(l["consumers"]) == consumers
        if l["end"] is not None:
            ends = [records[c]["end"] for c in consumers] or [owner["end"]]
            assert all(x is not None for x in ends) and l["end"] == max(ends)
            assert empty_by_owner[(l["slot"], l["generation"])]["set_time"] == l["end"]
    # 以严格事件序重放，包含同刻 SET→WAIT 的零长度占用，不能只扫时间区间。
    live, ready, peak = {}, set(), 0
    for index, e in enumerate(result["barrier_events"]):
        assert e["seq"] == index and e["mask"] == 1 << e["slot"]
        assert 0 <= e["slot"] < result["barrier_count"]
        key = (e["slot"], e["generation"])
        if e["event"] == "allocate":
            assert e["slot"] not in live
            live[e["slot"]] = key
            peak = max(peak, len(live))
        else:
            assert live[e["slot"]] == key
            if e["event"] == "set":
                assert key not in ready
                ready.add(key)
            elif e["event"] == "wait":
                assert key in ready
                ready.remove(key)
            elif e["event"] == "free":
                assert key not in ready
                del live[e["slot"]]
    assert peak == result["barrier_stats"]["peak_live"] <= result["barrier_count"]
    for slot in range(result["buffer_count"]):
        history = sorted((l for l in result["leases"] if l["slot"] == slot), key=lambda l: l["generation"])
        for a, b in zip(history, history[1:]):
            assert a["end"] is not None and a["end"] <= b["start"] and a["generation"] + 1 == b["generation"]
    for segment in result["segments"]:
        assert len(segment["buffers"]) <= result["buffer_count"]
        assert len(segment["barriers"]) <= result["barrier_count"]
        assert all(len(segment["queues"][e]) <= result["depths"][e] for e in ENGINES)
    if result["status"] == "complete":
        assert result["completed_instructions"] == len(records)
        assert not result["final_buffers"] and len(result["final_barriers"]) == result["buffer_count"]
        assert all(b["kind"] == "empty" and b["ready"] for b in result["final_barriers"])
