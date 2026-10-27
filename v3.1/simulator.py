"""v3.1：真实协程阻塞 API、提交前分配、独立 buffer/barrier 位图池。"""
from __future__ import annotations

import copy
import importlib.util
from collections import Counter
from pathlib import Path

from resources import BlockingAPI, EventPool, Handle

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("v31_v1", HERE.parent / "pipeline.py")
v1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v1)
ENGINES = v1.ENGINES


def encoded(h):
    return [h.slot, h.generation]


class Context:
    def __init__(self, kind, owner, generator):
        self.kind, self.owner, self.generator = kind, owner, generator
        self.request = None
        self.done = False
        self.advance()

    def advance(self, value=None):
        try:
            self.request = self.generator.send(value)
        except StopIteration:
            self.done, self.request = True, None


class Kernel:
    def __init__(self, config, buffers, barriers):
        self.config = copy.deepcopy(config)
        self.depths = v1.validate_config(config)
        if type(config.get("count_source_inputs", True)) is not bool:
            raise ValueError("count_source_inputs 必须为 bool")
        positions = {o["id"]: i for i, o in enumerate(config["operations"])}
        for i, op in enumerate(config["operations"]):
            if any(positions[d] >= i for d in op["deps"]):
                raise ValueError("提交前绑定数据句柄要求生产者先于消费者")
            if len(op["deps"]) != len(set(op["deps"])):
                raise ValueError("同一依赖不可重复")
        self.records = v1.instruction_records(config)
        self.children = {r["seq"]: [] for r in self.records}
        for r in self.records:
            r.update(input_handles=[], output_handle=None, source_handle=None,
                     allocated_barriers=[], wait_ids=[], set_ids=[], compute_end=None)
            for dep in r["deps"]:
                self.children[dep].append(r["seq"])
        self.buffer = EventPool(buffers, "buffer")
        self.barrier = EventPool(barriers, "barrier")
        self.api = BlockingAPI()
        self.time = self.cursor = 0
        self.output_handles, self.source_handles = {}, {}
        self.data_ids, self.release_ids = {}, {}
        self.queues = {e: [] for e in ENGINES}
        self.running = dict.fromkeys(ENGINES)
        self.callbacks, self.reclaimers = {}, {}
        self.reclaim_history = []
        self.done = set()
        self.iteration_done, self.done_counts = [], Counter()
        self.segments, self.api_events = [], []
        self.nops = len(config["operations"])
        self.front = Context("frontend", -1, self.frontend())
        self.front_exhausted = None
        self.status, self.deadlock, self.steady = "complete", None, None

    def alloc_buffer(self, owner):
        h = yield from self.api.get_buffer(owner)
        yield from self.api.set_buffer(h)
        return h

    def frontend(self):
        for r in self.records:
            seq = r["seq"]
            self.cursor = seq
            if not r["deps"] and self.config.get("count_source_inputs", True):
                h = yield from self.alloc_buffer(["source", seq])
                self.source_handles[seq] = h
                r["source_handle"] = encoded(h)
            out = yield from self.alloc_buffer(["output", seq])
            self.output_handles[seq] = out
            r["output_handle"] = encoded(out)
            for child in self.children[seq]:
                h = yield from self.api.get_barrier(["data", seq, child])
                self.data_ids[(seq, child)] = h
                r["allocated_barriers"].append({"kind": "data", "edge": [seq, child], "handle": encoded(h)})
            # 多消费者输出：每个消费者另向回收控制上下文发一个完成事件。
            for producer in r["deps"]:
                if len(self.children[producer]) > 1:
                    h = yield from self.api.get_barrier(["release", producer, seq])
                    self.release_ids[(producer, seq)] = h
                    r["allocated_barriers"].append({"kind": "release", "edge": [producer, seq], "handle": encoded(h)})
            inputs = [self.output_handles[d] for d in r["deps"]]
            if seq in self.source_handles:
                inputs.append(self.source_handles[seq])
            r["input_handles"] = [encoded(h) for h in inputs]
            r["wait_ids"] = [encoded(self.data_ids[(d, seq)]) for d in r["deps"]]
            r["set_ids"] = [x["handle"] for x in r["allocated_barriers"]]
            yield from self.api.submit_async(seq)
            # 与约定伪代码一致：最后一个直接消费者提交后才注册回收控制。
            for producer in r["deps"]:
                if len(self.children[producer]) > 1 and self.children[producer][-1] == seq:
                    yield from self.api.reclaim_async(producer)
        self.cursor = len(self.records)
        self.front_exhausted = self.time
        yield from self.api.drain_all()

    def on_done(self, seq):
        r = self.records[seq]
        if seq in self.source_handles:
            yield from self.api.free_buffer(self.source_handles[seq])
        for dep in r["deps"]:
            if len(self.children[dep]) == 1:
                yield from self.api.free_buffer(self.output_handles[dep])
        signals = [self.data_ids[(seq, c)] for c in self.children[seq]]
        signals += [self.release_ids[(d, seq)] for d in r["deps"] if len(self.children[d]) > 1]
        if signals:
            yield from self.api.set_barrier(signals)
        if not self.children[seq]:
            yield from self.api.free_buffer(self.output_handles[seq])

    def reclaim(self, producer):
        yield from self.api.wait_barrier([self.release_ids[(producer, c)] for c in self.children[producer]])
        yield from self.api.free_buffer(self.output_handles[producer])

    def actor(self, ctx):
        return [ctx.kind, self.cursor if ctx.kind == "frontend" else ctx.owner]

    def service(self, ctx):
        """只在请求满足时 send 返回值；未满足则协程仍挂在同一个 API 内。"""
        progress = False
        while not ctx.done:
            req, actor = ctx.request, self.actor(ctx)
            kind, arg, value = req.kind, req.args, None
            if kind in ("get_buffer", "get_barrier"):
                pool = self.buffer if kind == "get_buffer" else self.barrier
                value = pool.allocate_if_free(arg, self.time, actor)
                if value is None:
                    break
            elif kind in ("set_buffer", "set_barrier"):
                pool = self.buffer if kind == "set_buffer" else self.barrier
                pool.set(arg, self.time, actor)
            elif kind in ("free_buffer", "wait_barrier"):
                pool = self.buffer if kind == "free_buffer" else self.barrier
                if not pool.ready(arg):
                    break
                pool.consume(arg, self.time, actor)
            elif kind == "submit":
                r = self.records[arg]
                q = self.queues[r["engine"]]
                if len(q) >= self.depths[r["engine"]]:
                    break
                q.append(arg)
                r["enqueue"] = self.time
            elif kind == "reclaim":
                assert arg not in self.reclaimers
                self.reclaimers[arg] = Context("reclaim", arg, self.reclaim(arg))
                self.reclaim_history.append({"producer": arg, "registered": self.time, "end": None})
            elif kind == "drain":
                if len(self.done) != len(self.records) or self.callbacks or any(not x.done for x in self.reclaimers.values()):
                    break
            else:
                raise AssertionError(kind)
            self.api_events.append({"time": self.time, "context": actor, "api": kind,
                                    "result": encoded(value) if isinstance(value, Handle) else None})
            ctx.advance(value)
            progress = True
        return progress

    def issue(self):
        progress = False
        for engine in ENGINES:
            if self.running[engine] is not None:
                continue
            q = self.queues[engine]
            candidates = q if self.config["policy"] == "ooo" else q[:1]
            seq = next((s for s in candidates if self.barrier.ready([Handle(*h) for h in self.records[s]["wait_ids"]])), None)
            if seq is None:
                continue
            r = self.records[seq]
            self.barrier.consume([Handle(*h) for h in r["wait_ids"]], self.time, ["issue", seq])
            for h in r["input_handles"] + [r["output_handle"]]:
                self.buffer.lookup(Handle(*h))
            r["bypassed"] = q[:q.index(seq)]
            q.remove(seq)
            r["start"], r["compute_end"] = self.time, self.time + r["duration"]
            self.running[engine] = seq
            progress = True
        return progress

    def normalized(self):
        active = [self.cursor] + [s for q in self.queues.values() for s in q]
        active += [s for s in self.running.values() if s is not None]
        active += [l["owner"][1] for l in self.buffer.live.values()]
        active += [p for p, ctx in self.reclaimers.items() if not ctx.done]
        base = min(active) // self.nops
        offset = base * self.nops
        def owner(o):
            return (o[0], *(x - offset for x in o[1:]))
        def request(ctx):
            if ctx.done:
                return None
            k, a = ctx.request.kind, ctx.request.args
            if k.startswith("get_"):
                return (k, owner(a))
            if k in ("submit", "reclaim"):
                return (k, a - offset)
            if k == "drain":
                return (k,)
            pool = self.buffer if "buffer" in k else self.barrier
            return (k, tuple(owner(pool.lookup(h)["owner"]) for h in a))
        return base, (self.cursor - offset, request(self.front),
                      tuple(tuple(s - offset for s in self.queues[e]) for e in ENGINES),
                      tuple(None if self.running[e] is None else
                            (self.running[e] - offset, max(0, self.records[self.running[e]]["compute_end"] - self.time)) for e in ENGINES),
                      tuple(sorted(s - offset for s in self.done if s >= offset)),
                      tuple(sorted((owner(l["owner"]), l["set"] is not None) for l in self.buffer.live.values())),
                      tuple(sorted((owner(l["owner"]), l["set"] is not None) for l in self.barrier.live.values())),
                      tuple(sorted((p - offset, request(ctx)) for p, ctx in self.reclaimers.items() if not ctx.done)),
                      tuple(sorted((s - offset, request(ctx)) for s, ctx in self.callbacks.items())))

    def waiting_state(self):
        front = None if self.front.done else {"pc": self.cursor, "api": self.front.request.kind,
                                              "detail": str(self.front.request.args)}
        return {"time": self.time, "frontend": front, "queues": copy.deepcopy(self.queues),
                "running": dict(self.running), "buffer_pool": self.buffer.snapshot(),
                "barrier_pool": self.barrier.snapshot(),
                "reclaimers": [{"producer": p, "api": c.request.kind} for p, c in self.reclaimers.items() if not c.done]}

    def run(self):
        seen = {}
        while True:
            for e, seq in self.running.items():
                if seq is not None and self.records[seq]["compute_end"] <= self.time and seq not in self.callbacks:
                    self.callbacks[seq] = Context("completion", seq, self.on_done(seq))
            while True:
                progress = False
                for e in ENGINES:
                    seq = self.running[e]
                    if seq not in self.callbacks:
                        continue
                    ctx = self.callbacks[seq]
                    progress |= self.service(ctx)
                    if ctx.done:
                        self.records[seq]["end"] = self.time
                        self.running[e] = None
                        self.done.add(seq)
                        i = self.records[seq]["iteration"]
                        self.done_counts[i] += 1
                        if self.done_counts[i] == self.nops:
                            self.iteration_done.append({"iteration": i, "time": self.time})
                        del self.callbacks[seq]
                        progress = True
                for producer, ctx in self.reclaimers.items():
                    if not ctx.done:
                        progress |= self.service(ctx)
                        if ctx.done:
                            next(x for x in self.reclaim_history if x["producer"] == producer)["end"] = self.time
                if self.time >= self.config["max_cycles"]:
                    break
                if not self.front.done:
                    progress |= self.service(self.front)
                progress |= self.issue()
                if not progress:
                    break
            self.buffer.check()
            self.barrier.check()
            if len(self.done) == len(self.records) and not any(not x.done for x in self.reclaimers.values()):
                if not self.front.done:
                    self.service(self.front)
                assert not self.buffer.live and not self.barrier.live
                break
            if self.time >= self.config["max_cycles"]:
                self.status = "timeout"
                break
            if self.steady is None and self.cursor < len(self.records):
                base, key = self.normalized()
                history = seen.setdefault(key, [])
                history.append((self.time, base))
                if len(history) >= 3:
                    a, b, c = history[-3:]
                    if b[0] - a[0] == c[0] - b[0] and b[1] - a[1] == c[1] - b[1] > 0:
                        self.steady = {"start": a[0], "confirmed_at": c[0], "period": b[0] - a[0],
                                       "iterations_per_period": b[1] - a[1]}
            future = [self.records[s]["compute_end"] for s in self.running.values()
                      if s is not None and self.records[s]["compute_end"] > self.time]
            if not future:
                self.status, self.deadlock = "deadlock", self.waiting_state()
                break
            end = min(min(future), self.config["max_cycles"])
            state = {}
            for e in ENGINES:
                s = self.running[e]
                state[e] = ("compute" if s is not None and self.records[s]["compute_end"] > self.time
                            else "callback_wait" if s is not None else "empty" if not self.queues[e] else "dependency")
            self.segments.append({"start": self.time, "end": end, "running": dict(self.running),
                                  "queues": copy.deepcopy(self.queues), "engine_state": state,
                                  "frontend_api": self.front.request.kind if not self.front.done else "done",
                                  "cursor": self.cursor, "buffers": self.buffer.snapshot(),
                                  "barriers": self.barrier.snapshot(),
                                  "active_reclaimers": [p for p, c in self.reclaimers.items() if not c.done]})
            self.time = end
        if self.steady:
            stop = self.front_exhausted if self.front_exhausted is not None else self.time
            periods = (stop - self.steady["start"]) // self.steady["period"]
            self.steady.update(measurement_start=self.steady["start"],
                               measurement_end=self.steady["start"] + periods * self.steady["period"],
                               measurement_periods=periods)
        result = {"config": self.config, "buffer_count": self.buffer.capacity, "barrier_count": self.barrier.capacity,
                  "depths": self.depths, "status": self.status, "time": self.time,
                  "makespan": self.time if self.status == "complete" else None,
                  "completed_iterations": len(self.iteration_done), "completed_instructions": len(self.done),
                  "records": self.records, "segments": self.segments, "steady": self.steady,
                  "iteration_completions": self.iteration_done, "reclaimers": self.reclaim_history,
                  "buffer_leases": self.buffer.leases, "barrier_leases": self.barrier.leases,
                  "buffer_events": self.buffer.events, "barrier_events": self.barrier.events,
                  "api_events": self.api_events, "buffer_peak": self.buffer.peak, "barrier_peak": self.barrier.peak,
                  "final_buffers": self.buffer.snapshot(), "final_barriers": self.barrier.snapshot(),
                  "deadlock": self.deadlock}
        result["metrics"] = metrics(result)
        verify(result)
        return result


def simulate(config, buffers, barriers):
    return Kernel(config, buffers, barriers).run()


def overlap(a, b, c, d):
    return max(0, min(b, d) - max(a, c))


def window(r, a, b):
    duration = b - a
    units = {}
    for e in ENGINES:
        counts = {reason: sum(overlap(a, b, s["start"], s["end"]) for s in r["segments"] if s["engine_state"][e] == reason)
                  for reason in ("compute", "callback_wait", "empty", "dependency")}
        units[e] = {**counts, "utilization": counts["compute"] / duration}
    front = {api: sum(overlap(a, b, s["start"], s["end"]) for s in r["segments"] if s["frontend_api"] == api)
             for api in ("get_buffer", "get_barrier", "submit", "drain", "done")}
    return {"start": a, "end": b, "duration": duration, "units": units, "frontend_wait": front,
            "completed_iterations": sum(a < c["time"] <= b for c in r["iteration_completions"])}


def metrics(r):
    work = {e: sum(o["duration"] for o in r["config"]["operations"] if o["engine"] == e) for e in ENGINES}
    m = {"work_per_iteration": work, "ii_lower_bound": max(work.values()), "steady": None,
         "full": window(r, 0, r["time"]) if r["time"] else None}
    s = r["steady"]
    if r["status"] == "complete" and s and s["measurement_periods"] > 0:
        w = window(r, s["measurement_start"], s["measurement_end"])
        w.update(ii=s["period"] / s["iterations_per_period"])
        w["efficiency"] = m["ii_lower_bound"] / w["ii"]
        m["steady"] = w
    return m


def verify(r):
    records = r["records"]
    buf = {(x["slot"], x["generation"]): x for x in r["buffer_leases"]}
    ids = {(x["slot"], x["generation"]): x for x in r["barrier_leases"]}
    children = {x["seq"]: [] for x in records}
    for op in records:
        for dep in op["deps"]:
            children[dep].append(op["seq"])
        if op["start"] is None:
            continue
        assert op["enqueue"] <= op["start"] and op["compute_end"] - op["start"] == op["duration"]
        out = buf[tuple(op["output_handle"])]
        assert out["owner"] == ["output", op["seq"]] and out["start"] <= op["enqueue"]
        assert len({h[0] for h in op["input_handles"] + [op["output_handle"]]}) == len(op["input_handles"]) + 1
        source = [buf[tuple(h)] for h in op["input_handles"] if buf[tuple(h)]["owner"][0] == "source"]
        assert len(source) == int(r["config"].get("count_source_inputs", True) and not op["deps"])
        assert all(l["owner"] == ["source", op["seq"]] for l in source)
        assert {buf[tuple(h)]["owner"][1] for h in op["input_handles"] if buf[tuple(h)]["owner"][0] == "output"} == set(op["deps"])
        assert len(op["wait_ids"]) == len(op["deps"])
        for dep, h in zip(op["deps"], op["wait_ids"]):
            l = ids[tuple(h)]
            assert l["owner"] == ["data", dep, op["seq"]]
            assert l["set"] == records[dep]["end"] <= op["start"] == l["end"]
        for h in op["input_handles"]:
            l = buf[tuple(h)]
            assert l["set"] is not None and l["set"] <= op["start"]
            assert l["end"] is None or l["end"] >= op["compute_end"], "仍在读取时覆盖了buffer"
    for e in ENGINES:
        lane = sorted((x for x in records if x["engine"] == e and x["start"] is not None), key=lambda x: x["start"])
        assert all(a["end"] is not None and a["end"] <= b["start"] for a, b in zip(lane, lane[1:]))
    for l in r["buffer_leases"]:
        if l["end"] is None:
            assert r["status"] != "complete"
            continue
        kind, seq = l["owner"]
        consumers = children[seq] if kind == "output" and children[seq] else [seq]
        assert all(records[c]["end"] is not None and records[c]["end"] <= l["end"] for c in consumers)
        if kind == "output" and len(children[seq]) > 1:
            assert l["freed_by"] == ["reclaim", seq]
            notices = [x for x in r["barrier_leases"] if x["owner"][0:2] == ["release", seq]]
            assert len(notices) == len(children[seq])
            assert all(x["end"] is not None and x["end"] <= l["end"] for x in notices)
            control = next(x for x in r["reclaimers"] if x["producer"] == seq)
            assert l["end"] == control["end"] == max(control["registered"], *(records[c]["end"] for c in children[seq]))
        else:
            assert l["end"] == max(records[c]["end"] for c in consumers)
    for l in r["barrier_leases"]:
        kind, producer, consumer = l["owner"]
        setter = producer if kind == "data" else consumer
        assert kind in ("data", "release")
        assert l["allocated_by"] == ["frontend", setter]
        assert records[setter]["enqueue"] is None or l["start"] <= records[setter]["enqueue"]
        if l["set"] is not None:
            assert l["set"] == records[setter]["end"]
        if l["end"] is not None:
            assert l["freed_by"] == (["issue", consumer] if kind == "data" else ["reclaim", producer])
        if r["status"] == "complete":
            assert l["end"] is not None
    for poolname in ("buffer", "barrier"):
        leases = r[f"{poolname}_leases"]
        for slot in range(r[f"{poolname}_count"]):
            history = sorted((x for x in leases if x["slot"] == slot), key=lambda x: x["generation"])
            assert all(a["end"] is not None and a["end"] <= b["start"] and b["generation"] == a["generation"] + 1 for a, b in zip(history, history[1:]))
        live, ready, peak = {}, set(), 0
        for event in r[f"{poolname}_events"]:
            key = (event["slot"], event["generation"])
            assert event["mask"] == 1 << event["slot"]
            if event["event"] == "get":
                assert event["slot"] not in live
                live[event["slot"]] = key
                peak = max(peak, len(live))
            elif event["event"] == "set":
                assert live[event["slot"]] == key and key not in ready
                ready.add(key)
            else:
                assert live[event["slot"]] == key and key in ready
                del live[event["slot"]]
                ready.remove(key)
            allocated = sum(1 << s for s in live)
            assert event["free_bitmap"] == ((1 << r[f"{poolname}_count"]) - 1) ^ allocated
            assert event["flag_bitmap"] == sum(1 << s for s, g in ready)
        assert peak == r[f"{poolname}_peak"]
    for segment in r["segments"]:
        assert all(len(segment["queues"][e]) <= r["depths"][e] for e in ENGINES)
        assert len(segment["buffers"]["live"]) <= r["buffer_count"]
        assert len(segment["barriers"]["live"]) <= r["barrier_count"]
    if r["status"] == "complete":
        assert len(records) == r["completed_instructions"]
        assert not r["final_buffers"]["live"] and not r["final_barriers"]["live"]
        assert all(x["end"] is not None for x in r["reclaimers"])
