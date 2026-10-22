"""等大小共享物理 buffer；生产者输出就是所有消费者的输入。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Handle:
    slot: int
    generation: int


@dataclass
class Lease:
    handle: Handle
    owner: int
    kind: str
    start: int
    consumers: set[int]
    remaining: set[int]
    ready: bool = False
    readers: set[int] = field(default_factory=set)
    end: int | None = None


class BufferPool:
    def __init__(self, capacity):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("buffer_count 必须是正整数")
        self.capacity = capacity
        self.live: dict[int, Lease] = {}
        self.generations = [0] * capacity
        self.leases: list[Lease] = []
        self.events = []
        self.peak = 0

    @property
    def free_count(self):
        return self.capacity - len(self.live)

    def get(self, handle):
        lease = self.live.get(handle.slot)
        if lease is None or lease.handle != handle:
            raise AssertionError(f"stale buffer handle: {handle}")
        return lease

    def allocate_many(self, requests, time):
        """请求为 (owner, kind, consumer-set)，全部成功或不改变任何状态。"""
        if len(requests) > self.free_count:
            return None
        free = [i for i in range(self.capacity) if i not in self.live]
        handles = []
        for slot, (owner, kind, consumers) in zip(free, requests):
            self.generations[slot] += 1
            handle = Handle(slot, self.generations[slot])
            lease = Lease(handle, owner, kind, time, set(consumers), set(consumers),
                          ready=kind == "source_input")
            self.live[slot] = lease
            self.leases.append(lease)
            handles.append(handle)
            self.events.append({"time": time, "event": "allocate", "slot": slot,
                                "generation": handle.generation, "owner": owner, "kind": kind})
        self.peak = max(self.peak, len(self.live))
        return handles

    def begin_read(self, handle, consumer, time):
        lease = self.get(handle)
        if not lease.ready or consumer not in lease.remaining or consumer in lease.readers:
            raise AssertionError("读取未完成、已消费或不属于本消费者的 buffer")
        lease.readers.add(consumer)
        self.events.append({"time": time, "event": "read_begin", "slot": handle.slot,
                            "generation": handle.generation, "consumer": consumer})

    def finish_read(self, handle, consumer, time):
        lease = self.get(handle)
        if consumer not in lease.readers:
            raise AssertionError("没有有效 reader 却结束读取")
        lease.readers.remove(consumer)
        lease.remaining.remove(consumer)
        self.events.append({"time": time, "event": "read_end", "slot": handle.slot,
                            "generation": handle.generation, "consumer": consumer})
        if not lease.remaining:
            self.release(handle, time)

    def finish_write(self, handle, time):
        lease = self.get(handle)
        if lease.kind != "output" or lease.ready:
            raise AssertionError("重复完成输出或错误的 lease 类型")
        lease.ready = True
        self.events.append({"time": time, "event": "write_done", "slot": handle.slot,
                            "generation": handle.generation, "owner": lease.owner})
        if not lease.remaining:
            # 末级结果由零延迟外部 sink 接收，输出至少占用整个执行时间。
            self.release(handle, time)

    def release(self, handle, time):
        lease = self.get(handle)
        if not lease.ready or lease.readers or lease.remaining:
            raise AssertionError("生产者/消费者未完成，禁止提前回收")
        lease.end = time
        del self.live[handle.slot]
        self.events.append({"time": time, "event": "release", "slot": handle.slot,
                            "generation": handle.generation, "owner": lease.owner})

    def snapshot(self):
        return [{"slot": s, "generation": l.handle.generation, "owner": l.owner,
                 "kind": l.kind, "ready": l.ready, "remaining": sorted(l.remaining),
                 "readers": sorted(l.readers)} for s, l in sorted(self.live.items())]

    def export_leases(self):
        return [{"slot": l.handle.slot, "generation": l.handle.generation, "owner": l.owner,
                 "kind": l.kind, "start": l.start, "end": l.end,
                 "consumers": sorted(l.consumers)} for l in self.leases]
