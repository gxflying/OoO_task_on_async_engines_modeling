"""两个同构而独立的 one-hot 事件池，显式维护 free/flag bitmap。"""
from dataclasses import dataclass
from copy import deepcopy


@dataclass(frozen=True)
class Handle:
    slot: int
    generation: int

    @property
    def mask(self):
        return 1 << self.slot


class EventPool:
    def __init__(self, capacity, name):
        if type(capacity) is not int or capacity < 0:
            raise ValueError("capacity 必须是非负整数")
        self.capacity, self.name = capacity, name
        self.free_bitmap = (1 << capacity) - 1
        self.flag_bitmap = 0
        self.generations = [0] * capacity
        self.live = {}
        self.leases, self.events = [], []
        self.peak = 0

    @property
    def free_count(self):
        return self.free_bitmap.bit_count()

    def event(self, event, lease, time, actor):
        self.events.append({"seq": len(self.events), "time": time, "event": event,
                            "pool": self.name, "slot": lease["slot"], "generation": lease["generation"],
                            "mask": 1 << lease["slot"], "owner": deepcopy(lease["owner"]),
                            "actor": deepcopy(actor), "free_bitmap": self.free_bitmap,
                            "flag_bitmap": self.flag_bitmap})

    def allocate_if_free(self, owner, time, actor):
        """内部原语；公共 blocking API 由内核挂起/恢复，绝不把 None 返回调用者。"""
        if not self.free_bitmap:
            return None
        bit = self.free_bitmap & -self.free_bitmap
        slot = bit.bit_length() - 1
        self.free_bitmap &= ~bit
        self.generations[slot] += 1
        lease = {"slot": slot, "generation": self.generations[slot], "owner": deepcopy(owner),
                 "start": time, "set": None, "end": None, "allocated_by": deepcopy(actor),
                 "freed_by": None}
        self.live[slot] = lease
        self.leases.append(lease)
        self.peak = max(self.peak, len(self.live))
        self.event("get", lease, time, actor)
        return Handle(slot, lease["generation"])

    def lookup(self, handle):
        lease = self.live.get(handle.slot)
        if lease is None or lease["generation"] != handle.generation:
            raise AssertionError(f"stale {self.name} handle: {handle}")
        return lease

    def ready(self, handles):
        for h in handles:
            self.lookup(h)
        return all(self.flag_bitmap & h.mask for h in handles)

    def set(self, handles, time, actor):
        assert len(set(handles)) == len(handles)
        leases = [self.lookup(h) for h in handles]
        assert all(not (self.flag_bitmap & h.mask) for h in handles), "重复 SET"
        for h, lease in zip(handles, leases):
            self.flag_bitmap |= h.mask
            lease["set"] = time
            self.event("set", lease, time, actor)

    def consume(self, handles, time, actor):
        assert len(set(handles)) == len(handles)
        assert self.ready(handles), "WAIT/FREE 尚未 SET"
        for h in handles:
            lease = self.lookup(h)
            self.flag_bitmap &= ~h.mask
            self.free_bitmap |= h.mask
            lease["end"], lease["freed_by"] = time, deepcopy(actor)
            del self.live[h.slot]
            self.event("wait_free", lease, time, actor)

    def check(self):
        allocated = sum(1 << s for s in self.live)
        assert allocated & self.free_bitmap == 0
        assert allocated | self.free_bitmap == (1 << self.capacity) - 1
        assert self.flag_bitmap & ~allocated == 0
        assert self.free_count + len(self.live) == self.capacity

    def snapshot(self):
        return {"free_bitmap": self.free_bitmap, "flag_bitmap": self.flag_bitmap,
                "live": [{"slot": s, "generation": l["generation"], "owner": deepcopy(l["owner"]),
                          "set": l["set"] is not None} for s, l in sorted(self.live.items())]}


@dataclass
class Request:
    kind: str
    args: object


class BlockingAPI:
    """yield from 调用；资源不足时协程停在该 API 内，不返回失败 flag。"""
    def get_buffer(self, owner):
        handle = yield Request("get_buffer", owner)
        assert isinstance(handle, Handle)
        return handle

    def set_buffer(self, handle):
        yield Request("set_buffer", [handle])

    def free_buffer(self, handle):
        yield Request("free_buffer", [handle])

    def get_barrier(self, owner):
        handle = yield Request("get_barrier", owner)
        assert isinstance(handle, Handle)
        return handle

    def set_barrier(self, handles):
        yield Request("set_barrier", list(handles))

    def wait_barrier(self, handles):
        yield Request("wait_barrier", list(handles))

    def submit_async(self, seq):
        yield Request("submit", seq)

    def reclaim_async(self, producer):
        yield Request("reclaim", producer)

    def drain_all(self):
        yield Request("drain", None)
