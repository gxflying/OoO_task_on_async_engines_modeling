"""有限 one-hot barrier 池：占用、SET 就绪和 WAIT 回收是三个独立状态。

动态 ID 在 WAIT 成功后回收，复用同一 slot 时递增 generation。保留 ID
则在 WAIT 后仅清位，slot 和 generation 不变；cycle 用于报告每一轮
SET/WAIT。保留 ID 的 handle 是长期有效的静态地址，不提供跨 cycle 的
旧消息识别，调用方必须保证上一轮 WAIT 后才进行下一轮 SET。

本模块不决定在解码、发射或完成时分配 ID，也不决定 empty 应使用动态
还是保留 ID。所有时点由调度器显式传入。
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json


@dataclass(frozen=True)
class BarrierHandle:
    slot: int
    generation: int

    @property
    def mask(self):
        return 1 << self.slot


@dataclass
class BarrierLease:
    handle: BarrierHandle
    kind: str
    owner: object
    start: int
    reserved: bool = False
    ready: bool = False
    cycle: int = 0
    end: int | None = None
    start_seq: int = 0
    end_seq: int | None = None
    cycles: list[dict] = field(default_factory=list)

    @property
    def allocated_time(self):
        return self.start

    @property
    def set_time(self):
        return self.cycles[-1]["set"] if self.cycles else None

    @property
    def released_time(self):
        return self.end


class BarrierPool:
    def __init__(self, capacity):
        if type(capacity) is not int or capacity < 0:
            raise ValueError("barrier_count 必须是非负整数")
        self.capacity = capacity
        self.live: dict[int, BarrierLease] = {}
        self.generations = [0] * capacity
        self.leases: list[BarrierLease] = []
        self.events: list[dict] = []
        self.peak_live = 0
        self.peak_dynamic_live = 0
        self.peak_full_live = 0

    @property
    def free_count(self):
        return self.capacity - len(self.live)

    @property
    def live_mask(self):
        return sum(1 << slot for slot in self.live)

    @property
    def ready_mask(self):
        return sum(1 << slot for slot, lease in self.live.items() if lease.ready)

    @property
    def stats(self):
        return {"peak_live": self.peak_live,
                "peak_dynamic_live": self.peak_dynamic_live,
                "peak_full_live": self.peak_full_live}

    def _check_time(self, time):
        if type(time) is not int or time < 0:
            raise ValueError("barrier 事件时间必须是非负整数")
        if self.events and time < self.events[-1]["time"]:
            raise ValueError("barrier 事件时间不能倒退")

    @staticmethod
    def _request(request):
        if not isinstance(request, dict) or set(request) != {"kind", "owner"}:
            raise ValueError("分配请求必须包含且只包含 kind 和 owner")
        if not isinstance(request["kind"], str) or not request["kind"]:
            raise ValueError("barrier kind 必须是非空字符串")
        # 在开始分配之前验证所有请求，并隔离调用者之后修改 owner 的影响。
        try:
            json.dumps(request["owner"], allow_nan=False)
        except (ValueError, TypeError) as error:
            raise ValueError("barrier owner 必须可序列化为 JSON") from error
        return request["kind"], deepcopy(request["owner"])

    def _event(self, event, lease, time):
        self.events.append({"seq": len(self.events), "time": time, "event": event,
                            "slot": lease.handle.slot,
                            "generation": lease.handle.generation,
                            "mask": lease.handle.mask,
                            "kind": lease.kind, "owner": deepcopy(lease.owner),
                            "reserved": lease.reserved, "cycle": lease.cycle})

    def _allocate(self, validated, time, reserved=False):
        if len(validated) > self.free_count:
            return None
        free = [slot for slot in range(self.capacity) if slot not in self.live]
        handles = []
        for slot, (kind, owner) in zip(free, validated):
            self.generations[slot] += 1
            handle = BarrierHandle(slot, self.generations[slot])
            lease = BarrierLease(handle, kind, owner, time, reserved=reserved,
                                 start_seq=len(self.events))
            self.live[slot] = lease
            self.leases.append(lease)
            handles.append(handle)
            self._event("allocate", lease, time)
        self.peak_live = max(self.peak_live, len(self.live))
        self.peak_dynamic_live = max(self.peak_dynamic_live,
                                     sum(not lease.reserved for lease in self.live.values()))
        self.peak_full_live = max(self.peak_full_live,
                                  sum(lease.kind == "full" for lease in self.live.values()))
        return handles

    def allocate_many(self, requests, time=0):
        """requests=[{kind, owner}, ...]；全部成功或无变化，返回 handle 列表/None。"""
        self._check_time(time)
        validated = [self._request(request) for request in requests]
        return self._allocate(validated, time)

    def reserve(self, kind, owner, ready=False, time=0):
        """保留一个静态 ID，WAIT 后仅清位；ready=True 表示初始化为已 SET。"""
        self._check_time(time)
        validated = [self._request({"kind": kind, "owner": owner})]
        handles = self._allocate(validated, time, reserved=True)
        if handles is None:
            return None
        handle = handles[0]
        if ready:
            self.signal(handle, time)
        return handle

    def get(self, handle):
        if not isinstance(handle, BarrierHandle):
            raise AssertionError(f"无效 barrier handle: {handle!r}")
        lease = self.live.get(handle.slot)
        if lease is None or lease.handle != handle:
            raise AssertionError(f"stale barrier handle: {handle}")
        return lease

    def assert_valid(self, handle):
        self.get(handle)

    def is_ready(self, handle):
        return self.get(handle).ready

    def wait_mask(self, handles):
        """组合 one-hot ID；同时核对代数，不能只凭裸 bitmask 判断就绪。"""
        mask = 0
        for handle in handles:
            self.get(handle)
            mask |= handle.mask
        return mask

    def wait_all(self, handles):
        mask = self.wait_mask(handles)
        return self.ready_mask & mask == mask

    def signal(self, handle, time=0):
        self._check_time(time)
        lease = self.get(handle)
        if lease.ready:
            raise AssertionError("重复 SET 已就绪的 barrier")
        lease.ready = True
        lease.cycles.append({"cycle": lease.cycle, "set": time, "wait": None,
                             "set_seq": len(self.events), "wait_seq": None})
        self._event("set", lease, time)

    def _consume(self, lease, time):
        if not lease.ready:
            raise AssertionError("barrier 尚未 SET，不能 WAIT 或提前回收")
        lease.cycles[-1]["wait_seq"] = len(self.events)
        self._event("wait", lease, time)
        lease.cycles[-1]["wait"] = time
        lease.ready = False

    def consume_and_release(self, handle, time=0):
        """动态 ID：成功 WAIT，清位并回收 slot。"""
        self._check_time(time)
        lease = self.get(handle)
        if lease.reserved:
            raise AssertionError("保留 ID 应使用 consume_reserved 清位，不能回收")
        self._consume(lease, time)
        lease.end = time
        del self.live[handle.slot]
        lease.end_seq = len(self.events)
        self._event("free", lease, time)

    def consume_reserved(self, handle, time=0):
        """保留 ID：成功 WAIT，仅清位；下一次 SET 属于下一 cycle。"""
        self._check_time(time)
        lease = self.get(handle)
        if not lease.reserved:
            raise AssertionError("动态 ID 应使用 consume_and_release")
        self._consume(lease, time)
        lease.cycle += 1

    def snapshot(self):
        return [{"slot": slot, "generation": lease.handle.generation,
                 "mask": lease.handle.mask, "kind": lease.kind,
                 "owner": deepcopy(lease.owner), "ready": lease.ready,
                 "reserved": lease.reserved, "cycle": lease.cycle}
                for slot, lease in sorted(self.live.items())]

    def export_leases(self):
        return [{"slot": lease.handle.slot, "generation": lease.handle.generation,
                 "mask": lease.handle.mask, "kind": lease.kind,
                 "owner": deepcopy(lease.owner), "start": lease.start,
                 "end": lease.end, "reserved": lease.reserved,
                 "allocated_time": lease.allocated_time,
                 "set_time": lease.set_time, "released_time": lease.released_time,
                 "start_seq": lease.start_seq, "end_seq": lease.end_seq,
                 "cycles": deepcopy(lease.cycles)} for lease in self.leases]

    def check_invariants(self):
        assert len(self.live) <= self.capacity
        assert self.ready_mask & ~self.live_mask == 0
        assert self.live_mask < (1 << self.capacity)
        assert len(self.live) == self.live_mask.bit_count()
        for slot, lease in self.live.items():
            assert 0 <= slot < self.capacity
            assert lease.handle.slot == slot
            assert lease.handle.generation == self.generations[slot]
            assert lease.end is None
            assert lease.handle.mask.bit_count() == 1
            if lease.ready:
                assert lease.cycles and lease.cycles[-1]["wait"] is None
            elif lease.cycles:
                assert lease.cycles[-1]["wait"] is not None
        return True
