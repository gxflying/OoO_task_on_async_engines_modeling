"""one-hot ID 生命周期及容量约束，不依赖任何调度分配时点。"""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from barrier import BarrierHandle, BarrierPool


def request(owner, kind="full"):
    return {"kind": kind, "owner": owner}


class BarrierPoolTests(unittest.TestCase):
    def test_one_hot_id_and_wait_all(self):
        pool = BarrierPool(3)
        first, second = pool.allocate_many([request((0, 1)), request((0, 2))])
        self.assertEqual((first.mask, second.mask), (1, 2))
        self.assertEqual(pool.wait_mask([first, second]), 3)
        self.assertFalse(pool.wait_all([first, second]))
        pool.signal(first, 1)
        self.assertFalse(pool.wait_all([first, second]))
        pool.signal(second, 2)
        self.assertTrue(pool.wait_all([first, second]))
        self.assertTrue(pool.wait_all([]))
        self.assertTrue(pool.check_invariants())

    def test_allocation_failure_is_atomic(self):
        pool = BarrierPool(2)
        pool.allocate_many([request(0)])
        before = (pool.snapshot(), pool.export_leases(), list(pool.events),
                  list(pool.generations), pool.stats)
        self.assertIsNone(pool.allocate_many([request(1), request(2)], 7))
        self.assertEqual(before, (pool.snapshot(), pool.export_leases(), pool.events,
                                  pool.generations, pool.stats))

    def test_unready_wait_and_early_release_forbidden(self):
        pool = BarrierPool(1)
        handle, = pool.allocate_many([request(0)])
        before = pool.snapshot(), list(pool.events)
        with self.assertRaisesRegex(AssertionError, "尚未 SET"):
            pool.consume_and_release(handle, 1)
        self.assertEqual(before, (pool.snapshot(), pool.events))
        self.assertEqual(pool.free_count, 0)

    def test_signal_persists_until_consumed(self):
        pool = BarrierPool(1)
        handle, = pool.allocate_many([request(0)])
        pool.signal(handle, 2)
        self.assertTrue(pool.is_ready(handle))
        self.assertEqual(pool.free_count, 0)
        with self.assertRaisesRegex(AssertionError, "重复 SET"):
            pool.signal(handle, 3)
        pool.consume_and_release(handle, 500)
        self.assertEqual(pool.ready_mask, 0)
        self.assertEqual(pool.live_mask, 0)
        self.assertEqual(pool.export_leases()[0]["cycles"],
                         [{"cycle": 0, "set": 2, "wait": 500,
                           "set_seq": 1, "wait_seq": 2}])
        self.assertEqual([event["event"] for event in pool.events],
                         ["allocate", "set", "wait", "free"])

    def test_reuse_generation_rejects_stale_handles(self):
        pool = BarrierPool(1)
        old, = pool.allocate_many([request(0)])
        pool.signal(old, 1)
        pool.consume_and_release(old, 2)
        new, = pool.allocate_many([request(1)], 2)
        self.assertEqual(new.slot, old.slot)
        self.assertEqual(new.generation, old.generation + 1)
        for operation in (pool.assert_valid, pool.is_ready, pool.signal,
                          pool.consume_and_release):
            with self.assertRaisesRegex(AssertionError, "stale"):
                if operation in (pool.signal, pool.consume_and_release):
                    operation(old, 2)
                else:
                    operation(old)
        with self.assertRaisesRegex(AssertionError, "stale"):
            pool.wait_all([old])
        self.assertFalse(pool.is_ready(new))

    def test_lowest_free_and_same_time_recycle(self):
        pool = BarrierPool(3)
        first, second, third = pool.allocate_many([request(i) for i in range(3)])
        pool.signal(second, 7)
        pool.consume_and_release(second, 7)
        replacement, = pool.allocate_many([request(3)], 7)
        self.assertEqual(replacement.slot, 1)
        self.assertEqual((first.slot, third.slot), (0, 2))
        self.assertEqual(pool.export_leases()[1]["end"], 7)
        self.assertEqual(pool.export_leases()[3]["start"], 7)
        self.assertTrue(pool.check_invariants())

    def test_reserved_empty_lifecycle_reuses_id_without_free(self):
        pool = BarrierPool(2)
        empty = pool.reserve("empty", {"buffer": 0}, ready=True)
        self.assertTrue(pool.is_ready(empty))
        pool.consume_reserved(empty, 1)
        self.assertFalse(pool.is_ready(empty))
        self.assertEqual(pool.free_count, 1)
        self.assertEqual(pool.get(empty).cycle, 1)
        pool.signal(empty, 10)
        pool.consume_reserved(empty, 11)
        self.assertEqual(pool.get(empty).cycle, 2)
        self.assertEqual(pool.get(empty).handle, empty)
        self.assertEqual(pool.export_leases()[0]["cycles"],
                         [{"cycle": 0, "set": 0, "wait": 1,
                           "set_seq": 1, "wait_seq": 2},
                          {"cycle": 1, "set": 10, "wait": 11,
                           "set_seq": 3, "wait_seq": 4}])
        self.assertFalse(any(event["event"] == "free" for event in pool.events))
        self.assertTrue(pool.check_invariants())

    def test_reserved_and_dynamic_use_separate_consume_operations(self):
        pool = BarrierPool(2)
        empty = pool.reserve("empty", 0, ready=True)
        full, = pool.allocate_many([request(0)])
        pool.signal(full)
        with self.assertRaisesRegex(AssertionError, "保留 ID"):
            pool.consume_and_release(empty)
        with self.assertRaisesRegex(AssertionError, "动态 ID"):
            pool.consume_reserved(full)
        self.assertTrue(pool.wait_all([empty, full]))

    def test_peaks_distinguish_reserved_full_and_dynamic_empty(self):
        pool = BarrierPool(4)
        pool.reserve("empty", 0, ready=True)
        handles = pool.allocate_many([request(1), request(2), request(3, "empty")])
        self.assertEqual(pool.stats, {"peak_live": 4, "peak_dynamic_live": 3,
                                      "peak_full_live": 2})
        for handle in handles:
            pool.signal(handle)
            pool.consume_and_release(handle)
        self.assertEqual(pool.stats["peak_live"], 4)
        self.assertEqual(len(pool.live), 1)

    def test_invalid_request_cannot_partially_allocate(self):
        pool = BarrierPool(2)
        with self.assertRaises(ValueError):
            pool.allocate_many([request(0), {"kind": "full"}])
        self.assertEqual(pool.free_count, 2)
        self.assertEqual(pool.events, [])
        self.assertEqual(pool.generations, [0, 0])

    def test_owner_and_export_cannot_mutate_pool(self):
        pool = BarrierPool(1)
        owner = {"edge": [1, 2]}
        handle, = pool.allocate_many([request(owner)])
        owner["edge"][0] = 999
        pool.snapshot()[0]["owner"]["edge"][0] = 123
        pool.export_leases()[0]["owner"]["edge"][0] = 456
        self.assertEqual(pool.get(handle).owner, {"edge": [1, 2]})

    def test_zero_capacity_and_invalid_capacity(self):
        pool = BarrierPool(0)
        self.assertIsNone(pool.allocate_many([request(0)]))
        self.assertIsNone(pool.reserve("empty", 0, ready=True))
        self.assertEqual(pool.allocate_many([]), [])
        self.assertTrue(pool.check_invariants())
        for capacity in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                BarrierPool(capacity)

    def test_time_cannot_go_backwards(self):
        pool = BarrierPool(1)
        handle, = pool.allocate_many([request(0)], 10)
        with self.assertRaises(ValueError):
            pool.signal(handle, 9)
        self.assertFalse(pool.is_ready(handle))
        self.assertEqual(len(pool.events), 1)

    def test_invalid_handle_rejected_without_index_access(self):
        pool = BarrierPool(1)
        for handle in (BarrierHandle(-1, 1), BarrierHandle(100, 1), None):
            with self.assertRaises(AssertionError):
                pool.assert_valid(handle)

    def test_zero_duration_leases_preserve_total_event_order_and_peak(self):
        pool = BarrierPool(2)
        first, second = pool.allocate_many([request(0), request(1)], 10)
        pool.signal(first, 10)
        pool.signal(second, 10)
        pool.consume_and_release(first, 10)
        pool.consume_and_release(second, 10)
        self.assertEqual(pool.peak_live, 2)
        self.assertEqual([event["seq"] for event in pool.events], list(range(8)))
        leases = pool.export_leases()
        self.assertEqual((leases[0]["allocated_time"], leases[0]["set_time"],
                          leases[0]["released_time"]), (10, 10, 10))
        self.assertEqual((leases[0]["start_seq"], leases[0]["end_seq"]), (0, 5))
        self.assertEqual((leases[1]["start_seq"], leases[1]["end_seq"]), (1, 7))
        # 逐事件重放，而不是按 [start,end) 统计，才能看见同周期的占用峰值。
        live = peak = 0
        for event in pool.events:
            live += (event["event"] == "allocate") - (event["event"] == "free")
            peak = max(peak, live)
        self.assertEqual((live, peak), (0, 2))


if __name__ == "__main__":
    unittest.main()
