import copy
import json
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from memory import BufferPool
from simulator import HERE, fixed_trace_requirement, simulate, verify, v1


def op(name, engine, duration, *deps):
    return {"id": name, "engine": engine, "duration": duration, "deps": list(deps)}


def config(ops, iterations=1, policy="ooo"):
    return {"operations": ops, "iterations": iterations, "queue_depth": 5,
            "max_cycles": 1000000, "policy": policy, "count_source_inputs": True}


class MemoryTests(unittest.TestCase):
    def test_allocation_is_atomic_and_cannot_overwrite_live_slot(self):
        pool = BufferPool(1)
        self.assertIsNone(pool.allocate_many([(0, "output", {1}), (0, "source_input", {0})], 0))
        self.assertEqual(pool.live, {})
        self.assertEqual(pool.generations, [0])
        h = pool.allocate_many([(0, "output", {1})], 0)[0]
        self.assertIsNone(pool.allocate_many([(2, "output", set())], 1))
        self.assertEqual(pool.get(h).owner, 0)

    def test_fanout_keeps_data_until_last_consumer_completion(self):
        pool = BufferPool(1)
        h = pool.allocate_many([(0, "output", {1, 2})], 0)[0]
        with self.assertRaises(AssertionError):
            pool.begin_read(h, 1, 0)
        pool.finish_write(h, 2)
        pool.begin_read(h, 1, 2)
        pool.begin_read(h, 2, 2)
        pool.finish_read(h, 1, 4)
        self.assertEqual(pool.free_count, 0)
        with self.assertRaises(AssertionError):
            pool.release(h, 4)
        pool.finish_read(h, 2, 8)
        self.assertEqual(pool.free_count, 1)

    def test_unstarted_consumer_also_prevents_release(self):
        pool = BufferPool(1)
        h = pool.allocate_many([(0, "output", {1, 2})], 0)[0]
        pool.finish_write(h, 1)
        pool.begin_read(h, 1, 1)
        pool.finish_read(h, 1, 2)
        self.assertEqual(pool.get(h).remaining, {2})
        self.assertEqual(pool.free_count, 0)
        pool.begin_read(h, 2, 7)
        pool.finish_read(h, 2, 9)
        self.assertEqual(pool.leases[0].end, 9)

    def test_generation_rejects_stale_handle_after_reuse(self):
        pool = BufferPool(1)
        old = pool.allocate_many([(0, "output", set())], 0)[0]
        pool.finish_write(old, 3)
        new = pool.allocate_many([(1, "output", set())], 3)[0]
        self.assertEqual(old.slot, new.slot)
        self.assertEqual(new.generation, old.generation + 1)
        with self.assertRaisesRegex(AssertionError, "stale"):
            pool.get(old)
        with self.assertRaisesRegex(AssertionError, "stale"):
            pool.finish_write(old, 4)

    def test_fork_join_has_two_real_inputs_and_no_inplace_write(self):
        ops = [op("a", "rdma", 2), op("b", "vector", 4, "a"),
               op("c", "tensor", 7, "a"), op("join", "wdma", 1, "b", "c")]
        r = simulate(config(ops), 4)
        self.assertEqual(r["makespan"], 10)
        self.assertEqual([o["start"] for o in r["records"]], [0, 2, 2, 9])
        a = next(l for l in r["leases"] if l["owner"] == 0 and l["kind"] == "output")
        self.assertEqual((a["start"], a["end"]), (0, 9))
        join = r["records"][3]
        self.assertEqual(len(join["input_handles"]), 2)
        self.assertEqual(len({h[0] for h in join["input_handles"] + [join["output_handle"]]}), 3)

    def test_same_engine_chain_releases_only_after_read_finishes(self):
        r = simulate(config([op("a", "vector", 2), op("b", "vector", 2, "a")]), 2)
        self.assertEqual(r["status"], "complete")
        self.assertEqual(r["makespan"], 4)
        a, b = r["records"]
        self.assertNotEqual(a["output_handle"][0], b["output_handle"][0])
        self.assertEqual(b["input_handles"], [a["output_handle"]])

    def test_root_needs_two_buffers_and_sink_releases_at_end(self):
        c = config([op("a", "rdma", 3)])
        self.assertEqual(simulate(c, 1)["status"], "deadlock")
        r = simulate(c, 2)
        self.assertEqual(r["makespan"], 3)
        self.assertEqual(len(r["leases"]), 2)
        self.assertTrue(all(l["end"] == 3 for l in r["leases"]))
        self.assertEqual(simulate({**c, "count_source_inputs": False}, 1)["status"], "complete")

    def test_same_boundary_reclaim_and_refill_has_no_bubble(self):
        r = simulate(config([op("a", "rdma", 3)], iterations=3), 2)
        self.assertEqual([o["start"] for o in r["records"]], [0, 3, 6])
        self.assertEqual(r["makespan"], 9)

    def test_corrupted_lifetimes_and_alias_are_rejected(self):
        r = simulate(config([op("a", "rdma", 2), op("b", "vector", 3, "a")]), 3)
        broken = copy.deepcopy(r)
        next(l for l in broken["leases"] if l["owner"] == 0 and l["kind"] == "output")["end"] = 2
        with self.assertRaises(AssertionError):
            verify(broken)
        broken = copy.deepcopy(r)
        broken["records"][1]["output_handle"] = broken["records"][0]["output_handle"]
        with self.assertRaises(AssertionError):
            verify(broken)

    def test_fixed_trace_peak_and_interval_boundaries(self):
        c = config([op("a", "rdma", 3)], iterations=3)
        base = v1.simulate(c)
        self.assertEqual(fixed_trace_requirement(base["records"])["minimum_for_fixed_trace"], 2)
        self.assertEqual(fixed_trace_requirement(base["records"], False)["minimum_for_fixed_trace"], 1)

    def test_pixel_sweep_minimum_and_nonmonotonic_cases(self):
        c = json.loads((HERE / "config.json").read_text())
        expected = {3: 793, 4: 660, 5: 587, 6: 595, 7: 448.25, 8: 477, 9: 298, 10: 298, 11: 298}
        for n in range(1, 12):
            with self.subTest(buffers=n):
                r = simulate(c, n)
                if n < 3:
                    self.assertEqual(r["status"], "deadlock")
                    self.assertIsNone(r["makespan"])
                else:
                    self.assertEqual(r["status"], "complete")
                    self.assertEqual(r["metrics"]["steady"]["ii"], expected[n])
                    self.assertEqual(r["peak_buffers"], n)

    def test_11_matches_v1_and_9_only_delays_noncritical_transfers(self):
        c = json.loads((HERE / "config.json").read_text())
        base = v1.simulate(c)
        self.assertEqual(fixed_trace_requirement(base["records"])["minimum_for_fixed_trace"], 11)
        a, b = simulate(c, 9), simulate(c, 11)
        self.assertEqual(a["makespan"], base["makespan"])
        self.assertEqual([(o["start"], o["end"]) for o in b["records"]],
                         [(o["start"], o["end"]) for o in base["records"]])
        for x, y in zip(a["records"], base["records"]):
            if x["engine"] in ("tensor", "vector"):
                self.assertEqual((x["start"], x["end"]), (y["start"], y["end"]))
        self.assertEqual(a["metrics"]["steady"]["units"]["vector"]["buffer"], 0)

    def test_300_iterations_confirm_8_9_and_11(self):
        c = json.loads((HERE / "config.json").read_text())
        for n, expected in [(8, 477), (9, 298), (11, 298)]:
            with self.subTest(buffers=n):
                r = simulate({**c, "iterations": 300}, n)
                self.assertEqual(r["status"], "complete")
                self.assertEqual(r["metrics"]["steady"]["ii"], expected)
                self.assertEqual(len(r["iteration_completions"]), 300)

    def test_large_pool_matches_v1_for_random_dags_and_fifo(self):
        rng = random.Random(20261022)
        for case in range(24):
            ops = []
            for i in range(7):
                parents = rng.sample([o["id"] for o in ops], min(i, rng.randint(0, 2)))
                ops.append(op(str(i), rng.choice(["rdma", "wdma", "tensor", "vector"]), rng.randint(1, 9), *parents))
            c = config(ops, iterations=8, policy="ooo" if case % 2 else "fifo")
            base = v1.simulate(c)
            capacity = fixed_trace_requirement(base["records"])["minimum_for_fixed_trace"]
            r = simulate(c, capacity)
            self.assertEqual(r["makespan"], base["makespan"])
            self.assertEqual([(o["start"], o["end"]) for o in r["records"]],
                             [(o["start"], o["end"]) for o in base["records"]])

    def test_timeout_does_not_report_success(self):
        r = simulate({**config([op("a", "rdma", 20)]), "max_cycles": 3}, 2)
        self.assertEqual(r["status"], "timeout")
        self.assertIsNone(r["makespan"])
        self.assertIsNone(r["metrics"]["steady"])


if __name__ == "__main__":
    unittest.main()
