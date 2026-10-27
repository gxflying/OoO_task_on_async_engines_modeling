import copy
import json
from pathlib import Path
import random
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from resources import BlockingAPI, EventPool
from simulator import HERE, Context, Kernel, simulate, verify, v1


def op(name, engine, duration, *deps):
    return {"id": name, "engine": engine, "duration": duration, "deps": list(deps)}


def cfg(ops, iterations=1, source=True, policy="ooo"):
    return {"operations": ops, "iterations": iterations, "count_source_inputs": source,
            "queue_depth": 5, "max_cycles": 100000, "policy": policy}


class ModelTests(unittest.TestCase):
    def test_two_bitmaps_and_two_namespaces_are_independent(self):
        a, b = EventPool(2, "buffer"), EventPool(2, "barrier")
        x = a.allocate_if_free(["output", 0], 0, "test")
        y = b.allocate_if_free(["data", 0, 1], 0, "test")
        self.assertEqual((x.slot, y.slot), (0, 0))
        self.assertEqual((a.free_bitmap, a.flag_bitmap), (2, 0))
        a.set([x], 0, "test")
        self.assertEqual(b.flag_bitmap, 0)
        a.consume([x], 3, "test")
        self.assertEqual(a.free_bitmap, 3)
        self.assertEqual(b.free_bitmap, 2)

    def test_wait_requires_set_and_reuse_rejects_old_generation(self):
        p = EventPool(1, "barrier")
        old = p.allocate_if_free(["data", 0, 1], 0, "test")
        with self.assertRaises(AssertionError):
            p.consume([old], 0, "test")
        p.set([old], 2, "test")
        p.consume([old], 2, "test")
        new = p.allocate_if_free(["release", 2, 3], 2, "test")
        self.assertEqual(new.slot, old.slot)
        self.assertEqual(new.generation, old.generation + 1)
        with self.assertRaises(AssertionError):
            p.lookup(old)

    def test_blocking_api_does_not_return_failure_to_caller(self):
        api = BlockingAPI()
        call = api.get_buffer(["source", 0])
        request = next(call)
        self.assertEqual(request.kind, "get_buffer")
        self.assertIsNotNone(call.gi_frame)  # 仍挂在 API 内，未返回 None。
        p = EventPool(1, "buffer")
        h = p.allocate_if_free(["source", 0], 0, "test")
        with self.assertRaises(StopIteration) as completed:
            call.send(h)
        self.assertEqual(completed.exception.value, h)

    def test_partial_frontend_allocation_is_retained_on_block(self):
        k = Kernel(cfg([op("A", "rdma", 3)]), 1, 2)
        r = k.run()
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(k.front.request.kind, "get_buffer")
        self.assertFalse(k.front.done)
        self.assertEqual(k.front.request.args, ["output", 0])
        self.assertEqual(len(r["buffer_leases"]), 1)
        self.assertIsNone(r["buffer_leases"][0]["end"])
        self.assertIsNone(r["records"][0]["enqueue"])

    def test_free_api_really_suspends_until_buffer_flag_is_set(self):
        k = Kernel(cfg([op("A", "rdma", 3)]), 1, 0)
        h = k.buffer.allocate_if_free(["output", 0], 0, "setup")
        ctx = Context("reclaim", 0, k.api.free_buffer(h))
        self.assertFalse(k.service(ctx))
        self.assertFalse(ctx.done)
        self.assertEqual(ctx.request.kind, "free_buffer")
        self.assertEqual(k.buffer.free_bitmap, 0)
        k.buffer.set([h], 0, "setup")
        self.assertTrue(k.service(ctx))
        self.assertTrue(ctx.done)
        self.assertEqual(k.buffer.free_bitmap, 1)

    def test_barrier_get_wakes_on_wait_recycle_without_serializing_iterations(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "vector", 3, "A")], iterations=2), 6, 1)
        self.assertEqual(r["status"], "complete")
        a0, b0, a1, b1 = r["records"]
        self.assertEqual((a1["start"], b0["start"], b0["end"]), (2, 2, 5))
        first, second = r["barrier_leases"]
        self.assertEqual((first["end"], second["start"]), (2, 2))
        self.assertEqual(first["slot"], second["slot"])
        self.assertEqual(second["generation"], first["generation"] + 1)

    def test_barrier_block_is_before_submit_and_does_not_create_empty_ids(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "vector", 1, "A")]), 4, 0)
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(r["deadlock"]["frontend"]["api"], "get_barrier")
        self.assertEqual(len(r["buffer_leases"]), 2)
        self.assertEqual(len(r["barrier_leases"]), 0)
        self.assertIsNone(r["records"][0]["enqueue"])

    def test_simple_chain_by_hand(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "vector", 3, "A")]), 3, 1)
        self.assertEqual(r["makespan"], 5)
        self.assertEqual([(x["start"], x["end"]) for x in r["records"]], [(0, 2), (2, 5)])
        event = r["barrier_leases"][0]
        self.assertEqual((event["start"], event["set"], event["end"]), (0, 2, 2))
        data = next(x for x in r["buffer_leases"] if x["owner"] == ["output", 0])
        self.assertEqual(data["end"], 5)

    def test_fanout_has_two_data_and_two_completion_ids_and_one_free(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "vector", 3, "A"),
                          op("C", "tensor", 7, "A")]), 6, 8)
        self.assertEqual(r["makespan"], 9)
        self.assertEqual([x["owner"] for x in r["barrier_leases"]],
                         [["data", 0, 1], ["data", 0, 2], ["release", 0, 1], ["release", 0, 2]])
        output = next(l for l in r["buffer_leases"] if l["owner"] == ["output", 0])
        self.assertEqual(output["end"], 9)
        self.assertEqual(output["freed_by"], ["reclaim", 0])
        self.assertEqual(r["reclaimers"], [{"producer": 0, "registered": 0, "end": 9}])
        self.assertTrue(all(l["end"] is not None for l in r["barrier_leases"]))

    def test_fanin_wait_all_does_not_consume_early_half(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "wdma", 5),
                          op("C", "vector", 1, "A", "B")]), 5, 2)
        self.assertEqual(r["makespan"], 6)
        self.assertEqual([l["end"] for l in r["barrier_leases"]], [5, 5])

    def test_one_engine_remains_serial_without_data_dependencies(self):
        r = simulate(cfg([op("A", "rdma", 3)], iterations=4), 2, 0)
        self.assertEqual([x["start"] for x in r["records"]], [0, 3, 6, 9])
        self.assertEqual(r["makespan"], 12)

    def test_queued_task_preallocates_but_does_not_execute(self):
        r = simulate(cfg([op("A", "rdma", 3), op("B", "rdma", 3)]), 4, 0)
        self.assertEqual(r["records"][1]["enqueue"], 0)
        self.assertEqual(r["records"][1]["start"], 3)
        self.assertEqual(r["buffer_peak"], 4)

    def test_completion_exactly_at_time_limit(self):
        c = {**cfg([op("A", "rdma", 3)]), "max_cycles": 3}
        self.assertEqual(simulate(c, 2, 0)["status"], "complete")
        self.assertEqual(simulate({**c, "max_cycles": 2}, 2, 0)["status"], "timeout")

    def test_bad_alias_or_early_release_cannot_pass_verifier(self):
        r = simulate(cfg([op("A", "rdma", 2), op("B", "vector", 3, "A")]), 4, 3)
        bad = copy.deepcopy(r)
        next(l for l in bad["buffer_leases"] if l["owner"] == ["output", 0])["end"] = 2
        with self.assertRaises(AssertionError):
            verify(bad)
        bad = copy.deepcopy(r)
        bad["records"][1]["output_handle"] = bad["records"][1]["input_handles"][0]
        with self.assertRaises(AssertionError):
            verify(bad)

    def test_generous_pools_match_existing_v1_compute_timing(self):
        c = json.loads((HERE / "config.json").read_text())
        for policy in ("ooo", "fifo"):
            with self.subTest(policy=policy):
                local = {**c, "policy": policy, "iterations": 30}
                base = v1.simulate(local)
                r = simulate(local, 128, 128)
                self.assertEqual([(x["start"], x["end"]) for x in r["records"]],
                                 [(x["start"], x["end"]) for x in base["records"]])
                self.assertEqual(sum(l["owner"][0] == "data" for l in r["barrier_leases"]), 300)
                self.assertEqual(sum(l["owner"][0] == "release" for l in r["barrier_leases"]), 60)
                self.assertEqual(len(r["buffer_leases"]), 360)

    def test_random_topological_dags_have_valid_lifetimes_and_match_reference(self):
        rng = random.Random(20261026)
        for case in range(12):
            ops = []
            for i in range(7):
                parents = rng.sample([o["id"] for o in ops], min(i, rng.randint(0, 3)))
                ops.append(op(str(i), rng.choice(["rdma", "wdma", "tensor", "vector"]), rng.randint(1, 6), *parents))
            c = cfg(ops, iterations=6, policy="ooo" if case % 2 else "fifo")
            r, base = simulate(c, 128, 128), v1.simulate(c)
            self.assertEqual(r["makespan"], base["makespan"])
            self.assertFalse(r["final_buffers"]["live"])
            self.assertFalse(r["final_barriers"]["live"])


if __name__ == "__main__":
    unittest.main()
