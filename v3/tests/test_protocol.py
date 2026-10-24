"""从实际发布/WAIT 与 buffer 生命周期验证 full/empty 协议。"""
import json
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simulator import simulate, v2, verify


def op(name, engine, duration, *deps):
    return {"id": name, "engine": engine, "duration": duration, "deps": list(deps)}


def config(operations, iterations=1, policy="ooo"):
    return {"operations": operations, "iterations": iterations, "queue_depth": 5,
            "max_cycles": 1000000, "policy": policy, "count_source_inputs": True}


def full_lease(result, producer, consumer):
    matches = [lease for lease in result["barrier_leases"]
               if lease["kind"] == "full" and list(lease["owner"]) == [producer, consumer]]
    if len(matches) != 1:
        raise AssertionError(f"依赖边 {producer}->{consumer} 的 full ID 必须恰好一个: {matches}")
    return matches[0]


def output_lease(result, owner):
    matches = [lease for lease in result["leases"]
               if lease["kind"] == "output" and lease["owner"] == owner]
    if len(matches) != 1:
        raise AssertionError(f"指令 {owner} 的 output 必须恰好一个: {matches}")
    return matches[0]


class FullEmptyProtocolTests(unittest.TestCase):
    def test_initial_empty_ids_are_real_capacity_occupants(self):
        result = simulate(config([op("A", "rdma", 2)]), 3, 2)
        self.assertEqual(result["status"], "initialization_blocked")
        self.assertEqual(result["time"], 0)
        self.assertIsNone(result["makespan"])
        self.assertTrue(all(record["start"] is None for record in result["records"]))

    def test_fanout_publish_deadlock_cannot_release_source_early(self):
        cfg = config([op("A", "rdma", 2), op("B", "vector", 1, "A"),
                      op("C", "tensor", 1, "A")])
        result = simulate(cfg, 3, 3)
        self.assertEqual(result["status"], "deadlock")
        self.assertEqual(result["time"], 2)
        self.assertIsNone(result["makespan"])
        a, b, c = result["records"]
        self.assertEqual((a["start"], a["compute_end"], a["end"]), (0, 2, None))
        self.assertIsNone(b["start"])
        self.assertIsNone(c["start"])
        self.assertEqual(len(result["final_buffers"]), 2)
        self.assertTrue(all(lease["end"] is None for lease in result["leases"]))
        # 必须原子申请 2 full + 1 source empty，不能发布部分 full 后等待。
        self.assertFalse(any(lease["kind"] == "full"
                             for lease in result["barrier_leases"]))
        recovered = simulate(cfg, 3, 4)
        self.assertEqual(recovered["status"], "complete")
        self.assertEqual(recovered["makespan"], 3)

    def test_fanin_wait_is_atomic_and_keeps_early_full_ready(self):
        result = simulate(config([op("A", "rdma", 2), op("B", "wdma", 5),
                                  op("C", "tensor", 1, "A", "B")]), 5, 10)
        self.assertEqual(result["status"], "complete")
        a, b, c = result["records"]
        self.assertEqual((a["end"], b["end"], c["start"]), (2, 5, 5))
        early, late = full_lease(result, 0, 2), full_lease(result, 1, 2)
        self.assertEqual((early["set_time"], early["released_time"]), (2, 5))
        self.assertEqual((late["set_time"], late["released_time"]), (5, 5))
        waits = [event for event in result["barrier_events"]
                 if event["event"] == "wait" and event["kind"] == "full"]
        self.assertEqual(len(waits), 2)
        self.assertTrue(all(event["time"] == c["start"] for event in waits))
        self.assertEqual(len(c["wait_full"]), 2)

    def test_fanout_empty_is_set_only_after_last_reader_finishes(self):
        result = simulate(config([op("A", "rdma", 2), op("B", "vector", 3, "A"),
                                  op("C", "tensor", 7, "A")]), 4, 8)
        self.assertEqual(result["status"], "complete")
        self.assertEqual([record["start"] for record in result["records"]], [0, 2, 2])
        self.assertEqual([record["end"] for record in result["records"]], [2, 5, 9])
        data = output_lease(result, 0)
        self.assertEqual((data["start"], data["end"]), (0, 9))
        empty = [lease for lease in result["barrier_leases"]
                 if lease["kind"] == "empty"
                 and list(lease["owner"]) == [data["slot"], data["generation"]]]
        self.assertEqual(len(empty), 1)
        self.assertEqual(empty[0]["set_time"], 9)
        self.assertEqual(full_lease(result, 0, 1)["released_time"], 2)
        self.assertEqual(full_lease(result, 0, 2)["released_time"], 2)

    def test_full_is_released_at_consumer_start_but_input_remains_live(self):
        result = simulate(config([op("A", "rdma", 2), op("B", "vector", 8, "A")]), 3, 6)
        self.assertEqual(result["status"], "complete")
        barrier, data = full_lease(result, 0, 1), output_lease(result, 0)
        self.assertEqual(barrier["released_time"], result["records"][1]["start"])
        self.assertEqual(data["end"], result["records"][1]["end"])
        self.assertLess(barrier["released_time"], data["end"])

    def test_wait_release_retries_blocked_completion_in_same_timestamp(self):
        # X 的三条边占用 ID。A 算完后因无法原子发布保持执行单元和输入；
        # P 完成、Q 发射释放 ID 后，A 必须在同一 t=6 发布，无额外时钟空泡。
        cfg = config([op("A", "rdma", 2), op("X", "vector", 1),
                      op("P", "vector", 5, "X"), op("Q", "vector", 1, "X"),
                      op("R", "vector", 1, "X"), op("B", "wdma", 1, "A"),
                      op("C", "tensor", 1, "A")])
        result = simulate(cfg, 4, 4)
        self.assertEqual(result["status"], "complete")
        a, x, p, q, r, b, c = result["records"]
        self.assertEqual((a["start"], a["compute_end"], a["end"]), (0, 2, 6))
        self.assertEqual((p["end"], q["start"]), (6, 6))
        for segment in result["segments"]:
            if segment["start"] < 6 and segment["end"] > 2:
                self.assertEqual(segment["running"]["rdma"], 0)
        source = next(lease for lease in result["leases"]
                      if lease["owner"] == 0 and lease["kind"] == "source_input")
        self.assertEqual(source["end"], 6)
        self.assertEqual(full_lease(result, 0, 5)["set_time"], 6)
        self.assertEqual(full_lease(result, 0, 6)["set_time"], 6)

    def test_sufficient_ids_preserve_v2_timing_for_attention_pipeline(self):
        source = Path(__file__).resolve().parents[2] / "v2" / "config.json"
        cfg = json.loads(source.read_text(encoding="utf-8"))
        cfg["iterations"] = 30
        for policy in ("ooo", "fifo"):
            for buffers in (9, 11):
                with self.subTest(policy=policy, buffers=buffers):
                    cfg["policy"] = policy
                    base = v2.simulate(cfg, buffers)
                    result = simulate(cfg, buffers, 2 * buffers)
                    self.assertEqual(result["status"], "complete")
                    self.assertEqual(result["makespan"], base["makespan"])
                    self.assertEqual([(r["start"], r["end"]) for r in result["records"]],
                                     [(r["start"], r["end"]) for r in base["records"]])
                    self.assertTrue(all(r["compute_end"] == r["end"]
                                        for r in result["records"]))

    def test_completion_leaves_only_one_ready_empty_per_buffer(self):
        result = simulate(config([op("A", "rdma", 2), op("B", "vector", 3, "A"),
                                  op("C", "tensor", 2, "A"),
                                  op("D", "wdma", 1, "B", "C")], iterations=4), 5, 10)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["final_buffers"], [])
        final = result["final_barriers"]
        self.assertEqual(len(final), 5)
        self.assertTrue(all(item["kind"] == "empty" and item["ready"] for item in final))
        self.assertEqual({item["owner"][0] for item in final}, set(range(5)))
        self.assertFalse(any(lease["kind"] == "full" and lease["released_time"] is None
                             for lease in result["barrier_leases"]))
        for lease in result["barrier_leases"]:
            self.assertEqual(lease["mask"].bit_count(), 1)
        live = 0
        for seq, event in enumerate(result["barrier_events"]):
            self.assertEqual(event["seq"], seq)
            live += (event["event"] == "allocate") - (event["event"] == "free")
            self.assertLessEqual(live, 10)
            self.assertGreaterEqual(live, 0)
        self.assertEqual(live, 5)

    def test_completion_exactly_at_limit_is_allowed(self):
        cfg = {**config([op("A", "rdma", 3)]), "max_cycles": 3}
        result = simulate(cfg, 2, 4)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["makespan"], 3)
        short = simulate({**cfg, "max_cycles": 2}, 2, 4)
        self.assertEqual(short["status"], "timeout")
        self.assertIsNone(short["records"][0]["end"])

    def test_verifier_rejects_full_empty_reference_swapping(self):
        result = simulate(config([op("A", "rdma", 2), op("B", "vector", 3, "A")]), 3, 6)
        broken = copy.deepcopy(result)
        a, b = broken["records"][1]["wait_full"][0], broken["records"][1]["wait_empty"][0]
        for field in ("slot", "generation", "mask"):
            a[field], b[field] = b[field], a[field]
        with self.assertRaisesRegex(AssertionError, "错配"):
            verify(broken)

    def test_verifier_rejects_empty_owner_wrong_buffer_generation(self):
        result = simulate(config([op("A", "rdma", 2)]), 2, 4)
        broken = copy.deepcopy(result)
        ref = broken["records"][0]["wait_empty"][0]
        lease = next(l for l in broken["barrier_leases"] if (l["slot"], l["generation"]) == (ref["slot"], ref["generation"]))
        ref["owner"][1] = 99
        lease["owner"][1] = 99
        with self.assertRaisesRegex(AssertionError, "上一代"):
            verify(broken)

    def test_verifier_rejects_omitted_empty_wait_or_source_input(self):
        result = simulate(config([op("A", "rdma", 2)]), 2, 4)
        for field in ("wait_empty", "input_handles"):
            with self.subTest(field=field):
                broken = copy.deepcopy(result)
                broken["records"][0][field] = []
                with self.assertRaises(AssertionError):
                    verify(broken)


if __name__ == "__main__":
    unittest.main()
