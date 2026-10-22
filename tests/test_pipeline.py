"""行为回归：手工可核对的小程序、乱序见证、完整工作负载与稳态。"""
import copy
import json
from pathlib import Path
import random
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import ENGINES, HERE, simulate, verify_schedule, window_metrics, write_outputs


def op(name, engine, duration, *deps):
    return {"id": name, "engine": engine, "duration": duration, "deps": list(deps)}


def config(ops, iterations=1, depth=5, policy="ooo"):
    return {"operations": ops, "iterations": iterations, "queue_depth": depth,
            "policy": policy, "max_cycles": 100000}


class PipelineTests(unittest.TestCase):
    def test_report_contains_actual_ooo_and_fifo_traces(self):
        ops = [op("load", "rdma", 6), op("blocked", "tensor", 2, "load"),
               op("ready", "tensor", 3)]
        for policy in ("ooo", "fifo"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as folder:
                r = simulate(config(ops, policy=policy))
                write_outputs(r, [], Path(folder))
                html = (Path(folder) / "report.html").read_text()
                payload = json.loads(re.search(r'<script id="report-data" type="application/json">(.*?)</script>', html, re.S)[1])
                other = "fifo" if policy == "ooo" else "ooo"
                runs = {policy: payload, other: payload["policy_runs"][other]}
                self.assertEqual(runs["ooo"]["makespan"], 8)
                self.assertEqual(runs["fifo"]["makespan"], 11)
                self.assertEqual(runs["ooo"]["records"][2]["start"], 0)
                self.assertEqual(runs["fifo"]["records"][2]["start"], 8)
                self.assertEqual(runs["ooo"]["records"][2]["bypassed"], [1])
                self.assertTrue(all(not op["bypassed"] for op in runs["fifo"]["records"]))
                saved = json.loads((Path(folder) / f"trace_{other}.json").read_text())
                self.assertEqual(saved["instructions"], runs[other]["records"])
                self.assertNotEqual(runs["ooo"]["config_sha256"], runs["fifo"]["config_sha256"])

    def test_zero_frontend_and_same_time_refill(self):
        r = simulate(config([op("a", "rdma", 2), op("b", "rdma", 2), op("c", "rdma", 2)], depth=1))
        self.assertEqual([(x["enqueue"], x["start"], x["end"]) for x in r["records"]],
                         [(0, 0, 2), (0, 2, 4), (2, 4, 6)])
        self.assertEqual(r["makespan"], 6)

    def test_frontend_blocking_is_global_but_execution_is_async(self):
        r = simulate(config([op("a", "rdma", 3), op("b", "rdma", 3),
                             op("c", "rdma", 3), op("d", "vector", 1)], depth=1))
        self.assertEqual([x["enqueue"] for x in r["records"]], [0, 0, 3, 3])
        self.assertEqual([x["start"] for x in r["records"]], [0, 3, 6, 3])
        self.assertEqual(r["cycles"][0]["blocked_engine"], "rdma")

    def test_ooo_bypasses_blocked_head(self):
        ops = [op("load", "rdma", 6), op("blocked", "tensor", 2, "load"),
               op("ready", "tensor", 3)]
        ooo = simulate(config(ops))
        fifo = simulate(config(ops, policy="fifo"))
        self.assertEqual([x["start"] for x in ooo["records"]], [0, 6, 0])
        self.assertEqual(ooo["records"][2]["bypassed"], [1])
        self.assertEqual(fifo["makespan"], 11)
        self.assertEqual(ooo["makespan"], 8)

    def test_oldest_ready_has_priority(self):
        r = simulate(config([op("a", "tensor", 5), op("b", "tensor", 2), op("c", "tensor", 1)]))
        self.assertEqual([x["start"] for x in r["records"]], [0, 5, 7])

    def test_fork_join_waits_for_all_producers(self):
        r = simulate(config([op("a", "rdma", 3), op("b", "tensor", 8),
                             op("join", "vector", 2, "a", "b")]))
        self.assertEqual([x["start"] for x in r["records"]], [0, 0, 8])
        self.assertEqual(r["makespan"], 10)

    def test_completed_token_is_persistent(self):
        r = simulate(config([op("a", "rdma", 1), op("b", "vector", 10),
                             op("late", "tensor", 2, "a", "b")], depth=1))
        self.assertEqual(r["records"][2]["start"], 10)

    def test_iter_tokens_are_separate_and_iterations_overlap(self):
        r = simulate(config([op("a", "rdma", 2), op("b", "vector", 5, "a")], iterations=3))
        self.assertEqual([x["start"] for x in r["records"]], [0, 2, 2, 7, 4, 12])
        self.assertEqual(r["records"][5]["deps"], [4])

    def test_screenshot_single_iteration_by_hand(self):
        c = json.loads((HERE / "config_approximate.json").read_text())
        c["iterations"] = 1
        r = simulate(c)
        expected = {"load1R1": (0, 9), "wdma1R1": (9, 14), "load1R2": (9, 18),
                    "wdma1R2": (18, 23), "tensor1R1": (14, 26), "vector1R1": (26, 35),
                    "vector1R2": (35, 40), "tensor1R2": (40, 51), "vector1R3": (40, 47),
                    "vector1R4": (51, 60)}
        self.assertEqual({x["op"]: (x["start"], x["end"]) for x in r["records"]}, expected)
        self.assertIsNone(r["steady"])

    def test_steady_state_matches_longer_run_and_resource_bound(self):
        c = json.loads((HERE / "config_approximate.json").read_text())
        short = simulate(c)
        long = simulate({**c, "iterations": 200})
        self.assertEqual(short["metrics"]["steady"]["ii"], 30)
        self.assertEqual(long["metrics"]["steady"]["ii"], 30)
        self.assertEqual(short["metrics"]["steady"]["bound_efficiency"], 1)
        self.assertEqual(long["makespan"] - short["makespan"], 100 * 30)
        s = short["steady"]
        self.assertEqual(short["metrics"]["steady"]["units"]["vector"]["idle_dependency"], 0)
        # 在不同的未排空窗口核对固定周期，不把尾部平均数当作稳态证据。
        for start in (s["start"], s["start"] + 30 * 10, s["start"] + 30 * 50):
            m = window_metrics(short, start, start + 30 * 10)
            self.assertEqual(m["completed_iterations"], 10)
            self.assertEqual(m["units"]["vector"]["utilization"], 1)

    def test_depth_and_fifo_change_actual_schedule(self):
        c = json.loads((HERE / "config_approximate.json").read_text())
        for depth, policy, expected in [(1, "ooo", 40), (2, "ooo", 37), (3, "ooo", 37),
                                        (4, "ooo", 30), (5, "ooo", 30), (5, "fifo", 37)]:
            with self.subTest(depth=depth, policy=policy):
                r = simulate({**c, "queue_depth": depth, "policy": policy})
                self.assertEqual(r["metrics"]["steady"]["ii"], expected)

    def test_no_fake_stability_for_short_run(self):
        r = simulate(config([op("a", "rdma", 7)], iterations=2))
        self.assertIsNone(r["steady"])
        self.assertIsNone(r["metrics"]["steady"])

    def test_per_engine_depth_configuration(self):
        c = json.loads((HERE / "config_approximate.json").read_text())
        c["queue_depth"] = {"rdma": 1, "wdma": 2, "tensor": 3, "vector": 5}
        r = simulate(c)
        for e in ENGINES:
            self.assertLessEqual(max(len(t["queues"][e]) for t in r["cycles"]), c["queue_depth"][e])

    def test_invalid_input_and_cycles(self):
        base = config([op("a", "rdma", 1)])
        for k, v in [("queue_depth", 0), ("iterations", -1), ("policy", "random")]:
            with self.subTest(k=k), self.assertRaises(ValueError):
                simulate({**base, k: v})
        for ops in [[op("a", "rdma", 0)], [op("a", "rdma", 1, "unknown")],
                    [op("a", "rdma", 1, "b"), op("b", "tensor", 1, "a")]]:
            with self.assertRaises(ValueError):
                simulate(config(ops))

    def test_deadlock_and_timeout_are_reported(self):
        # 非拓扑前端序会把生产者挡在满队列之后，必须报错，不能伪造完成。
        ops = [op("a", "tensor", 1, "c"), op("b", "tensor", 1), op("c", "rdma", 1)]
        with self.assertRaisesRegex(RuntimeError, "死锁"):
            simulate(config(ops, depth=1))
        with self.assertRaisesRegex(RuntimeError, "max_cycles"):
            simulate({**config([op("a", "rdma", 10)]), "max_cycles": 3})

    def test_verifier_rejects_corrupted_dependency_and_overlap(self):
        r = simulate(config([op("a", "rdma", 3), op("b", "vector", 2, "a")]))
        broken = copy.deepcopy(r)
        broken["records"][1].update(start=0, end=2)
        with self.assertRaisesRegex(AssertionError, "依赖"):
            verify_schedule(broken)
        r = simulate(config([op("a", "rdma", 3), op("b", "rdma", 3)]))
        r["records"][1].update(start=2, end=5)
        with self.assertRaisesRegex(AssertionError, "冲突"):
            verify_schedule(r)

    def test_random_dags_obey_work_conservation_and_independent_dependencies(self):
        rng = random.Random(20261022)
        for case in range(30):
            ops = []
            for i in range(rng.randint(3, 12)):
                deps = rng.sample([o["id"] for o in ops], k=min(len(ops), rng.randint(0, 2)))
                ops.append(op(str(i), rng.choice(ENGINES), rng.randint(1, 9), *deps))
            r = simulate(config(ops, iterations=12, depth=rng.randint(1, 6)))
            with self.subTest(case=case):
                for e in ENGINES:
                    expected = sum(o["duration"] for o in ops if o["engine"] == e) * 12
                    m = r["metrics"]["full"]["units"][e]
                    self.assertEqual(m["busy"], expected)
                    self.assertEqual(m["busy"] + m["idle_empty"] + m["idle_dependency"], r["makespan"])
                self.assertEqual(len(r["iteration_completions"]), 12)
                if r["metrics"]["steady"]:
                    self.assertLessEqual(r["metrics"]["steady"]["bound_efficiency"], 1)

    def test_measured_pixels_match_default_configuration(self):
        c = json.loads((HERE / "config.json").read_text())
        expected = {"load1R1": 87, "wdma1R1": 46, "load1R2": 87, "wdma1R2": 46,
                    "tensor1R1": 118, "vector1R1": 88, "vector1R2": 53,
                    "tensor1R2": 111, "vector1R3": 65, "vector1R4": 92}
        self.assertEqual({o["id"]: o["duration"] for o in c["operations"]}, expected)
        for row in c["timing_source"]["blocks"]:
            self.assertEqual(row["width_px"], row["x_right"] - row["x_left"])
            self.assertEqual(row["width_px"], row["attention_x_right"] - row["attention_x_left"])

    def test_pixel_single_iteration_hand_calculation(self):
        c = json.loads((HERE / "config.json").read_text())
        r = simulate({**c, "iterations": 1})
        expected = {"load1R1": (0, 87), "wdma1R1": (87, 133), "load1R2": (87, 174),
                    "wdma1R2": (174, 220), "tensor1R1": (133, 251), "vector1R1": (251, 339),
                    "vector1R2": (339, 392), "tensor1R2": (392, 503), "vector1R3": (392, 457),
                    "vector1R4": (503, 595)}
        self.assertEqual({x["op"]: (x["start"], x["end"]) for x in r["records"]}, expected)
        self.assertEqual(r["makespan"], 595)

    def test_pixel_sweep_and_two_iteration_period(self):
        c = json.loads((HERE / "config.json").read_text())
        for depth in range(1, 6):
            for policy in ("ooo", "fifo"):
                with self.subTest(depth=depth, policy=policy):
                    r = simulate({**c, "queue_depth": depth, "policy": policy})
                    expected = 392 if depth == 1 else 370
                    if policy == "ooo" and depth >= 3:
                        expected = 320.5 if depth == 3 else 298
                    self.assertEqual(r["metrics"]["steady"]["ii"], expected)
                    if policy == "ooo" and depth == 3:
                        self.assertEqual(r["steady"]["period"], 641)
                        self.assertEqual(r["steady"]["iterations_per_period"], 2)

    def test_pixel_periods_hold_over_200_iterations(self):
        c = json.loads((HERE / "config.json").read_text())
        for depth, interval in [(3, 320.5), (5, 298)]:
            with self.subTest(depth=depth):
                a = simulate({**c, "queue_depth": depth})
                b = simulate({**c, "queue_depth": depth, "iterations": 200})
                self.assertEqual(b["metrics"]["steady"]["ii"], interval)
                self.assertEqual(b["makespan"] - a["makespan"], 100 * interval)
                period = a["steady"]["period"]
                count = a["steady"]["iterations_per_period"]
                for start in [a["steady"]["start"], a["steady"]["start"] + 20 * period]:
                    window = window_metrics(a, start, start + 10 * period)
                    self.assertEqual(window["completed_iterations"], 10 * count)


if __name__ == "__main__":
    unittest.main()
