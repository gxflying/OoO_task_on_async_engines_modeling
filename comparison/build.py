"""用真实 trace 生成离线、独立的 v3 / v3.1 buffer 对比 HTML。"""
from collections import defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT = HERE / "out"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalize(version, key, r, source):
    assert r["status"] == "complete", (version, key, r["status"])
    records = [{k: x[k] for k in ("seq", "iteration", "op", "label", "engine", "start", "compute_end", "end")}
               for x in r["records"]]
    readers = defaultdict(list)
    for x in r["records"]:
        for handle in x["input_handles"]:
            readers[tuple(handle)].append(x["seq"])
    leases = []
    raw = r["leases"] if version == "v3" else r["buffer_leases"]
    for x in raw:
        seq = x["owner"] if version == "v3" else x["owner"][1]
        kind = x["kind"] if version == "v3" else ("source_input" if x["owner"][0] == "source" else "output")
        consumers = readers[x["slot"], x["generation"]]
        if version == "v3":
            assert sorted(consumers) == sorted(x["consumers"])
        p = records[seq]
        assert x["start"] <= p["start"] <= p["compute_end"] <= p["end"] <= x["end"]
        for c in consumers:
            assert x["start"] <= records[c]["start"] <= records[c]["end"] <= x["end"]
        if kind == "output":
            assert r["records"][seq]["output_handle"] == [x["slot"], x["generation"]]
        leases.append({k: x[k] for k in ("slot", "generation", "start", "end")}
                      | {"owner": seq, "kind": kind, "consumers": consumers,
                         "logical": f"{p['iteration']}:{p['op']}:{kind}"})
    for slot in range(r["buffer_count"]):
        history = sorted((x for x in leases if x["slot"] == slot), key=lambda x: x["generation"])
        assert all(a["end"] <= b["start"] for a, b in zip(history, history[1:]))
    for engine in ("rdma", "wdma", "tensor", "vector"):
        ordered = sorted((x for x in records if x["engine"] == engine), key=lambda x: x["start"])
        assert all(a["end"] <= b["start"] for a, b in zip(ordered, ordered[1:]))
    events = defaultdict(int)
    for x in leases:
        events[x["start"]] += 1
        events[x["end"]] -= 1
    live, curve = 0, []
    for time, delta in sorted(events.items()):
        live += delta
        curve.append([time, live])
    assert live == 0
    outputs = [x for x in leases if x["kind"] == "output"]
    pre = sum(records[x["owner"]]["start"] - x["start"] for x in outputs)
    steady = r["metrics"]["steady"]
    return {"version": version, "key": key, "buffers": r["buffer_count"], "barriers": r["barrier_count"],
            "policy": r["config"]["policy"], "iterations": r["config"]["iterations"], "queue_depth": r["config"]["queue_depth"],
            "time": r["time"], "ii": steady["ii"], "steady_start": steady["start"], "steady_end": steady["end"],
            "peak": r.get("peak_buffers", r.get("buffer_peak")),
            "mean_live": sum(x["end"] - x["start"] for x in leases) / r["time"],
            "mean_precompute": pre / len(outputs), "records": records, "leases": leases, "curve": curve,
            "source": source.relative_to(ROOT).as_posix(), "source_sha256": digest(source)}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    # 原始版本和既有报告保持不变；只补算缺失的相同资源配置。
    sys.path.insert(0, str(ROOT / "v3.1"))
    spec = importlib.util.spec_from_file_location("compare_v31", ROOT / "v3.1/simulator.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    configs = {v: read(ROOT / v / "config.json") for v in ("v3", "v3.1")}
    for field in ("iterations", "queue_depth", "operations", "count_source_inputs", "time_unit"):
        assert configs["v3"][field] == configs["v3.1"][field], field
    runs = {}
    for version, sizes in (("v3", [(9, 10), (11, 12)]), ("v3.1", [(9, 10), (11, 12), (13, 11)])):
        sweep = read(ROOT / version / "out/summary.json")["sweep"]
        for buffers, barriers in sizes:
            for policy in ("ooo", "fifo"):
                key = f"b{buffers}_m{barriers}_{policy}"
                source = ROOT / version / "out" / f"trace_{key}.json"
                if not source.exists():
                    trace = module.simulate({**configs[version], "policy": policy}, buffers, barriers)
                    source = OUT / f"trace_v31_{key}.json"
                    source.write_text(json.dumps(trace, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                    provenance = "补算并通过模型内置校验"
                else:
                    trace = read(source)
                    provenance = "既有 trace"
                normalized = normalize(version, key, trace, source)
                prior = next(row for row in sweep if row["buffers"] == buffers and
                             row.get("barriers", row.get("ids")) == barriers and row["policy"] == policy)
                assert normalized["ii"] == prior["ii"] and trace["makespan"] == prior["makespan"]
                runs[f"{version}:{key}"] = normalized
                print(f"{version} {key}: II={normalized['ii']}, 总时间={trace['time']} ({provenance})", flush=True)
    data = {"runs": runs, "operations": configs["v3"]["operations"], "scenarios": [
        {"id": "target", "label": "各版 II=298 的代表配置（OoO）", "v3": [9, 10], "v3.1": [13, 11]},
        {"id": "same9", "label": "相同容量：9 buffer / 10 barrier", "v3": [9, 10], "v3.1": [9, 10]},
        {"id": "same11", "label": "相同容量：11 buffer / 12 barrier", "v3": [11, 12], "v3.1": [11, 12]}]}
    packed = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    html = (HERE / "report_template.html").read_text(encoding="utf-8").replace("__DATA__", packed)
    (OUT / "buffer_comparison.html").write_text(html, encoding="utf-8")
    sources = {r["source"]: r["source_sha256"] for r in runs.values()}
    sources.update({str(p.relative_to(ROOT)).replace("\\", "/"): digest(p)
                    for v in ("v3", "v3.1") for p in (ROOT / v / "config.json", ROOT / v / "simulator.py")})
    (OUT / "manifest.json").write_text(json.dumps({"sources_sha256": sources, "cases": len(runs),
        "validation": "模型原始 sweep II/makespan 一致；输入引用、占用区间、复用不重叠、engine 不重叠通过"},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"PASS: {OUT / 'buffer_comparison.html'}")


if __name__ == "__main__":
    main()
