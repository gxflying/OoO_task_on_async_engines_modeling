"""可选 HTML 浏览器验收；仅 QA 使用 Playwright，模拟器本身无依赖。"""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", help="可选 Chromium / headless-shell 可执行文件")
    args = parser.parse_args()
    output = Path(__file__).resolve().parent / "out"
    summary = json.loads((output / "summary.json").read_text())
    fifo_summary = json.loads((output / "summary_fifo.json").read_text())
    fifo_records = json.loads((output / "trace_fifo.json").read_text())["instructions"]
    interval = summary["metrics"]["steady"]["ii"]
    default_span = min(summary["makespan"], int(6 * interval))
    errors = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.browser, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((output / "report.html").as_uri())
        page.wait_for_selector("#timeline svg")
        assert f"{interval:g}" in page.locator("#conclusion").inner_text()
        assert page.locator("#measurements tbody tr").count() == 10
        assert page.locator("#comparisons tbody tr").count() == 5
        assert page.locator("#measurement-image").evaluate("img => img.complete && img.naturalWidth > 0")
        assert page.locator("#timeline .op").count() > 20
        page.screenshot(path=str(output / "report_preview.png"), full_page=True)
        page.locator("#timeline").screenshot(path=str(output / "steady_hardware.png"))
        page.locator("#timeline .op").first.click()
        assert "入队" in page.locator("#selection").inner_text()
        page.locator("#layout").select_option("iteration")
        page.locator("#arrows").check()
        assert page.locator("#timeline .dep").count() > 10
        page.locator("#timeline").screenshot(path=str(output / "steady_pipe.png"))
        # 所有绘制方块宽度都来自时长（跨窗口边界时仅显示可见部分）。
        width_checks = page.evaluate("""() => {
          const d=JSON.parse(document.getElementById('report-data').textContent);
          const a=+document.getElementById('start').value,b=a+(+document.getElementById('span').value);
          const scale=(+document.querySelector('#timeline svg').getAttribute('width')-132)/(b-a);
          return [...document.querySelectorAll('#timeline .op')].every(g=>{
            const r=d.records[+g.dataset.seq], w=+g.querySelector('rect').getAttribute('width');
            return Math.abs(w-(Math.min(b,r.end)-Math.max(a,r.start))*scale)<1e-6;
          });
        }""")
        assert width_checks
        page.locator('[data-preset="startup"]').click()
        assert page.locator("#start").input_value() == "0"
        page.locator('[data-preset="tail"]').click()
        assert page.locator("#start").input_value() == str(summary["makespan"] - default_span)
        page.locator('[data-preset="all"]').click()
        assert page.locator("#timeline .op").count() == 1000
        page.locator('[data-preset="steady"]').click()
        page.locator("#layout").select_option("hardware")
        with page.expect_download() as download_info:
            page.locator("#export").click()
        download_info.value.save_as(output / "steady_pipeline.svg")

        # 新 tab 必须使用实际 FIFO 数据，不能只换标题或复用 OoO 方块。
        page.locator("#tab-fifo").click()
        assert page.locator("#tab-fifo").get_attribute("aria-selected") == "true"
        assert page.locator("#execution-panel").get_attribute("data-policy") == "fifo"
        assert "FIFO" in page.locator("#policy-summary").inner_text() or "顺序执行" in page.locator("#policy-summary").inner_text()
        assert str(fifo_summary["makespan"]) in page.locator("#policy-summary").inner_text()
        assert f'{fifo_summary["metrics"]["steady"]["ii"]:g}' in page.locator("#conclusion").inner_text()
        assert page.locator("#timeline .op rect[stroke-dasharray]").count() == 0
        assert page.locator("#selection").inner_text().startswith("点击方块")
        page.locator("#execution-panel").screenshot(path=str(output / "fifo_timeline.png"))
        page.locator("#layout").select_option("iteration")
        assert page.locator("#timeline .dep").count() > 0
        page.locator("#timeline").screenshot(path=str(output / "fifo_pipe.png"))
        page.locator('[data-preset="startup"]').click()
        assert page.locator("#start").input_value() == "0"
        page.locator('[data-preset="tail"]').click()
        fifo_span = min(fifo_summary["makespan"], int(6 * fifo_summary["metrics"]["steady"]["ii"]))
        assert page.locator("#start").input_value() == str(fifo_summary["makespan"] - fifo_span)
        page.locator("#layout").select_option("hardware")
        page.locator('[data-preset="all"]').click()
        assert page.locator("#timeline .op").count() == len(fifo_records)
        assert page.locator("#span").input_value() == str(fifo_summary["makespan"])
        assert page.evaluate("""records => {
          const start=+document.getElementById('start').value,end=start+(+document.getElementById('span').value);
          const scale=(+document.querySelector('#timeline svg').getAttribute('width')-132)/(end-start);
          return [...document.querySelectorAll('#timeline .op')].every(g=>{
            const r=records[+g.dataset.seq],rect=g.querySelector('rect');
            return Math.abs(+rect.getAttribute('x')-(110+(r.start-start)*scale))<1e-6 &&
              Math.abs(+rect.getAttribute('width')-r.duration*scale)<1e-6;
          });
        }""", fifo_records)
        chosen = fifo_records[14]
        page.locator(f'#timeline .op[data-seq="{chosen["seq"]}"]').dispatch_event("click")
        assert f'执行 [{chosen["start"]}, {chosen["end"]})' in page.locator("#selection").inner_text()
        page.locator('[data-preset="steady"]').click()
        with page.expect_download() as fifo_download:
            page.locator("#export").click()
        assert fifo_download.value.suggested_filename == "attention-pipeline-fifo.svg"
        fifo_download.value.save_as(output / "steady_pipeline_fifo.svg")
        # 键盘切回 OoO 后也应恢复真实数据与指标。
        page.locator("#tab-fifo").focus()
        page.keyboard.press("ArrowLeft")
        assert page.locator("#tab-ooo").get_attribute("aria-selected") == "true"
        assert str(summary["makespan"]) in page.locator("#policy-summary").inner_text()
        assert page.locator("#timeline .op rect[stroke-dasharray]").count() > 0
        page.set_viewport_size({"width": 390, "height": 844})
        page.screenshot(path=str(output / "report_mobile.png"), full_page=True)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.locator("#tab-fifo").click()
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert not errors, errors
        browser.close()
    print(json.dumps({"page_errors": errors, "tested": ["startup", "steady", "tail", "all 1000 instructions",
                     "iteration layout", "dependency arrows", "instruction selection", "width = time",
                     "SVG export", "390px mobile overflow", "10 measured instruction widths",
                     "5 paired comparison rows", "embedded measurement image", "FIFO tab actual trace coordinates",
                     "FIFO startup/tail/all/iteration views", "FIFO details and SVG export",
                     "keyboard tab switch restores OoO", "FIFO mobile layout"], "result": "PASS"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
