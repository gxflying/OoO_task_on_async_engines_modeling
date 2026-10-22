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
    errors = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.browser, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((output / "report.html").as_uri())
        page.wait_for_selector("#timeline svg")
        assert "30" in page.locator("#conclusion").inner_text()
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
        assert page.locator("#start").input_value() == "2850"
        page.locator('[data-preset="all"]').click()
        assert page.locator("#timeline .op").count() == 1000
        page.locator('[data-preset="steady"]').click()
        page.locator("#layout").select_option("hardware")
        with page.expect_download() as download_info:
            page.locator("#export").click()
        download_info.value.save_as(output / "steady_pipeline.svg")
        page.set_viewport_size({"width": 390, "height": 844})
        page.screenshot(path=str(output / "report_mobile.png"), full_page=True)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert not errors, errors
        browser.close()
    print(json.dumps({"page_errors": errors, "tested": ["startup", "steady", "tail", "all 1000 instructions",
                     "iteration layout", "dependency arrows", "instruction selection", "width = time",
                     "SVG export", "390px mobile overflow"], "result": "PASS"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
