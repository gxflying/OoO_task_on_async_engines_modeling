"""v2 HTML 实际渲染、物理槽位视图和交互验证。"""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser")
    args = parser.parse_args()
    out = Path(__file__).resolve().parent / "out"
    errors = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.browser, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((out / "report.html").as_uri())
        page.wait_for_selector("#memory svg")
        assert "9 块" in page.locator("#conclusion").inner_text()
        assert "11 块" in page.locator("#conclusion").inner_text()
        assert page.locator("#sweep tbody tr").count() == 12
        assert page.locator("#memory [data-lease]").count() > 20
        page.screenshot(path=str(out / "report_preview.png"), full_page=True)
        page.locator("#memory").screenshot(path=str(out / "buffer_lifetimes.png"))
        page.locator("#engines [data-op]").first.click()
        assert "输出" in page.locator("#detail").inner_text()
        page.locator("#memory [data-lease]").first.click()
        assert "释放" in page.locator("#detail").inner_text()
        page.locator("#cursor").fill("800")
        page.locator("#cursor").dispatch_event("input")
        assert "t=800" in page.locator("#snapshot").inner_text()
        assert page.evaluate("""() => {
          const a=+document.getElementById('start').value,b=a+(+document.getElementById('span').value);
          const scale=(+document.querySelector('#memory svg').getAttribute('width')-137)/(b-a);
          return [...document.querySelectorAll('#memory [data-lease]')].every(g=>{
            const l=R.leases[+g.dataset.lease],rect=g.querySelector('rect');
            const left=115+(Math.max(a,l.start)-a)*scale;
            const width=(Math.min(b,l.end??R.time)-Math.max(a,l.start))*scale;
            return Math.abs(+rect.getAttribute('x')-left)<1e-6 && Math.abs(+rect.getAttribute('width')-width)<1e-6;
          });
        }""")
        page.locator("#tab-fifo").click()
        assert "顺序执行" in page.locator("#run-note").inner_text()
        assert "390" in page.locator("#stats").inner_text()
        page.locator("#tab-fifo").focus()
        page.keyboard.press("ArrowRight")
        assert page.locator("#tab-exact_v1").get_attribute("aria-selected") == "true"
        assert "11 块" in page.locator("#run-note").inner_text()
        page.locator("#peak-view").click()
        assert "11/11" in page.locator("#snapshot").inner_text()
        page.locator("#memory").screenshot(path=str(out / "exact_v1_peak.png"))
        page.locator("#tab-ooo").click()
        for preset in ("startup", "tail", "all", "steady"):
            page.locator(f'[data-preset="{preset}"]').click()
            if preset == "all":
                assert page.locator("#engines [data-op]").count() == 1000
                assert page.locator("#memory [data-lease]").count() == 1200
        with page.expect_download() as info:
            page.locator("#export").click()
        assert info.value.suggested_filename == "buffers-ooo-9.svg"
        info.value.save_as(out / "buffer_lifetimes.svg")
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(out / "report_mobile.png"), full_page=True)
        assert not errors, errors
        browser.close()
    print(json.dumps({"result": "PASS", "page_errors": errors,
                      "checks": ["12 capacities", "1000 operations and 1200 leases", "lease coordinates",
                                 "producer/consumer details", "time cursor", "OOO/FIFO/exact tabs",
                                 "11 simultaneous live buffers", "all presets", "SVG export", "390px overflow"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
