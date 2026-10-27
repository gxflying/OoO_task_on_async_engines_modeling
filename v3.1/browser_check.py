"""实际检查双资源性能矩阵、边界流水、资源生命周期及交互。"""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser")
    args = parser.parse_args()
    out = Path(__file__).resolve().parent / "out"
    summary = json.loads((out / "summary.json").read_text())
    errors = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.browser, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((out / "report.html").as_uri(), timeout=60000)
        page.wait_for_selector("#engines svg")
        assert page.locator("#matrix tbody td").count() == 576
        assert page.locator("#matrix .pareto").count() == 2
        assert "13 buffer + 11 barrier" in page.locator("#conclusion").inner_text()
        assert "14 buffer + 10 barrier" in page.locator("#conclusion").inner_text()
        page.screenshot(path=str(out / "report_preview.png"), full_page=True)
        page.locator("#matrix").screenshot(path=str(out / "performance_matrix.png"))
        page.locator('[data-b="13"][data-m="11"]').click()
        assert "298" in page.locator("#case-detail").inner_text()
        page.locator('[data-b="13"][data-m="12"]').click()
        assert "307" in page.locator("#case-detail").inner_text()
        page.locator("#policy").select_option("fifo")
        assert page.locator("#matrix .pareto").count() == 1
        page.locator('[data-b="8"][data-m="8"]').click()
        assert "370" in page.locator("#case-detail").inner_text()
        page.locator("#policy").select_option("ooo")
        page.locator("#engines [data-op]").first.click()
        assert "WAIT" in page.locator("#detail").inner_text()
        page.locator("#buffers [data-lease]").first.click()
        assert "get=" in page.locator("#detail").inner_text()
        page.locator("#barriers [data-lease]").first.click()
        assert "set=" in page.locator("#detail").inner_text()
        page.locator("#engines").screenshot(path=str(out / "pipeline.png"))
        page.locator("#buffers").screenshot(path=str(out / "buffer_lifetimes.png"))
        page.locator("#barriers").screenshot(path=str(out / "barrier_lifetimes.png"))
        assert page.evaluate("""() => {
          const a=+document.getElementById('start').value,b=a+(+document.getElementById('span').value);
          const scale=(+document.querySelector('#buffers svg').getAttribute('width')-137)/(b-a);
          return [...document.querySelectorAll('#buffers [data-lease]')].every(g=>{
            const l=R.buffer_leases[+g.dataset.lease],rect=g.querySelector('rect');
            return Math.abs(+rect.getAttribute('width')-(Math.min(b,l.end??R.time)-Math.max(a,l.start))*scale)<1e-6;
          });
        }""")
        for key in summary["display_runs"]:
            page.locator("#run-select").select_option(key)
            assert page.locator("#engines svg").count() == 1
            if page.locator("#deadlock-panel").is_visible():
                if page.locator("#deadlock-panel").get_attribute("open") is None:
                    page.locator("#deadlock-panel summary").click()
                assert "frontend" in page.locator("#deadlock").inner_text()
        page.locator("#run-select").select_option(summary["default_run"])
        for preset in ("startup", "tail", "all", "steady"):
            page.locator(f'[data-preset="{preset}"]').click()
            if preset == "all":
                assert page.locator("#engines [data-op]").count() == 1000
                assert page.locator("#buffers [data-lease]").count() == 1200
                assert page.locator("#barriers [data-lease]").count() == 1200
        with page.expect_download() as download:
            page.locator("#export").click()
        download.value.save_as(out / "buffer_lifetimes.svg")
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(out / "report_mobile.png"), full_page=True)
        assert not errors, errors
        browser.close()
    print(json.dumps({"result": "PASS", "page_errors": errors,
                      "checks": ["576 cells per policy", "Pareto borders", "nonmonotonic case",
                                 "all saved traces", "blocking snapshots", "1000 instructions",
                                 "1200 buffer and 1200 barrier lifetimes", "real interval geometry",
                                 "instruction/resource details", "SVG export", "390px viewport"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
