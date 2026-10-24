"""v3 报告渲染、真实轨迹和 one-hot 事件视图验收。"""
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
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto((out / "report.html").as_uri())
        page.wait_for_selector("#barriers svg")
        assert "10 个动态 ID" in page.locator("#conclusion").inner_text()
        assert page.locator("#sweep tbody tr").count() == 22
        assert page.locator("#program tbody tr").count() == 10
        assert page.locator("#run-select option").count() == 6
        page.screenshot(path=str(out / "report_preview.png"), full_page=True)
        page.locator("#barriers").screenshot(path=str(out / "barrier_lifetimes.png"))
        page.locator("#engines [data-op]").first.click()
        assert "WAIT_FULL 0b" in page.locator("#detail").inner_text()
        assert "GET_AND_SET_EMPTY" in page.locator("#detail").inner_text()
        page.locator("#barriers [data-lease]").first.click()
        assert "严格事件序" in page.locator("#detail").inner_text()
        page.locator("#peak").click()
        assert "10/10 IDs" in page.locator("#event-label").inner_text()
        assert page.locator("#bits .bit.full").count() + page.locator("#bits .bit.empty").count() == 10
        assert page.locator("#event").input_value() == str(summary["peaks"][summary["default_run"]]["event_seq"])
        page.locator("#bits").screenshot(path=str(out / "peak_bitmap.png"))
        # 所有正时长和零时长 ID 生命周期都使用真实记录。
        assert page.evaluate("""() => {
          const a=+document.getElementById('start').value,b=a+(+document.getElementById('span').value);
          const scale=(+document.querySelector('#barriers svg').getAttribute('width')-142)/(b-a);
          return [...document.querySelectorAll('#barriers [data-lease]')].every(g=>{
            const l=R.barrier_leases[+g.dataset.lease],r=g.querySelector('rect');
            if(l.end===l.start)return !!g.querySelector('path') && !r;
            return Math.abs(+r.getAttribute('width')-(Math.min(b,l.end??R.time)-Math.max(a,l.start))*scale)<1e-6;
          });
        }""")
        for run_key in summary["display_runs"]:
            page.locator("#run-select").select_option(run_key)
            if "_m9_" in run_key or "_m11_" in run_key:
                assert "死锁" in page.locator("#run-note").inner_text()
                assert not page.locator("#deadlock-panel").is_hidden()
                if page.locator("#deadlock-panel").get_attribute("open") is None:
                    page.locator("#deadlock-panel summary").click()
                assert "free_ids" in page.locator("#deadlock").inner_text()
                page.locator('[data-preset="tail"]').click()
                if run_key == "b9_m9_ooo":
                    page.locator("#engines").screenshot(path=str(out / "deadlock_pipeline.png"))
            else:
                assert "完成" in page.locator("#run-note").inner_text()
                page.locator('[data-preset="all"]').click()
                assert page.locator("#engines [data-op]").count() == 1000
        page.locator("#buffer-filter").select_option("11")
        assert "12" in page.locator("#sweep .chosen").inner_text()
        page.locator("#run-select").select_option(summary["default_run"])
        for preset in ("startup", "tail", "all", "steady"):
            page.locator(f'[data-preset="{preset}"]').click()
        with page.expect_download() as download:
            page.locator("#export").click()
        assert download.value.suggested_filename == "barrier-ids-b9_m10_ooo.svg"
        download.value.save_as(out / "barrier_lifetimes.svg")
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(out / "report_mobile.png"), full_page=True)
        assert not errors, errors
        browser.close()
    print(json.dumps({"result": "PASS", "page_errors": errors,
                      "checks": ["22 ID capacities", "10-operation program", "6 actual traces",
                                 "one-hot mask details", "strict event-order bitmap peak",
                                 "zero-duration ID markers", "deadlock state", "1000 completed operations",
                                 "buffer filter", "presets", "SVG export", "390px viewport"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
