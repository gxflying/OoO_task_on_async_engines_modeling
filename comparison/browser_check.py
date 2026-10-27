"""验证对比图区间与真实轨迹一致、两版同尺度、筛选/导出/移动布局可用。"""
import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "out"

GEOMETRY = """() => {
 const near=(a,b)=>Math.abs(a-b)<1e-6;
 return runs.every((r,index)=>{
  const svg=document.querySelector(`#plot-${index} svg`),logical=$('layout').value==='logical';
  if(+svg.getAttribute('width')!==axis.w)return false;
  const groups=[...svg.querySelectorAll('[data-lease]')];
  const expected=r.leases.filter(l=>l.end>axis.a&&l.start<axis.b&&(!logical||r.records[l.owner].iteration===+$('iteration').value));
  if(groups.length!==expected.length)return false;
  function check(rect,start,end){
   const a=Math.max(axis.a,start),b=Math.min(axis.b,end);
   if(b<=a)return rect===null;
   return !!rect&&near(+rect.getAttribute('x'),axis.x(a))&&near(+rect.getAttribute('width'),(b-a)*axis.scale);
  }
  return groups.every(g=>{const l=r.leases[+g.dataset.lease],p=r.records[l.owner];
   if(!check(g.querySelector('.life'),l.start,l.end))return false;
   if(l.kind==='source_input'){if(g.querySelector('.producer'))return false;}
   else if(!check(g.querySelector('.producer'),p.start,p.compute_end))return false;
   return l.consumers.every(seq=>check(g.querySelector(`[data-reader="${seq}"]`),r.records[seq].start,r.records[seq].compute_end));
  });
 });
}"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser")
    args = parser.parse_args()
    errors, remote_requests = [], []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=args.browser, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1600, "height": 1200}, device_scale_factor=1)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("request", lambda r: remote_requests.append(r.url) if r.url.startswith(("http:", "https:")) else None)
        page.goto((OUT / "buffer_comparison.html").as_uri())
        page.wait_for_selector("#plot-1 svg")
        assert page.evaluate("runs.map(r=>r.ii)") == [298, 298]
        assert page.evaluate("runs.map(r=>r.buffers)") == [9, 13]
        assert page.locator("#iteration-control").is_hidden()
        assert page.evaluate(GEOMETRY)
        page.locator(".compare").screenshot(path=str(OUT / "buffer_comparison.png"))
        page.screenshot(path=str(OUT / "report_preview.png"), full_page=True)
        for scenario in ("target", "same9", "same11"):
            page.locator("#scenario").select_option(scenario)
            for policy in ("ooo", "fifo"):
                page.locator("#policy").select_option(policy)
                for preset in ("startup", "steady", "all"):
                    page.locator(f'[data-preset="{preset}"]').click()
                    assert page.evaluate(GEOMETRY), (scenario, policy, preset)
                    if preset == "all":
                        assert page.locator("#plot-0 [data-lease]").count() == 1200
                        assert page.locator("#plot-1 [data-lease]").count() == 1200
                for iteration in (0, 37, 99):
                    page.locator("#layout").select_option("logical")
                    page.locator("#iteration").fill(str(iteration))
                    page.locator("#iteration").press("Tab")
                    assert page.locator("#plot-0 [data-lease]").count() == 12
                    assert page.locator("#plot-1 [data-lease]").count() == 12
                    assert page.evaluate(GEOMETRY), (scenario, policy, iteration)
                page.locator("#layout").select_option("physical")
        page.locator("#scenario").select_option("target")
        page.locator("#policy").select_option("ooo")
        page.locator("#layout").select_option("logical")
        page.locator("#iteration").fill("0")
        page.locator("#iteration").press("Tab")
        key = page.evaluate("runs[0].leases.findIndex(l=>l.owner===4 && l.kind==='output')")
        page.locator(f'#plot-0 [data-lease="{key}"]').click()
        page.wait_for_function("document.querySelectorAll('.selected').length===2")
        assert "分配 → 生产开始：0 px" in page.locator("#detail-0").inner_text()
        assert "分配 → 生产开始：133 px" in page.locator("#detail-1").inner_text()
        assert "占用 [133, 339)" in page.locator("#detail-0").inner_text()
        assert "占用 [0, 339)" in page.locator("#detail-1").inner_text()
        page.locator(".compare").screenshot(path=str(OUT / "logical_comparison.png"))
        page.locator("#layout").select_option("physical")
        page.locator('[data-preset="startup"]').click()
        page.locator("#plot-0 svg").hover(position={"x": 260, "y": 70})
        assert "v3.1 占用" in page.locator("#cursor").inner_text()
        with page.expect_download() as download:
            page.locator("#export").click()
        assert download.value.suggested_filename == "buffer-comparison-target-ooo.svg"
        download.value.save_as(OUT / "buffer_comparison.svg")
        ET.parse(OUT / "buffer_comparison.svg")
        page.set_viewport_size({"width": 390, "height": 844})
        page.locator("#plot-0 svg").wait_for()
        assert page.evaluate(GEOMETRY)
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), "页面不可整体横向溢出"
        page.locator("#plot-0").evaluate("el=>{el.scrollLeft=120}")
        page.wait_for_function("document.getElementById('plot-1').scrollLeft===120")
        page.screenshot(path=str(OUT / "report_mobile.png"), full_page=True)
        assert not errors, errors
        assert not remote_requests, remote_requests
        browser.close()
    print(json.dumps({"result": "PASS", "cases": 10, "pairings": 6, "checks": [
        "区间与 trace 几何一致", "两版同轴同尺度", "12 个逻辑数据对齐", "1200 leases 全程呈现",
        "i0/MM1 预分配差 133px", "联动选择与光标", "SVG 导出", "390px 布局与横向滚动同步", "无网络依赖、无 JS 错误"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
