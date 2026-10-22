"""原始 PNG 的确定性像素测量；Pillow 仅用于测量与最近邻放大。"""
from __future__ import annotations

import hashlib
import json
import argparse
from pathlib import Path
from statistics import median

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
REGIONS = {
    # 搜索区间人工按图中行定位；x/y 均为原图坐标，右端不含。
    "abstract_tensor": (170, 565, 208, 225),
    "abstract_vector": (290, 655, 228, 245),
    "abstract_loadK": (35, 190, 253, 270),
    "abstract_loadV": (125, 275, 274, 291),
    "attention_tensor": (170, 675, 58, 75),
    "attention_vector": (290, 655, 79, 95),
    "attention_loadK": (30, 180, 97, 114),
    "attention_loadV": (125, 275, 127, 144),
}
BLOCKS = [
    ("load1R1", "loadK", 0, 1, 251, 271),
    ("wdma1R1", "loadK", 1, 2, 251, 271),
    ("load1R2", "loadV", 0, 1, 272, 292),
    ("wdma1R2", "loadV", 1, 2, 272, 292),
    ("tensor1R1", "tensor", 0, 1, 206, 226),
    ("vector1R1", "vector", 0, 1, 226, 246),
    ("vector1R2", "vector", 1, 2, 226, 246),
    ("tensor1R2", "tensor", 2, 3, 206, 226),
    ("vector1R3", "vector", 2, 3, 226, 246),
    ("vector1R4", "vector", 4, 5, 226, 246),
]


def detect_edges(image, region):
    xa, xb, ya, yb = region
    # 只有贯穿整个内部高度的暗列才是竖边；排除文字、浅灰网格和箭头。
    profiles = {x: [max(image.getpixel((x, y))) for y in range(ya, yb)] for x in range(xa, xb)}
    columns = [x for x, values in profiles.items() if max(values) < 150]
    groups = []
    for x in columns:
        if not groups or x != groups[-1][-1] + 1:
            groups.append([])
        groups[-1].append(x)
    edges = []
    for group in groups:
        chosen = min(group, key=lambda x: (median(profiles[x]), x))
        edges.append({"x": chosen, "candidate_columns": group,
                      "median_intensity": {str(x): median(profiles[x]) for x in group}})
    return edges


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-config", action="store_true", help="将测得的整数像素宽度写入默认配置")
    args = parser.parse_args()
    source = HERE / "reference.png"
    output = HERE / "measurement"
    output.mkdir(exist_ok=True)
    image = Image.open(source).convert("RGB")
    assert image.size == (725, 584), "原图尺寸变化，需重新指定搜索区域"
    profiles = {name: detect_edges(image, region) for name, region in REGIONS.items()}
    rows = []
    for name, lane, left, right, top, bottom in BLOCKS:
        lower = profiles[f"abstract_{lane}"]
        upper = profiles[f"attention_{lane}"]
        xl, xr = lower[left]["x"], lower[right]["x"]
        upper_left, upper_right = upper[left]["x"], upper[right]["x"]
        assert xr - xl == upper_right - upper_left, f"上下图宽度不一致: {name}"
        rows.append({"op": name, "x_left": xl, "x_right": xr, "width_px": xr - xl,
                     "y_top": top, "y_bottom": bottom,
                     "attention_x_left": upper_left, "attention_x_right": upper_right,
                     "attention_width_px": upper_right - upper_left})
    # 右上角下一轮 MM1 也是独立的重复长度见证。
    upper_tensor = profiles["attention_tensor"]
    assert upper_tensor[4]["x"] - upper_tensor[3]["x"] == rows[4]["width_px"]
    data = {
        "source": "reference.png", "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "original_size": list(image.size), "zoom": 4, "resampling": "NEAREST",
        "coordinate_origin": "原图左上角为 (0,0)，x 向右，y 向下",
        "measurement_rule": "连续暗竖边按相邻像素列分组；选组内亮度中位数最小的列；width_px=x_right-x_left，不加1",
        "precision_note": "整数像素边界口径；抗锯齿边缘不用于推断原始矢量坐标或亚像素精度",
        "time_unit": "1 原图像素宽度 = 1 仿真时间单位",
        "search_regions": REGIONS, "edge_profiles": profiles, "blocks": rows,
    }
    (output / "measurements.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.update_config:
        config_path = HERE / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        widths = {row["op"]: row["width_px"] for row in rows}
        for op in config["operations"]:
            op["duration"] = widths[op["id"]]
        config["time_unit"] = "px"
        config["timing_source"] = {k: data[k] for k in (
            "source_sha256", "original_size", "zoom", "measurement_rule", "precision_note", "blocks")}
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    image.resize((image.width * 4, image.height * 4), Image.Resampling.NEAREST).save(output / "reference_4x.png")
    for name, crop in [("abstract", (30, 195, 660, 305)), ("attention", (30, 45, 680, 150))]:
        part = image.crop(crop)
        part.resize((part.width * 4, part.height * 4), Image.Resampling.NEAREST).save(output / f"{name}_4x.png")

    # 每一行是原方块的 4x 最近邻截取，不合成或改变原始边框。
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
    title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 23)
    board = Image.new("RGB", (1160, 120 + 140 * len(rows)), "white")
    draw = ImageDraw.Draw(board)
    draw.text((24, 15), "Original-pixel measurements / nearest-neighbor 4x crops", fill="#111111", font=title_font)
    draw.text((24, 54), "Width = right boundary x - left boundary x; original image: 725 x 584", fill="#333333", font=font)
    for i, row in enumerate(rows):
        y = 110 + i * 140
        xl, xr, yt, yb = (row[k] for k in ("x_left", "x_right", "y_top", "y_bottom"))
        draw.text((24, y), row["op"], fill="#111111", font=title_font)
        draw.text((24, y + 37), f"x={xl} -> {xr}", fill="#333333", font=font)
        draw.text((24, y + 67), f"{xr} - {xl} = {row['width_px']} px", fill="#c52b1e", font=font)
        crop = image.crop((xl - 2, yt - 2, xr + 3, yb + 3))
        board.paste(crop.resize((crop.width * 4, crop.height * 4), Image.Resampling.NEAREST), (330, y))
        for x in (xl, xr):
            marker = 330 + (x - (xl - 2)) * 4 + 2
            draw.line((marker, y - 7, marker, y + 7), fill="#c52b1e", width=2)
    board.save(output / "measured_blocks_4x.png")
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
