# v3 / v3.1 Buffer 占用对比

打开 [独立 HTML](out/buffer_comparison.html)。文件内嵌所有数据与代码，可以单独复制并离线打开。

- 默认：v3 `9 buffer / 10 barrier` 与 v3.1 `13 / 11` 的 OoO 轨迹；二者稳态 II 均为 298。
- 可切换 `9 / 10`、`11 / 12` 相同容量，以及 OoO / FIFO；相同数量不代表相同 barrier 协议。
- 两侧统一绝对时间轴；可查看启动、共同稳态、完整 100 轮或自定义区间。
- 物理 buffer 行显示复用；逻辑行将同一轮的同一数据对齐。点击色块联动突出同一逻辑数据并展示精确时间。
- 浅色框是完整占用，蓝线是生产计算，绿线是消费者读取，灰底是源输入。fan-out 各消费者分别显示，不重复计数 buffer。
- 额外提供占用数量曲线、峰值/平均值、输出预占用均值及 SVG 导出。

## 数据与口径

使用原始版本保存的 trace。仅缺失的 v3.1 `9 / 10`、`11 / 12`（各 OoO / FIFO）由原模拟器补算，结果存本目录 `out/trace_v31_*.json`；与原性能扫描的 II、总时间逐项核对。仿真轮数 100，队列深度 5，源输入计入池，硬件与持续时间完全沿用版本配置。

v3 在发射时分配；v3.1 在提交前分配，后续阻塞时保留已分配资源。平均占用用完整生命周期面积除以全程时间；预占用均值仅统计输出从分配到生产开始的时长。所有时间是截图像素相对单位。`out/manifest.json` 记录来源 SHA-256。现有 v3、v3.1 文件与结果不会被此生成器修改。

## WSL 重建与检查

从 Git 仓库根目录（直接含 `pipeline.py`、`v3/`、`v3.1/`）执行：

```bash
python3 comparison/build.py
~/.cache/attention-pipeline-qa/bin/python comparison/browser_check.py \
  --browser ~/.cache/attention-pipeline-qa/browser/chrome-headless-shell-linux64/chrome-headless-shell
```

生成器验证数据引用、生命周期、同槽复用及 engine 不重叠。浏览器检查全部 6 组配置/策略组合的启动、稳态、全程图，逻辑对齐、联动详情、SVG 导出与移动布局；并直接比较矩形位置/宽度与 trace 时间。
