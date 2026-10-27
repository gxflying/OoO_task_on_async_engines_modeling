# Attention pipe 像素测量与乱序调度模拟

首次阅读建议先看 **[仿真模型：硬件架构、执行约束与前提条件](MODEL_ARCHITECTURE.md)**，其中统一说明 v1/v2/v3 的建模对象、版本范围、资源与生命周期规则、前提假设及复现入口。

版本导航：[v1 队列与依赖基线（本文）](#像素测量方法) · [v2 共享 buffer](v2/README.md) · [v3 动态 barrier](v3/README.md) · [v3 循环伪代码](v3/pipeline_pseudocode.md) · [v3.1 独立 buffer/barrier 事件池与阻塞 API](v3.1/README.md) · [v3 / v3.1 Buffer 占用对比](comparison/README.md)。本文下方的“当前版本”及默认报告均指根目录 v1 像素时长模型；各版本结果在各自子目录。

新增报告：[v3.1 资源容量与性能矩阵](v3.1/out/report.html)、[独立 Buffer 占用对比](comparison/out/buffer_comparison.html)。下载 HTML 后可离线打开。v3.1 的资源分配和事件语义见其版本说明；上方硬件架构文档的版本范围仍为 v1/v2/v3。

当前版本以原始截图的**整数像素边界间距**作为指令时长，1 原图像素宽度对应 1 仿真时间单位，记为 px。已在 WSL 中执行 100 轮、1000 条指令，完成队列深度 1～5 的 OoO/FIFO 全部 10 组对照。

直接打开 [out/report.html](out/report.html)。报告内嵌全部动态轨迹、像素测量表和 4 倍放大标注图，无需服务器。此前的近似时长配置及报告保存在 [history/approximate_v1](history/approximate_v1/)，不与当前数据混用。

“执行时间线”提供 **乱序执行（OoO）/ 顺序执行（FIFO）** 两个 tab，使用同一配置各自实际仿真的完整轨迹。切换时同步更新队列、利用率、总耗时、依赖详情和单轮 pipe；保留布局和依赖箭头选项，按当前策略切换启动/稳态/尾部/全程视图。两种策略的 SVG 可分别导出。即使不传 `--compare`，报告也会生成两种策略的时间线。

## 像素测量方法

- 原图为 [reference.png](reference.png)，分辨率 **725×584 px**，原图左上角为 `(0,0)`。
- 使用 **4 倍最近邻放大**，每个原像素成为 4×4 像素方块；不用平滑插值或生成式重绘。
- 在各方块行的内部高度搜索连续暗竖边，排除文字、箭头和浅灰网格。相邻暗列组成一个边框候选组，取亮度中位数最低的一列作为整数像素边界。
- 时长为 `x_right - x_left`，不加 1。它表示两个边界位置的间距，不是把左右边框都包含在内的像素格计数。
- 下表记录下方“抽象流水线”的坐标；上方 attention pipe 对应方块逐项复测，全部宽度一致，右上方重复 MM1 的宽度也为 118。
- 个别边缘经过抗锯齿，跨 1～2 个暗像素列。因此这里给出的是可复现的**整数像素口径**，不声称恢复了原始矢量图的亚像素几何。

| 指令 | 左边界 x | 右边界 x | 新时长 / px | 上方图对应边界 x |
|---|---:|---:|---:|---|
| load1R1 | 46 | 133 | 87 | 39 → 126 |
| wdma1R1 | 133 | 179 | 46 | 126 → 172 |
| load1R2 | 134 | 221 | 87 | 135 → 222 |
| wdma1R2 | 221 | 267 | 46 | 222 → 268 |
| tensor1R1 | 181 | 299 | 118 | 181 → 299 |
| vector1R1 | 300 | 388 | 88 | 300 → 388 |
| vector1R2 | 388 | 441 | 53 | 388 → 441 |
| tensor1R2 | 441 | 552 | 111 | 441 → 552 |
| vector1R3 | 441 | 506 | 65 | 441 → 506 |
| vector1R4 | 552 | 644 | 92 | 552 → 644 |

测量证据：[逐方块 4 倍标注图](measurement/measured_blocks_4x.png)、[原图 4 倍放大](measurement/reference_4x.png)、[抽象流水线放大](measurement/abstract_4x.png)、[attention pipe 放大](measurement/attention_4x.png)、[边缘候选列和亮度数据](measurement/measurements.json)。JSON 同时保存原图 SHA-256、搜索区域、边界列及上下图对应宽度。

## 新测量值下的仿真结果

各队列使用相同深度，前端零耗时，按程序序入队，队列满则反压。OoO 选择队列中最老的就绪指令；FIFO 只能选择队首。顺序模式也保持异步执行。

| 深度 | 乱序总耗时 | 顺序总耗时 | 乱序稳态间隔 | 顺序稳态间隔 | 乱序吞吐效率 | 顺序吞吐效率 | 乱序吞吐提升 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 39403 | 39403 | 392 | 392 | 76.02% | 76.02% | 0% |
| 2 | 37225 | 37225 | 370 | 370 | 80.54% | 80.54% | 0% |
| 3 | 32301 | 37225 | 320.5 | 370 | 92.98% | 80.54% | 15.44% |
| 4 | 30097 | 37225 | 298 | 370 | 100.00% | 80.54% | 24.16% |
| 5 | 30097 | 37225 | 298 | 370 | 100.00% | 80.54% | 24.16% |

总耗时包含启动与排空；所有时间使用上述 px 单位。稳态间隔是平均每完成一轮需要的时间。吞吐效率为 `298 / 稳态间隔`，乱序吞吐提升为 `顺序间隔 / 乱序间隔 - 1`。

**新的像素比例改变了深度 3 的结果**：其完整调度状态每 **641 px 推进 2 轮**，平均间隔为 320.5。完成时刻例如 735、892、1376、1533，相邻间隔交替为 157 和 484；320.5 不是单轮固定间隔。100 轮和 200 轮运行都确认了这个周期。

默认深度 5 的重复状态为 t=654、952、1250；每 298 px 推进 1 轮。稳态统计窗口为 `[654,29560)`，包含 97 个周期，实际越过旧指令发射 198 次。

| 单元 | 每轮工作量 / px | 稳态利用率 | 全程利用率 |
|---|---:|---:|---:|
| RDMA | 174 | 58.39% | 57.81% |
| WDMA | 92 | 30.87% | 30.57% |
| Tensor | 229 | 76.85% | 76.09% |
| Vector | 298 | 100.00% | 99.01% |

Vector 每轮占用 `88+53+65+92=298`，是资源吞吐瓶颈。四单元稳态平均忙碌比例为 `793/(4×298)=66.53%`，与吞吐上限达成率 100% 是不同指标。在当前程序序和最老就绪优先策略下，统一队列深度 4 已达到资源下界。

## 依赖和程序序

`xxxNRM` 中 N 为硬件单元索引，本模型均为 1；M 是该轮中的操作编号。动态指令另有从 0 开始的 `iteration`，不同轮的完成 token 独立。

| 程序序 | 指令 | 对应计算 | 执行单元 | 同轮依赖 |
|---:|---|---|---|---|
| 0 | load1R1 | load K0 | RDMA | 无 |
| 1 | wdma1R1 | wdma K | WDMA | load1R1 |
| 2 | load1R2 | load V0 | RDMA | 无 |
| 3 | wdma1R2 | wdma V | WDMA | load1R2 |
| 4 | tensor1R1 | MM1 | Tensor | wdma1R1 |
| 5 | vector1R1 | v scale | Vector | tensor1R1 |
| 6 | vector1R2 | fa0 | Vector | vector1R1 |
| 7 | tensor1R2 | MM2 | Tensor | wdma1R2、vector1R2 |
| 8 | vector1R3 | fa1 | Vector | vector1R2 |
| 9 | vector1R4 | fa2 | Vector | tensor1R2、vector1R3 |

截图未规定完整前端程序序，以上是模型采用的顺序，可在配置的 `operations` 数组中修改。图中方块之间的绘图间隙不增加额外依赖或延迟；本次只测量方块自身宽度。

单独一轮的手工可核验执行区间：`load1R1 [0,87)`、`wdma1R1 [87,133)`、`load1R2 [87,174)`、`wdma1R2 [174,220)`、`tensor1R1 [133,251)`、`vector1R1 [251,339)`、`vector1R2 [339,392)`、`tensor1R2 [392,503)`、`vector1R3 [392,457)`、`vector1R4 [503,595)`。多轮共享硬件时单轮延迟可变化，以轨迹为准。

## 调度语义与稳态判定

1. 不同循环之间没有依赖；每条依赖只匹配同一轮生产者。
2. RDMA、WDMA、Tensor、Vector 各一个，同时各执行最多一条指令，非抢占，并占用完整时长。
3. 四个等待队列独立，深度不含正在执行的指令；发射立即释放槽位。
4. 所有指令异步；全局前端按程序序连续入队，不等旧指令完成，只有下一条所需队列满才停顿。
5. 每个时间点先处理完成事件，再反复“入队直到满 / 空闲单元发射”直到无进展。解码、入队、发射仲裁、唤醒均为零耗时，新入队指令可以立即发射。
6. 执行占用 `[start,end)`，下一条可在 end 开始，依赖在 end 可见且完成 token 持续有效。
7. 队列快照记录零耗时动作全部结束后的状态，表示 `[t,t+1)`；不能据此推断瞬时入队峰值。
8. 虚线框表示该次发射越过了同队列中的更老指令。点击 HTML 中的方块可查具体 `bypassed` 对象。

稳态检测比较完整调度状态：前端下一条位置、四个队列、执行中指令及剩余时长、仍有关联的完成 token。剔除绝对时间和整体轮次平移后，连续三次状态相同且时间/轮次差一致才确认两个重复周期。统计采用前端耗尽前的整数个周期，排除启动与排空。未找到周期则明确显示“未确认”；不以尾部平均数冒充稳态。

本模型只验证截图依赖图的调度，不执行真实矩阵或 softmax 运算；未加入额外共享带宽、数据缓冲区别名、寄存器容量或执行单元内部流水化接收约束。像素单位不等价于真实硬件时钟。

## WSL 复现

模拟器只需 Python 3.10+ 标准库；像素测量脚本另需 Pillow，当前 WSL 已安装。

```bash
cd '/mnt/d/02_学习总结/06_perf_tuning/02_simt_OoO_pipeline'
mkdir -p modeling/measurement modeling/out
set -e
set -o pipefail
python3 modeling/measure_reference.py --update-config | tee modeling/measurement/measurement.log
python3 -m unittest discover -s modeling/tests -v 2>&1 | tee modeling/out/tests.log
python3 modeling/pipeline.py --compare | tee modeling/out/run.log
```

修改循环数、队列深度或策略：

```bash
python3 modeling/pipeline.py --iterations 200 --queue-depth 3 --output modeling/out_200
python3 modeling/pipeline.py --policy fifo --output modeling/out_fifo
python3 modeling/pipeline.py --config modeling/config_approximate.json --output modeling/out_approximate
```

也可以在 `queue_depth` 中分别配置四个单元，例如 `{"rdma":1,"wdma":2,"tensor":3,"vector":5}`。仅支持同轮依赖。无效时长、未知依赖、依赖环、死锁或达到 `max_cycles` 仍未完成都会报错。

可选浏览器验收使用本机已安装的 Playwright 和 Chromium headless-shell：

```bash
~/.cache/attention-pipeline-qa/bin/python modeling/browser_check.py \
  --browser ~/.cache/attention-pipeline-qa/browser/chrome-headless-shell-linux64/chrome-headless-shell
```

## 产物与验证

| 文件 | 内容 |
|---|---|
| `measure_reference.py` | 搜索边框、4 倍放大、标注、导出像素测量，支持写回配置 |
| `measurement/measurements.json` | 像素坐标、候选暗列、亮度、原图哈希、交叉核对 |
| `config.json` | 当前像素时长、依赖、程序序、测量来源 |
| `config_approximate.json` | 原来的近似时长，供旧行为回归 |
| `pipeline.py` | 模拟、稳态检测、轨迹校验、完整 10 组对照与导出 |
| `out/report.html` | 最新交互 HTML，含像素测量和逐轮 pipe |
| `out/summary.json` / `out/depth_1_5_comparison.json` | 最新统计与全部 10 组对照 |
| `out/effective_config.json` | 实际运行配置 |
| `out/trace.json` / `out/instructions.csv` | 全部逐周期状态及动态指令记录 |
| `out/trace_fifo.json` / `out/summary_fifo.json` | 默认 OoO 报告对应的真实 FIFO 轨迹及统计；主策略为 FIFO 时额外生成 `_ooo` 文件 |
| `out/run.log` / `out/tests.log` / `out/browser_check.log` | WSL 运行、测试与浏览器验收证据 |
| `out/steady_hardware.png` / `out/steady_pipe.png` | 更新后的稳态四单元图和逐轮 pipe 图 |
| `out/steady_pipeline.svg` | 页面实际导出的稳态 SVG |
| `out/fifo_timeline.png` / `out/fifo_pipe.png` / `out/steady_pipeline_fifo.svg` | 顺序 tab 的时间线、逐轮 pipe 截图与实际导出 SVG |
| `history/approximate_v1/` | 修改前的代码、配置、说明和报告归档 |

行为测试覆盖原近似配置回归、像素配置一致性、单轮 595 px 手算见证、全部 10 组对照、深度 3/5 在 100 与 200 轮中的周期一致性，以及 HTML 内嵌的两种策略确实具有各自不同的执行轨迹。随机 DAG 覆盖继续保留。每次仿真均校验依赖、资源独占、时长、队列容量、前端顺序、反压目标和指令无漏发/重复发射。

浏览器检查包含启动/稳态/尾部/全程视图、全部 1000 条指令、依赖箭头、点击详情、宽度与时长比例、SVG 导出、390px 窄屏，以及新增的 10 条像素测量、5 行 OoO/FIFO 配对表和内嵌测量图片。

tab 回归另核对 FIFO 的全部方块坐标与真实轨迹一致、FIFO 无越序虚线框、详情和 SVG 属于当前策略、键盘切回 OoO 后恢复原指标，以及两个 tab 在窄屏下均无页面溢出。
