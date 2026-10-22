# Attention pipe 乱序调度模拟

本目录根据用户截图实现了一个可复现的 Python 逐周期模拟器。用户已确认：**不同循环之间没有依赖**；全部指令异步；**前端解码和入队耗时忽略，队列满才反压前端**。模拟执行了 100 轮、1000 条指令，输出独立 HTML 和完整轨迹。

直接打开 [out/report.html](out/report.html) 查看结果，无须启动服务器。所有显示数据均由 Python 模拟生成，页面控件仅调整实际轨迹的视图。

## 已确认结果

默认配置：四种执行单元各一个、每个等待队列深度 5、全局前端按程序序入队、队列内最老就绪优先发射。

| 指标 | 结果 |
|---|---:|
| 循环数 / 完成指令数 | 100 / 1000 |
| 总耗时，包含启动与排空 | 3030 相对时间单位 |
| 稳态平均每轮完成间隔 | 30 |
| 资源约束给出的间隔下界 | 30 |
| 稳态吞吐上限达成率 | 100% |
| 实际越过队列内旧指令发射 | 198 次 |
| 首次观测到的重复状态 | t=67、97、127 |
| 稳态统计窗口 | [67,2977)，97 个完整周期 |

| 单元 | 每轮工作量 | 稳态利用率 | 全程利用率 |
|---|---:|---:|---:|
| RDMA | 18 | 60.00% | 59.41% |
| WDMA | 10 | 33.33% | 33.00% |
| Tensor | 23 | 76.67% | 75.91% |
| Vector | 30 | 100.00% | 99.01% |

瓶颈为 Vector。每轮它必须执行 `9+5+7+9=30` 个时间单位，因此平均完成间隔不能低于 30。默认模型实际达到该下界。

四单元稳态平均忙碌比例是 `(18+10+23+30)/(4×30)=67.50%`。它与“吞吐上限达成率 100%”是不同指标；其他单元工作量较少，所以即使 Vector 持续满载，它们也有空闲。

| 发射策略 | 各队列深度 | 总耗时 | 稳态完成间隔 | 吞吐上限达成率 |
|---|---:|---:|---:|---:|
| FIFO | 5 | 3723 | 37 | 81.08% |
| OoO | 1 | 4020 | 40 | 75.00% |
| OoO | 2 | 3723 | 37 | 81.08% |
| OoO | 3 | 3723 | 37 | 81.08% |
| OoO | 4 | 3030 | 30 | 100.00% |
| OoO | 5 | 3030 | 30 | 100.00% |
| OoO | 8 | 3030 | 30 | 100.00% |
| OoO | 16 | 3030 | 30 | 100.00% |

默认 OoO 相比同深度 FIFO 的**稳态吞吐提高 23.33%**，计算为 `37/30-1`。在本次已测程序序与策略下，深度 4 是达到上限的最小统一队列深度；这不是对所有程序序或调度策略的普遍结论。

一个实际乱序见证：第 1 轮 `vector1R4` 在 t=77 尚需等待 `tensor1R2` 于 t=81 完成，Vector 选择了已就绪的第 2 轮 `vector1R1`，执行 [77,86)。第 1 轮 `vector1R4` 随后执行 [86,95)，填补了原本的依赖空档。具体的被越过指令记录在 `bypassed` 字段。

## 从截图提取的指令模板

`xxxNRM` 中 N 是硬件单元索引，本模型 N 均为 1；M 是该轮该类型操作的编号。动态指令额外增加 `iteration`，避免不同轮的完成 token 混淆。轮次从 0 开始。

时长为用户确认的方块宽度近似，单位为相对时间。截图没有提供真实时钟或纳秒标尺。

| 前端程序序 | 指令 | 对应计算 | 执行单元 | 时长 | 同轮依赖 |
|---:|---|---|---|---:|---|
| 0 | load1R1 | load K0 | RDMA | 9 | 无 |
| 1 | wdma1R1 | wdma K | WDMA | 5 | load1R1 |
| 2 | load1R2 | load V0 | RDMA | 9 | 无 |
| 3 | wdma1R2 | wdma V | WDMA | 5 | load1R2 |
| 4 | tensor1R1 | MM1 | Tensor | 12 | wdma1R1 |
| 5 | vector1R1 | v scale | Vector | 9 | tensor1R1 |
| 6 | vector1R2 | fa0 | Vector | 5 | vector1R1 |
| 7 | tensor1R2 | MM2 | Tensor | 11 | wdma1R2、vector1R2 |
| 8 | vector1R3 | fa1 | Vector | 7 | vector1R2 |
| 9 | vector1R4 | fa2 | Vector | 9 | tensor1R2、vector1R3 |

截图给出了依赖，没有给出完整的前端程序序；上表是本模型采用的确定性输入顺序，可通过修改 `config.json` 的 `operations` 数组改变。截图横向位置不额外构成依赖；例如 `load1R2` 与 `wdma1R1` 可以重叠。

单独模拟一轮时，执行区间应为：`load1R1 [0,9)`、`wdma1R1 [9,14)`、`load1R2 [9,18)`、`wdma1R2 [18,23)`、`tensor1R1 [14,26)`、`vector1R1 [26,35)`、`vector1R2 [35,40)`、`tensor1R2 [40,51)`、`vector1R3 [40,47)`、`vector1R4 [51,60)`。测试包含这个手工可核验的 60 单位见证。多轮重叠会改变单轮延迟，以实际轨迹为准。

## 调度和时间语义

1. 每个硬件单元同时最多执行一条指令，持续占用整个时长，不可抢占。未进一步建模执行单元内部流水化接收多个操作。
2. 四个等待队列独立；默认各 5 项，**不含正在执行的指令**。发射时立即释放队列槽位。
3. 一个全局前端按上表程序序逐轮入队，不等待任意旧指令执行完成；遇下一条所需队列满时停止，不能跨过这条指令继续解码后面的指令。
4. 依赖条件只检查同一轮生产者是否执行完成。完成 token 持续有效，不是一闪而过的脉冲。
5. OoO 从当前队列选择程序序最老的就绪指令；FIFO 只检查队首。执行单元忙时不发射。
6. 时间 t 先处理全部完成事件，再反复“前端入队直到满 / 空闲单元发射”，直到该时间点没有进一步进展。入队、发射仲裁及唤醒的耗时均为零；每条指令执行时长至少为 1，所以这个过程有限。
7. 指令执行区间是 `[start,end)`。在 end 时间点，依赖结果可见、执行单元可立即接收下一条。新入队的就绪指令也可在同一时间点发射。
8. 逐周期快照对应上述零耗时动作结束后的状态，记录 `[t,t+1)` 的占用；瞬时队列峰值不应从快照推断。

模型假设每轮数据已隔离，不引入额外缓冲区别名、共享存储带宽、寄存器容量等约束。这里验证的是截图依赖图的资源调度可行性与效率，不执行实际矩阵运算或在线 softmax。用户明确的“循环之间没有依赖”是当前模型合同。

## 稳定性判定

不是仅检查几次相邻完成间隔是否相同。每个时间点保留：前端下一条位置、四个等待队列、执行中指令及其剩余时长，以及仍有关联的完成 token。用最老在途轮次归一化编号；此前的轮次已结束，且没有跨轮依赖，因此可从状态比较中移除。

完整状态在三个时间点相同，并且两段时间差和轮次平移量相同且为正，才报告稳定周期。因为调度是确定性的，有限程序尚未耗尽前，相同归一化状态具有相同后续行为。统计窗口从首次重复状态起，截取前端耗尽前的整数个周期；排除启动和尾部排空。

如果循环太少或没有找到重复状态，`steady` 为 `null`，HTML 显示“未确认”，不会伪造稳定结果。默认运行即使已确认稳态也继续完成全部 100 轮，以保留完整轨迹。

## 在 WSL 中复现

无需安装第三方 Python 库，Python 3.10 及以上即可。

```bash
cd '/mnt/d/02_学习总结/06_perf_tuning/02_simt_OoO_pipeline'
mkdir -p modeling/out
set -o pipefail
python3 -m unittest discover -s modeling/tests -v 2>&1 | tee modeling/out/tests.log
python3 modeling/pipeline.py --compare | tee modeling/out/run.log
```

改变循环次数、深度、策略或输出目录：

```bash
python3 modeling/pipeline.py --iterations 200 --queue-depth 4 --output modeling/out_200
python3 modeling/pipeline.py --policy fifo --output modeling/out_fifo
python3 modeling/pipeline.py --config modeling/config.json --compare
```

各执行单元使用不同队列深度时，在 JSON 中设置：

```json
"queue_depth": {"rdma": 1, "wdma": 2, "tensor": 3, "vector": 5}
```

`operations` 中的 `duration`、`deps` 和数组顺序均可修改。仅支持同轮依赖，不隐含支持跨轮递推。无效时长、未知依赖、依赖环、死锁以及达到 `max_cycles` 仍未完成的情况都会报错。

## 文件与验证

| 文件 | 内容 |
|---|---|
| `pipeline.py` | 模拟器、完整状态稳态检测、轨迹核验、统计和导出 |
| `config.json` | 用户确认的时长、依赖与默认配置 |
| `report_template.html` | 离线 HTML 模板；四单元与逐轮两种布局、队列曲线、SVG 导出 |
| `tests/test_pipeline.py` | 16 项行为测试，其中一项包含 30 个固定随机种子的 DAG 工作负载 |
| `browser_check.py` | 可选浏览器验收脚本，需 Playwright；不影响模拟器运行 |
| `reference.png` | 原始用户截图 |
| `out/report.html` | 可直接打开的完整报告，内嵌 1000 条指令和每周期队列数据 |
| `out/summary.json` | 配置、稳态证据、统计、策略和深度对照 |
| `out/effective_config.json` | 本次运行的实际配置 |
| `out/trace.json` | 完整动态指令与逐周期队列/执行单元状态 |
| `out/instructions.csv` | 逐指令入队、发射、结束、依赖与越序记录 |
| `out/tests.log` / `out/run.log` | WSL 测试和运行输出 |
| `out/browser_check.log` | 浏览器验收输出：无页面脚本错误，10 类交互和显示检查通过 |
| `out/steady_hardware.png` / `out/steady_pipe.png` | 稳态四单元视图与原图风格逐轮 pipe 截图 |
| `out/steady_pipeline.svg` | 从 HTML 实际导出的时间线 SVG |
| `out/report_preview.png` / `out/report_mobile.png` | 桌面和窄屏页面渲染检查 |

每次正常模拟结束都会核对所有指令完成、前端顺序、指令无漏发和重复发射、依赖先完成、执行单元无重叠、指令时长和队列容量。回归还覆盖零耗时入队、同一时刻回收/补入、持续完成 token、双依赖汇合、FIFO 与 OoO 的差异、异常输入和死锁，并用 200 轮运行验证默认稳态与 100 轮一致。

浏览器验收使用 WSL 中的 Playwright 和 Chromium headless-shell。检查启动/稳态/结尾/全程视图、1000 条指令、依赖箭头、方块宽度等于时长、点击详情、SVG 导出和 390px 窄屏。首次检查发现窄屏表格溢出，修复后全部通过；相关输出已保存。可选复现命令：

```bash
~/.cache/attention-pipeline-qa/bin/python modeling/browser_check.py \
  --browser ~/.cache/attention-pipeline-qa/browser/chrome-headless-shell-linux64/chrome-headless-shell
```

如果自行安装 Playwright 的 Chromium，可省略 `--browser`。这些是本机的可选 QA 工具路径，不是模拟器依赖。
