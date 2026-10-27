# Modeling v3：有限 one-hot barrier 与 buffer 复用

v3 在 v2 的共享 buffer 模型上加入有限 barrier ID 池，模拟循环展开后 full/empty 的真实分配、SET、WAIT 和回收。所有新代码及报告放在 `modeling/v3`，沿用 v1/v2 的指令时长、数据依赖和前端规则。

默认执行 100 轮；RDMA、WDMA、Tensor、Vector 各一个执行单元、各一个深度为 5 的队列。前端按全局程序序零耗时入队，队列满时反压。OoO 各队列选择最老的可发射指令；FIFO 只检查队首。不同循环之间没有数据依赖，但竞争执行单元、buffer 和 barrier ID。

HTML 的执行流水下方提供按物理 buffer 槽位展开的占用图：浅色区间为分配至实际释放，蓝线为生产者 `start→compute_end`，绿线分别表示各消费者的 `start→compute_end`，灰色表示外部源输入。计算结束后的发布阻塞仍计入占用期，不计为计算；未释放资源画至轨迹停止时刻并标记红色右边界。图与执行/barrier 时间线联动，支持点击详情和独立 SVG 导出。v3 是发射时分配，因此不能将 v3.1 的预分配时长套进本图；所有坐标来自已有 v3 轨迹。

## 计数口径与正式结果

用 `B` 表示等大小物理 buffer 数量，`M` 表示 full/empty **共用**的 barrier ID 总数。每轮模板有 10 条数据依赖边，动态 ID 可以跨边、跨轮复用；因此每轮 10 条边不等于必须静态分配 10 个 full ID。

已正式扫描 `buffer=9/11 × ID=1…22 × OoO/FIFO`，共 **88 个配置**，每组 100 轮，默认 queue 深度 5。另对每组最小达标 ID 及少一个 ID 做 **8 组 300 轮**复核。直接打开 [out/report.html](out/report.html)，原始证据见 `out/summary.json`、`out/sweep.json` 和 `out/run.log`。

| Buffer 数 B | Barrier 总数 M | 状态 | 100 轮总耗时 / px | 稳态间隔 / px | 瞬时 ID 峰值 |
|---:|---:|---|---:|---:|---:|
| 9 | 1～8 | 初始 empty 数量不足 | — | — | — |
| 9 | 9 | t=690 死锁，已完成 1 轮 | — | — | 9 |
| **9** | **10～22** | 完成 | **30097** | **298** | **10** |
| 11 | 1～10 | 初始 empty 数量不足 | — | — | — |
| 11 | 11 | t=690 死锁，已完成 1 轮 | — | — | 11 |
| **11** | **12～22** | 完成 | **30097** | **298** | **12** |

这里的 10 和 12 是 **full＋empty 总 ID 数**。它们是当前初始化、分配仲裁和整批发布规则下的最小达标值，不是对所有可能协议和调度策略的全局最优证明。增加 buffer 同时提高了初始化 empty 的数量要求，所以不能只比较 M 而忽略 B。

同容量顺序发射（FIFO）对照：

| Buffer | ID 数 | FIFO 100 轮总耗时 | FIFO 稳态间隔 | 与同 buffer 的 v2 一致 |
|---:|---:|---:|---:|---|
| 9 | 10～22 | 39185 | 390 | 每条指令时刻完全一致 |
| 11 | 12～22 | 37225 | 370 | 每条指令时刻完全一致 |

FIFO 在 9 buffer/9 ID 时 t=782 死锁，在 11 buffer/11 ID 时 t=762 死锁，均只完成 1 轮。达到阈值后，OoO/FIFO 都没有新增完成阻塞；本模板的影响表现为“低于阈值无法初始化或死锁，达到阈值后恢复基线”，没有虚构平滑的性能下降曲线。

9 buffer/10 ID 的第一次占用峰值在 **t=690、事件 #200**：**5 个 full＋5 个 empty**。同一时刻 producer 分配后，后续 consumer 的 WAIT 又会回收 ID。100 轮共分配 1000 个逻辑 full 实例，其中 **501 个 full 的分配与回收在同一时刻**，因此必须按事件序而非只按时间区间计算瞬时容量。11 buffer/12 ID 的峰值是 5 full＋7 empty。

300 轮复核结果：9 buffer/10 ID 和 11 buffer/12 ID 的 OoO 均为总耗时 89697、间隔 298；对应 FIFO 分别为 117185/390、111225/370。阈值少一个 ID 时仍在相同时刻死锁。所有达标长运行都与对应 v2 逐条入队、开始和结束时间一致。

稳态资源上限仍为每轮 Vector 工作量 `88+53+65+92=298 px`。吞吐效率计算为 `298 / 稳态间隔`；仅对完整执行且确认稳态的结果给出该指标。死锁、初始化阻塞和 timeout 不报告为完成，也不从未完成轨迹外推稳态吞吐。

## 已确认语义与模型约定

用户确认的语义：

- 每条数据依赖边使用一个独立 full ID。多输入指令等待多个 ID；fan-out 的不同消费者使用不同 full ID。
- full 在生产者完成时执行 `get_and_set`；没有空闲 ID 时阻塞完成。
- 消费者真正开始执行时，成功 WAIT 并释放相应 full ID，允许其立即复用。
- empty ID 同样动态分配和回收；上游输出就是下游输入，多输入各占一块数据 buffer。

以下是为闭合仿真而明确采用的模型约定：

| 阶段 | 规则 |
|---|---|
| 初始化 | 每个空闲物理 buffer 分配并 SET 一个 empty ID；M 小于 B 时初始化失败，不只启用部分 buffer。 |
| 入队 | 保存逻辑依赖边，不预占 buffer 或 barrier。 |
| 发射 | engine 空闲、所有输入 full 已 SET、所需空闲 buffer 的 empty 已 SET，才允许开始。所有条件同时满足后，消费并回收这些 full/empty。 |
| 计算 | 输入保持只读、输出保持独占；禁止原地覆盖输入。 |
| 计算结束 | 到达 `compute_end`，尚不释放 engine、输入或输出。 |
| 完成发布 | 一次性原子获得该指令的**所有输出 full＋所有即将变空 buffer 的 empty**。不足时不部分分配，不改变读者/所有权状态。 |
| 发布成功 | SET 所有新 ID，更新输入引用、输出 ready 和 buffer 释放状态，记录 `end`，释放 engine。 |
| 下次复用 | 新写入者开始时消费对应 empty，释放其 ID，再占用物理 buffer。 |

因此 `start → compute_end` 是真实计算，`compute_end → end` 是等待 barrier 的完成阻塞。报告分别统计计算利用率与包含完成阻塞的 engine 占用率。整批原子发布是本版本的公开模型约定；允许分步 SET、提前释放输入或提前释放 engine 会改变容量需求，必须作为不同协议重新仿真。

同一时刻先尝试完成发布，再派发/发射，反复执行直到不再有进展。WAIT 回收 ID 后，可在同一时刻唤醒此前被阻塞的完成；同刻释放的 buffer 也可立即复用。若没有任何未来计算完成事件、没有可发射指令且仍有未完成工作，则报告死锁并保存状态。

## one-hot 编码与动态绑定

单个物理 ID 的编码为 `1 << slot`，恰有一位为 1。等待多个 ID 时，把这些 one-hot 编码按位 OR，得到可以含多位 1 的 WAIT mask。例如等待 slot 1 和 slot 4，mask 为 `0b10010`。

队列中描述的是逻辑边 `(producer_seq, consumer_seq)`。物理 ID 在生产者成功完成发布时才绑定，消费者随后取得对应 `(slot, generation)`。本版本**不把未知的未来 ID 声称为已在静态指令中预编码的 one-hot mask**；导出的指令清单记录真实运行后取得的物理绑定。

每次复用同一个 ID slot 都递增 generation。WAIT/SET 同时校验代数和逻辑所有者，避免旧引用误命中新内容。generation 和逻辑边表是模型中明确存在的辅助元数据，不包含在 M 个 one-hot 位的数量里；本模型不估算其硬件面积或接口编码成本。

## 多输入、fan-out 与边界

`tensor1R2` 的输入为 `wdma1R2` 和 `vector1R2` 的输出：先等待两个 full，开始后释放两个 ID，但继续保留两块输入 buffer，直到自身完成发布。不能先消费一个 full，再等待另一输入或输出 buffer。

`vector1R2` 的输出同时供 `tensor1R2`、`vector1R3` 使用：生产者分配两个 full，各消费者开始时分别回收自己的 full。该输出 buffer 的 empty 只能在**两个消费者均成功完成发布**之后 SET。模型保留所有消费者及 remaining/readers 状态，未入队或未开始的消费者同样阻止回收。

这个 empty 的“最后消费者”判断需要引用计数或 join 元数据；一个 one-hot 位本身不执行多消费者计数。这里仍只分配一个 empty ID，不把聚合元数据另计为 barrier ID。

延续 v2 的边界假设：

- 首级 RDMA 需要一块池内外部输入和一块输出，发射时一起分配；外部输入数据已经就绪，不另建外部 full。输入在 RDMA 成功完成发布时释放并产生 empty。
- 末级 `vector1R4/fa2` 输出由外部 sink 同步接收，在该指令成功完成发布时释放，并产生 empty。
- `count_source_inputs: false` 可取消池内外部输入计数；改变此项或末级结果的保留规则后，必须重新扫描容量。

## 为什么会因 ID 不足而死锁

例如 3 个 buffer、3 个 ID，root A 的输出有两个消费者：初始化占用 3 个 empty；A 发射消费输入和输出的 2 个 empty，获得 2 个空闲 ID。A 完成需要两个 full 加一个输入释放的 empty，总共 3 个 ID，无法原子获得；消费者又必须等 A 的 full，系统无后续进展。增加到 4 个 ID 可解除这一协议死锁。

这类死锁来自有限资源及当前发布规则，即使数据依赖图没有环也会出现。报告保留阻塞指令、所需 full/empty 数、空闲 ID、buffer 读者和队列状态，便于区分 barrier 不足、buffer 不足及初始化失败。

## 复现与验证

在 WSL 中运行，只需要 Python 标准库执行模型和行为测试：

```bash
cd '/mnt/d/02_学习总结/06_perf_tuning/02_simt_OoO_pipeline'
set -e
set -o pipefail
mkdir -p modeling/v3/out
python3 -m unittest discover -s modeling/v3/tests -v 2>&1 | tee modeling/v3/out/tests.log
python3 modeling/v3/run.py | tee modeling/v3/out/run.log
```

显式指定扫描参数：

```bash
python3 modeling/v3/run.py --buffers 9 11 --max-ids 22 --long-iterations 300
```

本次 **27 项行为测试通过**，证据为 `out/tests.log`。包括 one-hot/generation、原子分配失败、full/fan-in/fan-out、动态 empty、完成阻塞及同刻恢复、排空和高容量基线对照。独立审查发现并修复了 `max_cycles` 恰好完成边界及 verifier 的引用错配漏检，增加了交换 full/empty、遗漏 empty WAIT、遗漏源输入和错误 buffer 代数的反例回归。

正式 runner 完成 88 个 OoO/FIFO 配置扫描与 8 组 300 轮复核；基线哈希全部保持不变。浏览器实际验证了 6 条轨迹、22 个 ID 容量行、1000 条完整操作、WAIT/SET mask、零时长 ID 标记、事件序位图峰值、死锁状态、SVG 导出及 390px 窄屏。证据分别保存在 `out/summary.json`、`out/run.log`、`out/browser_check.log`。浏览器检查中修正了时间窗口左边界绘制零宽已结束指令的问题。

可选浏览器复现：

```bash
~/.cache/attention-pipeline-qa/bin/python modeling/v3/browser_check.py \
  --browser ~/.cache/attention-pipeline-qa/browser/chrome-headless-shell-linux64/chrome-headless-shell
```

只更新 HTML 模板而不重复仿真时，使用 `python3 modeling/v3/run.py --render-only`。

稳态判断比较前端位置、各队列、运行/完成阻塞状态、buffer 所有者/读者、full 逻辑边和 empty 状态，要求经轮次平移后连续三次重复。所有物理槽等价且没有 bank 差异，因此允许等价槽位重命名；若后续增加 bank 或 slot 亲和性，必须扩展这个状态比较。

barrier 峰值依据严格事件序统计，包含同一时刻 SET→WAIT 的瞬时占用，不能只计算时间区间重叠。足够 ID 时还应逐条比较 v2 的入队、开始、完成及 buffer 时序，不能仅用总耗时或 II 相同代替基线一致性。

`baseline_manifest.json` 保存 Git 基准 `01289ce0f0c8b1fff2e4573cf354bce51914899f` 和 v1/v2 核心文件的 SHA-256，供 runner 核对旧版本未被修改。v3 复用 v2 的 buffer 实现，不替换旧版本源码和报告。

## 输出文件

| 文件 | 内容 |
|---|---|
| `out/report.html` | 离线流水线报告、资源容量比较及 ID 使用/复用视图 |
| `out/summary.json` | 结论、基线比较和验证摘要 |
| `out/sweep.json` | 各 buffer/barrier 容量及 OoO/FIFO 的真实扫描结果 |
| `out/trace_b9_m10_ooo.json` | 9 buffer、10 ID 的完整可运行轨迹 |
| `out/trace_b9_m9_ooo.json` | 9 buffer、9 ID 的死锁证据 |
| `out/trace_b11_m12_ooo.json` | 11 buffer、12 ID 的轨迹 |
| `out/instructions_b9_m10.txt` | 每条操作实际 WAIT/SET 的逻辑边和动态物理 ID 绑定 |
| `out/events_b9_m10.jsonl` | 分配、SET、WAIT、回收等事件，保留同刻事件次序 |
| `out/tests.log` / `out/run.log` / `out/browser_check.log` | 行为测试、批量运行、浏览器检查日志 |

执行时间沿用原截图测量值，`1 px = 1 仿真时间单位`。barrier 分配、SET、WAIT 的服务时间均为零，未建模硬件端口宽度、仲裁延迟、带宽、数据传输或矩阵运算；本版本回答的是所述协议下的容量、资源保留和调度影响。
