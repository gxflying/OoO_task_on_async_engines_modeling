# 仿真模型：硬件架构、执行约束与前提条件

本文是仓库的架构阅读入口，面向首次接触模型的读者。代码位置以本 Git 仓库根目录为基准：根目录是 v1，`v2/` 增加物理 buffer，`v3/` 增加有限 barrier ID。详细生命周期、伪代码与运行证据见文末链接。

## 1. 背景与建模目标

模型来自一张 Attention 执行流水草图：[原始截图](reference.png)。将同一套指令模板重复展开，在共享的 RDMA、WDMA、Tensor、Vector 执行资源上调度，回答三个问题：

1. 有限指令队列支持乱序发射后，能达到怎样的稳态吞吐？
2. 生产者输出直接作为消费者输入、允许物理存储复用时，需要多少 buffer？
3. 用 one-hot full/empty barrier 表达数据依赖和 buffer 复用时，需要多少动态 ID，容量不足会如何阻塞？

**当前工作负载明确假定不同循环之间没有算法数据依赖。** 各轮仍竞争执行单元、队列、buffer 和 ID，因此资源复用可以产生跨轮等待。模型不执行真实矩阵运算或在线 softmax，也不能据此证明带跨轮 `m/l/O` 递推状态的真实 FlashAttention 可以任意乱序。

## 2. 硬件对象与版本范围

| 硬件/状态对象 | 数量及含义 | 生效版本 |
|---|---|---|
| 全局前端 | 一个，按配置中的程序序逐轮入队 | 全部 |
| 指令队列 | RDMA/WDMA/Tensor/Vector 各一个，默认各 5 项 | 全部 |
| 执行单元 | 四种各 **1 个**；每个同时最多执行 **1 条**指令 | 全部 |
| 完成与依赖状态 | 每条动态指令有轮次和唯一序号，按同轮生产者匹配 | 全部 |
| 共享物理 buffer 池 | N 块等大小槽位，跨 engine 复用；没有按 engine 永久分区 | v2、v3 |
| buffer 所有权与读者状态 | 生产者、全部消费者、尚未完成者、当前读者、ready、generation | v2、v3 |
| 共享 barrier ID 池 | M 个物理 ID，full/empty 共用、动态分配和回收 | v3 |
| 逻辑边到物理 ID 的绑定 | 生产者完成时绑定 `(slot,generation)`，消费者开始时消费 | v3 |

| 版本 | 新增约束 | 不应混淆的边界 |
|---|---|---|
| v1：根目录 | 执行资源独占、队列容量、逻辑依赖、OoO/FIFO | 没有有限物理 buffer 或有限 barrier ID 容量约束 |
| v2 | buffer 分配、共享读取、最后消费者完成后回收、防覆盖 | 依赖仍由模型完成状态管理，没有 ID 数量不足导致的完成阻塞 |
| v3 | 逐边 full、逐空闲 buffer 的动态 empty、ID 容量与完成发布 | full 在消费者开始时回收；buffer 要等消费者完成才回收 |

输入指令描述首先被展开为有限程序，随后才按前端规则逐条入队。**生成描述不等于入队，入队不等于执行**；每轮不会创建新的硬件执行单元。

## 3. 单轮指令、依赖与执行时间

表中顺序是模型采用的全局前端程序序；截图没有完整规定该顺序，因此它是一项显式建模选择。所有依赖在 v2/v3 中均视为真实数据输入。

| 序号 | 指令 | 计算含义 | 单元 | 时长 / px | 同轮生产者 |
|---:|---|---|---|---:|---|
| 0 | load1R1 | load K | RDMA | 87 | 无 |
| 1 | wdma1R1 | wdma K | WDMA | 46 | load1R1 |
| 2 | load1R2 | load V | RDMA | 87 | 无 |
| 3 | wdma1R2 | wdma V | WDMA | 46 | load1R2 |
| 4 | tensor1R1 | MM1 | Tensor | 118 | wdma1R1 |
| 5 | vector1R1 | v scale | Vector | 88 | tensor1R1 |
| 6 | vector1R2 | fa0 | Vector | 53 | vector1R1 |
| 7 | tensor1R2 | MM2 | Tensor | 111 | wdma1R2、vector1R2 |
| 8 | vector1R3 | fa1 | Vector | 65 | vector1R2 |
| 9 | vector1R4 | fa2 | Vector | 92 | tensor1R2、vector1R3 |

每轮 10 条操作、10 条依赖边。`tensor1R2`、`vector1R4` 有双输入；`vector1R2` 的输出有两个消费者。`xxxNRM` 中 N 为单元索引（本模型均为 1），M 为该类型操作的编号，额外的 `iteration` 区分循环轮次。

时长来自原图整数像素边界间距，先用最近邻放大辅助检查，取 `x_right-x_left`。**1 原图像素宽度对应 1 仿真时间单位，不代表实际硬件周期或纳秒。** 方块之间的绘图空隙不自动增加依赖或延迟。测量原始数据见 [measurements.json](measurement/measurements.json)；早期粗略比例另存为 [config_approximate.json](config_approximate.json)。

## 4. 队列、发射与资源互斥

- 队列容量只计算等待指令，不含正在执行的指令；发射即释放队列项。
- 全局前端遇到下一条指令的目标队列已满，就停止派发，不能跳过它去派发后面的其他 engine 指令。
- OoO：每个队列选择程序序最老的、依赖与发射资源均满足的指令。FIFO：只检查队首；FIFO 仍然是异步提交和执行。
- 所有指令均异步。前端不等待前一条操作完成；各 engine 可以并行，但同一个 engine 内不能重叠执行。
- 同时竞争资源时，固定按 RDMA → WDMA → Tensor → Vector 的顺序尝试，buffer 和 ID 使用最低编号可用槽。它是确定性贪心策略，没有全局最优调度搜索。
- RDMA 的上游 full 列表为空，不等于没有硬件约束。它仍需执行单元空闲；v2/v3 还需新输入/输出 buffer，v3 还需对应 empty 已 SET。
- 没有建模单个 engine 内部允许多条操作重叠的流水化接收能力，也没有抢占、投机发射或回滚。

代码通过唯一的 `running[engine]` 占用项落实互斥；v3 在 `compute_end` 后因 ID 不足阻塞时，也不会清空该项。检查代码见 [v3/simulator.py](v3/simulator.py)。

因此 RDMA 有空泡不一定是依赖或执行延迟：也可能是 Vector 队列满，阻塞了全局前端，下一条 load 根本尚未入队。

## 5. Buffer 生命周期：v2 与 v3 的共同前提

每块容量记为 S，N 块的总容量为 N×S；具体字节数尚未提供。双输入操作需同时保留两块输入，另分配一块输出。

| 时点 | 规则 |
|---|---|
| 入队 | 不预占数据 buffer |
| 发射 | 所需新 buffer 一次性分配，失败不占用部分资源等待 |
| 输入绑定 | 上游输出就是下游输入；零拷贝、不重复计数 |
| 执行中 | 输入保持只读、输出独占；禁止输入/输出同址覆盖 |
| 生产者完成 | 输出标记 ready，不能因此提前释放仍需消费的数据 |
| 消费者完成 | 移除本消费者引用；全部直接消费者完成才回收 |
| 复用 | 同一时间边界可以释放后重新分配，generation 递增 |

尚未入队或尚未发射的消费者也保留引用。fan-out 可以同时只读，但不能仅凭“当前读者数量为零”释放。slot 与 generation 防止旧引用访问复用后的新数据。

边界假设：首级 RDMA 的外部数据在发射时已就绪，使用一块池内输入和一块输出；该输入在 RDMA 完成时释放。末级 `fa2` 输出由外部 sink 在完成时立即接收。如果末级结果长期保留，或首级输入来自池外，容量结论需重新计算。

## 6. Barrier 协议与完成阻塞：v3

| 对象 | 分配/SET 时点 | WAIT/回收时点 | 表达的条件 |
|---|---|---|---|
| full | 生产者完成发布时；每条依赖边独立一个 ID | 对应消费者真正开始时 | 上游数据可读 |
| empty | buffer 的最后一个消费者成功完成后；初始空闲 buffer 也各有一个 | 下一次写入者开始时 | 物理 buffer 可以覆盖复用 |

M 个 ID 对应 M 位 one-hot 空间，单个 ID 为 `1 << slot`，多个输入的 WAIT 是多个 one-hot 位的 OR。full 和 empty 在同一个 ID 池动态复用，不是每个 buffer 永久固定一对 ID。

消费者必须同时满足 full、empty、buffer 和 engine 条件才消费 WAIT，不会先释放部分依赖再等待剩余资源。fan-out 的 full 按边独立回收，empty 则等待全部消费者完成。最后消费者判断仍需要引用计数/join 元数据，一位 barrier 不会自行承担计数功能。

**完成发布采用整批原子申请：** 操作算完后，必须一次取得全部输出 full 及全部即将释放 buffer 的 empty ID。数量不足时不部分发布，并继续占用 engine、输入和输出；这是可能产生资源死锁的关键模型约定。

| 时间字段 | 含义 |
|---|---|
| enqueue | 真正进入硬件等待队列 |
| start | WAIT 成功、资源取得、开始计算 |
| compute_end | 计算耗时结束 |
| end | full/empty 发布成功，资源完成释放 |

`compute_end→end` 是完成阻塞，不能计为有效计算。物理 ID 在完成时才绑定；队列保存逻辑边 `(producer_seq,consumer_seq)`。实际 WAIT/SET 清单是运行后取得的绑定记录，并非预先知道未来 ID 的静态 ISA 编码。generation、逻辑边映射和引用计数的面积/位数不包含在 M 个 ID 数量里。

## 7. 时间、稳态与评价口径

前端解码、入队、仲裁、barrier 分配/SET/WAIT、动态绑定和唤醒均不另计服务时间。在一个时间点内反复处理完成发布、入队和发射，直到无进展；释放的槽位和 ID 可同刻复用。

v1 逐整数时间步模拟，v2/v3 直接推进到下一个计算完成事件；在当前确定时长、无外部延迟事件的合同下，两者保持同一时间语义。执行与 buffer 生命周期采用 `[start,end)`；barrier 的同刻分配和回收另按严格事件序统计，零长度 ID 生命周期仍会产生瞬时占用。

- 总耗时包含启动与排空，默认 100 轮。
- 稳态间隔是平均每完成一轮所需时间；周期可能覆盖多轮。
- 稳态通过完整调度/资源状态归一化后连续三次重复判定；v2/v3 包含相应 buffer/barrier 状态。仅在物理槽等价、无 bank 亲和性的前提下允许槽位重命名。
- 统计窗口排除启动和前端耗尽后的排空区间；未确认稳态、初始化失败、超时、死锁均明确标记，不外推成功吞吐。
- 每轮 RDMA/WDMA/Tensor/Vector 工作量分别为 174/92/229/298 px，资源吞吐下界为 298 px/轮。吞吐效率是 `298/实际稳态间隔`，与四单元平均忙碌比例不同。
- 在当前贪心策略下容量增加可能改变发射顺序，性能不保证单调；必须逐个扫描。容量最小值也不代表所有其他调度策略的全局最优。

## 8. 当前结果的适用条件

以下结果来自当前像素时长、全局前端程序序、默认队列深度 5、各 engine 一个及上述源/汇边界。

| 模型与配置 | 100 轮总耗时 | 稳态间隔 | 解释 |
|---|---:|---:|---|
| v1 OoO | 30097 | 298 | 未加入有限 buffer/ID 容量 |
| v1 FIFO | 37225 | 370 | FIFO 仍异步 |
| v2 OoO，9 buffer | 30097 | 298 | 当前策略下最少达标 buffer；部分预取时刻改变 |
| v2 OoO，11 buffer | 30097 | 298 | 逐条完整复现 v1 时序 |
| v3 OoO，9 buffer＋10 ID | 30097 | 298 | 与对应 v2 逐条时刻一致；9 ID 会死锁 |
| v3 OoO，11 buffer＋12 ID | 30097 | 298 | 与对应 v2 逐条时刻一致；11 ID 会死锁 |
| v3 FIFO，9 buffer＋10 ID | 39185 | 390 | buffer 约束使其不同于 v1 FIFO |
| v3 FIFO，11 buffer＋12 ID | 37225 | 370 | 恢复对应 FIFO 基线 |

可审计数据见 [v1 结果](out/summary.json)、[v2 结果](v2/out/summary.json)、[v3 结果](v3/out/summary.json)。这些是给定模型条件下的仿真证据，不是 RTL/formal 证明或真实硬件的性能、面积、功耗结论。

## 9. 尚未建模的因素

未模拟真实矩阵数据与浮点正确性、跨轮算法递推、缓存/地址别名、DRAM/SRAM 带宽和 bank 冲突、互连传输、DMA 端口与突发限制、执行时间波动、barrier 服务端口宽度、控制路径延迟、分配器或元数据面积，以及中断/异常恢复。数据的可见性简化为生产者成功完成即对下游可读。

因此改变以下任一条件都需要重新验证：程序序、数据依赖、源/汇边界、buffer 大小关系、资源仲裁、完成发布原子边界、engine 接收能力或上述新增延迟。图中存在并行方块不等于实际系统已满足这些条件。

## 10. 配置、运行与阅读导航

以下命令在 **WSL 中的 Git 仓库根目录**执行（该目录直接包含 `pipeline.py`、`v2/`、`v3/`），不依赖作者本机的绝对路径：

```bash
# Python 3.10+；模型和行为测试仅依赖标准库
python3 pipeline.py --compare
python3 v2/run.py
python3 v3/run.py --buffers 9 11 --max-ids 22 --long-iterations 300

python3 -m unittest discover -s tests -v
python3 -m unittest discover -s v2/tests -v
python3 -m unittest discover -s v3/tests -v
```

像素重新测量另需 Pillow，浏览器 QA 另需 Playwright/Chromium；均不是模拟器运行依赖。离线 HTML 下载到本机后用浏览器打开，GitHub 文件页本身不执行其交互脚本。

可配置项包括 `iterations`、`queue_depth`、`operations` 的顺序/时长/依赖和 `count_source_inputs`；v2/v3 的 CLI 另提供容量扫描参数，具体以 `--help` 为准。`memory_contract`、`barrier_contract` 是现有语义的说明元数据，**只修改这些字符串不会自动切换分配或发布算法**；协议变化必须落实到代码与行为测试。

| 建议阅读顺序 | 文档/代码 |
|---|---|
| 查看原图、像素依据与 v1 细节 | [根 README](README.md)、[config.json](config.json)、[pipeline.py](pipeline.py) |
| 理解物理 buffer 生命周期 | [v2/README.md](v2/README.md)、[v2/memory.py](v2/memory.py) |
| 理解动态 barrier 与容量死锁 | [v3/README.md](v3/README.md)、[v3/barrier.py](v3/barrier.py)、[v3/simulator.py](v3/simulator.py) |
| 逐步阅读循环与发射/完成规则 | [v3/pipeline_pseudocode.md](v3/pipeline_pseudocode.md) |
| 查看实际 one-hot 指令绑定 | [instructions_b9_m10.txt](v3/out/instructions_b9_m10.txt) |
| 查看交互流水和生命周期 | [v1 HTML](out/report.html)、[v2 HTML](v2/out/report.html)、[v3 HTML](v3/out/report.html) |
| 检查行为验证证据 | [v2 测试日志](v2/out/tests.log)、[v3 测试日志](v3/out/tests.log)、[v3 浏览器日志](v3/out/browser_check.log) |

历史近似时长及报告在 `history/approximate_v1/`，不与当前像素时长结果混用。v2/v3 通过各自 `baseline_manifest.json` 验证旧版本基线文件；生成结果时应保留有效配置、扫描范围、状态及日志。
