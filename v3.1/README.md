# Modeling v3.1：独立事件池、阻塞 API 与异步 pipe

本版本实现对话中确认的最新伪代码，位于 `modeling/v3.1`。buffer ID 与物理 buffer 一一对应，buffer 和 barrier 使用同构但独立的 free/flag bitmap 事件池；资源在每条任务提交前顺序申请。

直接打开 [out/report.html](out/report.html)。报告包括 OoO/FIFO 两套二维性能矩阵、达标资源边界、12 条详细轨迹、前端阻塞状态、buffer 与 barrier 的存活和复用时序。下载 HTML 后本地浏览器即可打开，无服务器和外部脚本依赖。

## 1. 正式结果

扫描参数：**buffer 1～24 × barrier 1～24 × OoO/FIFO，每组 100 轮，共 1152 个配置**。时长沿用原图像素值，四个 engine 各一个，队列深度 5。边界及相邻配置另做 300 轮复核，共 12 组。

| 策略 | 物理 buffer | 独立 barrier ID | 100 轮总耗时 / px | 稳态间隔 / px | Vector 利用率 |
|---|---:|---:|---:|---:|---:|
| **OoO** | **13** | **11** | **30051** | **298** | **100%** |
| **OoO** | **14** | **10** | **30060** | **298** | **100%** |
| FIFO | 8 | 8 | 37225 | 370 | 80.54% |

OoO 达到资源吞吐上限有两个 Pareto 边界点：**13 buffer＋11 barrier**，或者 **14 buffer＋10 barrier**。它们都不能再在不增加另一类资源的情况下减少资源。这里的最小组合限于当前顺序申请和调度规则以及已扫描范围，不是所有调度算法的全局最优证明。

barrier 数量包含数据依赖事件与共享 buffer 回收完成事件；buffer ID 是另一套独立资源，已经一一计入物理 buffer 数量。没有额外 empty barrier，也没有 `buf_id→empty_id` 映射。

部分边界附近的实际结果：

| Buffer | Barrier | OoO 稳态间隔 / px | 说明 |
|---:|---:|---:|---|
| 9 | 10 | 370 | 旧版 v3 的 9/10 结论不能套用 |
| 11 | 11 | 320.5 | 尚未达到 298 |
| 12 | 11 | 310.5 | 尚未达到 298 |
| 13 | 10 | 311.5 | ID 更紧，前端提交节奏改变 |
| **13** | **11** | **298** | 达标边界 |
| 13 | 12 | 307 | 增加 ID 后反而下降，已保留真实结果 |
| **14** | **10** | **298** | 另一个达标边界 |
| 14 | 12 | 303.6667 | 非单调案例 |
| 14 | 13 | 298 | 恢复满载 |

300 轮复核：13/11 OoO 总耗时 **89651**、14/10 OoO **89660**，均为 298 px/轮；8/8 FIFO 为 **111225**、370 px/轮。所有 12 组复核的状态和稳态间隔与 100 轮一致。

作为高容量参考，128 buffer＋128 barrier 的计算时序逐条匹配 v1：OoO 为 30097/298，FIFO 为 37225/370。两种资源的占用峰值从参考结果读取，OoO 为 **22 buffer、21 barrier**。这个峰值是完整保留该参考时序的用量，并不等于维持相同吞吐所需的最小组合。边界配置可以改变启动阶段和调度次序，所以 30051/30060 比参考总耗时略短，而稳态均为 298。

## 2. 与 v3 的语义区别

| 项目 | v3 | v3.1 |
|---|---|---|
| Buffer 管理 | 存储池＋共享 barrier 池中的 empty | 独立 buffer 事件池，ID 就是物理槽位 |
| 数据 barrier 分配 | 计算结束时申请，再 SET | 提交生产者前申请，计算完成后只 SET |
| Buffer 分配 | 发射时原子取得新 buffer | 前端按伪代码逐个取得，发生在 submit 之前 |
| 资源不足的位置 | 可能阻塞完成，保留 engine | 主要阻塞前端 get 或 submit，已分配资源继续持有 |
| 共享数据回收 | 引用计数触发 empty | 两个消费完成事件＋独立异步回收控制 |
| 初始化 | 每个空闲 buffer 有一个已 SET empty ID | 两池 free 位全 1、flag 全 0，没有初始 empty |
| 排空后 | 每个空闲 buffer 留有 ready empty | 两池所有 ID 都应回到 free-list |

因此 v3.1 采用独立实现，已有 v1/v2/v3 模拟器核心保持各自语义。`v3/pipeline_pseudocode.md` 描述 v3 的 full/empty 共享 ID 协议；本版本的阻塞 API 与独立事件池以本文和 `resources.py`、`simulator.py` 为准。两版的 buffer 图可在 [独立对比报告](../comparison/out/buffer_comparison.html) 中查看。

## 3. 真正的阻塞 API

`resources.py` 中提供实际 generator API：

```python
buf = yield from api.get_buffer(owner)       # 不足时挂在此调用内
yield from api.set_buffer(buf)
event = yield from api.get_barrier(owner)    # 不足时挂在此调用内
yield from api.submit_async(seq)             # 队列满时挂起前端

# 在异步 on_done 控制上下文中：
yield from api.free_buffer(buf)
yield from api.set_barrier([event])

# 在回收控制上下文中：
yield from api.wait_barrier([consumer_a_done, consumer_b_done])
yield from api.free_buffer(shared_buf)
```

公共 get API 只在成功后返回有效 handle，**不会返回 None/BLOCKED 给任务调用者**。内核发现请求暂不可满足时不恢复该 generator；其他 engine/控制上下文继续运行，资源释放后在同一时间点重新服务该请求。它是硬件阻塞行为的离散事件模拟，不使用 OS 线程阻塞来冻结整个进程。

两池均维护 `free_bitmap` 和 `flag_bitmap`：get 清 free 位，set 置 flag，wait/free 等待 flag 后清 flag、置 free 位。buffer 的 flag 表示本次占用已初始化，不是数据就绪；数据就绪只由数据 barrier 表达。one-hot 编码为 `1<<slot`；generation 防止旧句柄访问复用后的资源。两池同编号 ID 互不影响。

## 4. 逐任务提交与保留部分资源

当前程序序仍为 loadK、WDMA K、loadV、WDMA V、MM1、scale、fa0、MM2、fa1、fa2。

每条任务按以下顺序执行 API，全部成功后才进入 engine 队列：

1. 若为首级 RDMA，`get_buffer→set_buffer` 取得源输入。
2. `get_buffer→set_buffer` 取得输出。
3. 逐条 `get_barrier` 取得本任务输出依赖边的事件 ID。
4. 若本任务读取共享输出，额外 get 一个向回收控制报告“消费完成”的 ID。
5. `submit_async`：目标队列满则保持当前程序游标并阻塞。

这不是整批资源预留：若第二次 get 被阻塞，第一次已经获得的资源仍保留。任务也可能已经取得 buffer 和 ID，却尚未入队。记录中分别保留 get、enqueue、start、compute_end、end，不把预分配时间误计为执行时间。

RDMA/WDMA/Tensor/Vector 各只有一个 engine；队列中的 WAIT_ALL 满足且 engine 空闲时才发射。多个循环可入队和重叠，但同一 engine 不允许两个任务同时执行。OoO 选最老的就绪项，FIFO 只检查队首。

同一时间点按“完成回调（engine 顺序）→已注册回收控制→前端继续提交→各 engine 发射”反复执行直到无进展。等待状态变化、set/free 和队列服务都不额外计时。

## 5. fan-out 的精确回收

`vector1R2/fa0` 的同一个输出同时被 Tensor1R2 和 Vector1R3 读取：

- fa0 在提交前获取两个独立的数据 ready ID，完成时一起 SET。
- 两个消费者各自提交前再获取一个消费完成 ID，在自己的 on_done 中 SET。
- 最后一个消费者提交成功后，前端注册异步回收控制。
- 回收控制 WAIT_ALL 两个完成事件，成功回收两个 ID 后，只执行一次 `free_buffer(fa0_buf)`。

每轮因此产生 **10 个数据事件＋2 个回收完成事件**。100 轮完整运行共 1200 个 barrier lease、1200 个 buffer lease（10 个操作输出＋2 个 RDMA 源输入/轮）。两个事件池的物理容量远小于实例总数，依赖复用完成执行。

回收控制不是第五个计算 engine。当前假设它是零耗时、独立可挂起的控制上下文，不限制其服务带宽和挂起数量。这项控制能力已经显式实现和计数事件 ID，但其队列、面积或控制时钟成本尚未建模。

## 6. 死锁与边界条件

阻塞 API 自动唤醒不意味着没有资源死锁。若前端拿不到下一块资源，负责释放资源的消费者或回收控制又尚未提交，系统可以停住。只要没有未来计算事件且所有控制请求均阻塞，就返回 `deadlock` 并保存现场，不报告成功吞吐。

例如 2 buffer/1 barrier：首个 RDMA 取得源输入和输出并提交；前端随即想为 WDMA 取得输出却没有空闲槽。RDMA 完成后释放源输入，前端取得该槽，但申请 WDMA 的输出事件 ID 时发现唯一 ID 仍属于 RDMA→WDMA；WDMA 尚未提交，无法 WAIT 回收该 ID，于是死锁。这是按原伪代码顺序发生的真实资源循环，不会被自动跳过或偷偷释放资源。

首级外部输入数据假定在分配时已准备好，计入 buffer 池；末级结果在 fa2 完成时立即被外部接收。所有 buffer 等大小，单个容量记为 S，总存储为 N×S。未模拟真实矩阵计算、跨轮算法递推、地址别名、bank/带宽冲突或非零控制延迟。

## 7. 验证依据

- **16 项行为测试通过**：独立 bitmap、generation、公共 API 的真实挂起/返回、flag 未 SET 时 free 阻塞、WAIT 回收唤醒 get、跨轮并行、同 engine 互斥、预分配、fan-in/fan-out、精确释放、时间边界、错误生命周期拒绝。
- 包含 12 个固定随机种子 DAG，与 v1 在足够容量时交叉比较计算时刻；当前 pipe 的 OoO/FIFO 高容量参考也逐条匹配 v1。
- 每次模拟都独立核验两类资源的事件顺序、free/flag bitmap、slot/generation 生命周期、数据生产者和消费者、buffer 不覆盖、共享资源恰好回收一次及最终两池排空。
- 扫描 1152 个配置，12 组 300 轮复核，全部保存在实际输出文件。
- 浏览器验收检查每策略 576 个矩阵单元、Pareto 标识、非单调案例、所有保存轨迹、1000 条操作/1200＋1200 个资源实例、图形宽度、点击详情、SVG 导出及 390px 窄屏。
- `baseline_manifest.json` 保存了旧版核心代码及已有未提交文档的哈希，`out/verification.json` 核对全部保持不变。

## 8. WSL 复现与产物

在本 Git 仓库根目录（直接含 `pipeline.py`、`v2/`、`v3/`、`v3.1/`）运行，Python 3.10+ 标准库即可：

```bash
python3 -m unittest discover -s v3.1/tests -v
python3 v3.1/run.py --max-buffers 24 --max-barriers 24 --iterations 100 --long-iterations 300 --workers 4
```

`--workers` 是独立仿真配置的宿主进程并行度，不是被模拟硬件的 engine 数量。每个案例仍只有四个单执行单元。

改变输入使用 `--config`，改变输出位置使用 `--output`。只重绘已有数据的 HTML 使用 `python3 v3.1/run.py --render-only`。配置内 `protocol` 是当前行为说明，不是更改字符串即可切换策略的实现开关。

可选浏览器 QA 依赖 Playwright/Chromium，当前机器在 WSL 的路径为：

```bash
~/.cache/attention-pipeline-qa/bin/python v3.1/browser_check.py \
  --browser ~/.cache/attention-pipeline-qa/browser/chrome-headless-shell-linux64/chrome-headless-shell
```

| 文件 | 内容 |
|---|---|
| `resources.py` | 实际 free/flag bitmap 事件池及 generator 阻塞 API |
| `simulator.py` | 前端、四 engine、回调和回收控制调度；生命周期验证 |
| `config.json` | 当前时长、依赖与协议前提 |
| `run.py` | 双资源扫描、资源边界、长运行复核及报告生成 |
| `out/report.html` | 可直接打开的交互报告 |
| `out/sweep.json` / `out/performance.csv` | 全部 1152 个配置的性能与状态 |
| `out/summary.json` | 资源边界、参考、12 个长运行检查、轨迹列表 |
| `out/trace_*.json` | 已保存配置的指令、API、两类资源和协程等待状态 |
| `out/buffer_events.jsonl` / `out/barrier_events.jsonl` | 默认达标组合的真实事件记录 |
| `out/performance_matrix.png` | OoO 二维性能矩阵截图 |
| `out/pipeline.png` / `out/buffer_lifetimes.png` / `out/barrier_lifetimes.png` | 默认达标组合的实际流水和生命周期 |
| `out/tests.log` / `out/run.log` / `out/browser_check.log` | 行为测试、扫描和浏览器验证日志 |

默认 HTML 选择 13 buffer＋11 barrier 的 OoO 配置；可切换另一达标组合、FIFO、邻近资源配置和死锁轨迹。矩阵点击展示所有案例的真实指标；详细流水只展示已经保存完整轨迹的案例。
