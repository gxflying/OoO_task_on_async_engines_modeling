# Pipe 循环与动态 barrier 伪代码

本文对应当前 v3 的调度语义，用于说明循环提交、硬件资源限制、buffer 复用和 full/empty barrier 生命周期。它是说明性伪代码，不是可直接运行的 Python；实际实现见 [simulator.py](simulator.py) 与 [barrier.py](barrier.py)。

## 1. 硬件与初始化

RDMA、WDMA、Tensor、Vector **各一个执行单元、各一个指令队列**。每个执行单元同时最多执行一条指令，且只有成功完成发布后才释放；计算结束但 barrier ID 不足时仍保持占用。

```python
BUFFER_COUNT  = 9
BARRIER_COUNT = 10
QUEUE_DEPTH   = 5
LOOP_COUNT    = 100

ENGINES = [RDMA, WDMA, TENSOR, VECTOR]

buffer_pool  = create_buffer_pool(BUFFER_COUNT)
barrier_pool = create_barrier_pool(BARRIER_COUNT)
queues       = {engine: Queue(QUEUE_DEPTH) for engine in ENGINES}
running      = {engine: None for engine in ENGINES}

# 每块空闲 buffer 初始持有一个已 SET 的动态 empty ID。
# 必须有 BUFFER_COUNT 个可用 ID，否则初始化不能成功。
initial_ids = GET_IDS_ATOMIC(BUFFER_COUNT)
for buf, id in zip(buffer_pool, initial_ids):
    buf.empty = id
    SET(id)
```

单个 ID 编码为 `1 << slot`；多个 WAIT 的 mask 为这些 one-hot 编码的 OR。物理 ID 复用时递增 generation，通过 `(slot, generation)` 区分不同使用实例。full 与 empty 共用同一个 ID 池。

## 2. 循环展开与异步提交

`F0…F9` 是每轮独立的**逻辑依赖引用**，创建它们不会占用物理 barrier ID。生产者成功完成时才取得物理 ID，消费者通过对应的逻辑边获得绑定结果。

```python
for i in range(LOOP_COUNT):
    F = new_dependency_refs(count=10, iteration=i)

    # SUBMIT(engine, wait_full, operation, set_full)
    # 按程序序入队；目标队列满时反压全局前端。
    # 入队成功即可提交下一条，不等当前指令执行完成。

    SUBMIT(RDMA,   [],          load1R1(i),   [F[0]])
    SUBMIT(WDMA,   [F[0]],      wdma1R1(i),   [F[2]])

    SUBMIT(RDMA,   [],          load1R2(i),   [F[1]])
    SUBMIT(WDMA,   [F[1]],      wdma1R2(i),   [F[5]])

    SUBMIT(TENSOR, [F[2]],      tensor1R1(i), [F[3]])
    SUBMIT(VECTOR, [F[3]],      vector1R1(i), [F[4]])
    SUBMIT(VECTOR, [F[4]],      vector1R2(i), [F[6], F[7]])

    SUBMIT(TENSOR, [F[5],F[6]], tensor1R2(i), [F[8]])
    SUBMIT(VECTOR, [F[7]],      vector1R3(i), [F[9]])
    SUBMIT(VECTOR, [F[8],F[9]], vector1R4(i), [])

DRAIN_ALL_ENGINES()
```

实际代码先生成循环的指令描述，再由前端游标逐条送入有限队列，与上述提交语义一致。生成描述、入队和开始执行是三个不同阶段；不会为每轮创建一套执行单元。

输出的全部消费者来自已知依赖图，包括尚未入队、尚未开始的消费者。`vector1R2` 的两个 full 引用指向同一块输出 buffer，但分别属于不同依赖边。

## 3. 发射条件与单执行单元限制

```python
def TRY_ISSUE(op):
    # 硬件资源检查独立于 barrier 依赖检查。
    if running[op.engine] is not None:
        return BLOCKED_ENGINE_BUSY

    if not ALL_FULL_READY(op.wait_full):
        return BLOCKED_DEPENDENCY

    # 普通指令另申请一个输出 buffer。
    # 首级 RDMA 还需要一个池内外部输入 buffer。
    needed = 1 + (1 if op.is_root else 0)
    new_buffers = find_free_buffers_with_ready_empty(needed)
    if new_buffers is None:
        return BLOCKED_BUFFER

    # 必须在回收 full ID 前取得它们对应的数据 buffer。
    inputs = resolve_upstream_buffers(op.wait_full)

    atomic:
        # 只有全部条件齐备、现在就能开始，才消费并回收 WAIT。
        WAIT_AND_FREE_ALL(op.wait_full)

        for buf in new_buffers:
            WAIT_AND_FREE(buf.empty)
            claim_new_buffer_generation(buf)

        op.output = new_buffers[0]
        if op.is_root:
            inputs.append(new_buffers[1])   # 外部输入已就绪
        op.inputs = inputs

        # 输出与输入不能同址；上游输出与下游输入共用物理 buffer。
        assert op.output not in op.inputs
        op.output.remaining_consumers = all_consumers_of(op)
        mark_inputs_reading(op)

        remove_from_queue(op)
        running[op.engine] = op
        op.start = now
        op.compute_end = now + latency[op]

    return ISSUED
```

OoO 从各队列选择最老的、同时满足依赖与资源条件的指令；FIFO 只检查队首。RDMA 的 `wait_full=[]` 只表示没有上游数据依赖，它仍需检查唯一 RDMA 单元是否空闲，并 WAIT 输入、输出两块 buffer 的 empty。

## 4. 完成、发布与回收

```python
def TRY_COMPLETE(op):
    if now < op.compute_end:
        return STILL_COMPUTING

    # 按当前已成功完成的状态重新计算，不预先批量计算多条指令。
    # 包括尚未发射的消费者；只在本 op 是最后消费者时释放。
    released_buffers = [
        buf for buf in op.inputs
        if buf.remaining_consumers == {op}
    ]

    # 末级输出由外部 sink 在成功完成时立即接收。
    if op.set_full == []:
        released_buffers.append(op.output)

    full_count  = len(op.set_full)
    empty_count = len(released_buffers)

    # 一次性得到全部 ID，失败时不得半分配或提前释放资源。
    ids = TRY_GET_IDS_ATOMIC(full_count + empty_count)
    if ids is None:
        # 保留 running、输入读者、输出和所有 buffer 所有权。
        return BLOCKED_ON_BARRIER_IDS

    full_ids  = ids[:full_count]
    empty_ids = ids[full_count:]

    atomic:
        finish_input_reads(op)
        mark_output_ready(op.output)

        for buf in released_buffers:
            release_buffer(buf)

        # 每条依赖边一个 full；fan-out 多个 full 指向同一输出。
        for ref, id in zip(op.set_full, full_ids):
            bind(ref, id, data_buffer=op.output)
            SET(id)

        for buf, id in zip(released_buffers, empty_ids):
            buf.empty = id
            SET(id)

        op.end = now
        mark_operation_complete(op)
        running[op.engine] = None

    return COMPLETED
```

`compute_end` 是计算结束时刻，`end` 是成功发布、释放执行单元的时刻。full 在消费者**开始**时回收；数据 buffer 则要等全部直接消费者**成功完成**后才可复用。两者不能混淆。

## 5. 同一时间点的调度循环

```python
while unfinished_operations_exist():
    repeat:
        progress = False

        # 固定仲裁顺序与当前实现一致。
        for engine in [RDMA, WDMA, TENSOR, VECTOR]:
            op = running[engine]
            if op is not None:
                progress |= (TRY_COMPLETE(op) == COMPLETED)

        # 遇目标队列满则停；不会跳过阻塞指令派发后面的指令。
        dispatch_in_program_order_until_queue_full()

        for engine in [RDMA, WDMA, TENSOR, VECTOR]:
            if running[engine] is not None:
                continue

            op = select_oldest_eligible_queued_op(engine)
            if op is not None:
                progress |= (TRY_ISSUE(op) == ISSUED)

    until not progress

    # WAIT 刚回收的 ID 可以同一时刻解除另一操作的完成阻塞。
    # 同一时刻释放的 buffer 也允许立即复用。
    if all_operations_completed():
        break
    if no_future_compute_completion_exists():
        report_deadlock()
        break

    now = next_future_compute_completion_time()
```

以上省略了实际代码中的最大时间上限、轨迹记录和独立验证器。barrier、buffer 与执行单元资源的检查及释放顺序均按当前 v3 语义表达。

## 6. tensor1R1 → vector1R1 的局部对应关系

```text
tensor1R1:
    WAIT 上游 wdma1R1 的 full(F2)
    WAIT 自己输出 buffer 的 empty
    执行 tensor1R1
    GET_AND_SET full(F3)                 # 通知 vector1R1 数据就绪
    GET_AND_SET 已消费完输入 buffer 的 empty

vector1R1:
    WAIT full(F3)，回收该 full ID
    WAIT 自己输出 buffer 的 empty
    读取 tensor1R1 的输出，执行 vector1R1
    GET_AND_SET full(F4)                 # 通知 vector1R2
    GET_AND_SET tensor 输出 buffer 的 empty  # 此时才允许覆盖复用
```

这个局部写法中的 GET_AND_SET 仍属于完成时的同一原子批次，并不表示允许先后独立成功、部分发布。

完整执行轨迹见 [out/instructions_b9_m10.txt](out/instructions_b9_m10.txt)，其中包含每条操作实际绑定的物理 ID、generation、one-hot mask 和起止时刻。
