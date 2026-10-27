# Pipe 循环：阻塞 API、双 free-list 与异步执行

本文以**调用本身阻塞、成功才返回**的 API 语义描述模型。`try_get_buffer`、`try_get_barrier_id` 不向上层返回 BLOCKED 或 None；资源暂不可用时挂起当前调用上下文，由调度器继续执行其他工作，资源变化后恢复。

这是说明性伪代码，不是可直接运行的 Python。现有 [simulator.py](simulator.py) 用单线程事件循环、队列保留及固定点重试实现阻塞行为，并没有名为这两个 API 的 Python 阻塞函数。实际 buffer 空闲集合由 `mem.live` 的补集表示，ID 空闲集合由 `ids.live` 的补集表示；本文 bitmap 是这些状态的显式表示。没有新增硬件功能或重新生成性能结果。

## 1. 三类 bitmap，各自只表达一种状态

| 状态 | bit=1 的含义 | 修改时点 |
|---|---|---|
| `buffer_free_bitmap` | 对应物理 buffer 可分配，且其当前 empty 已 SET | 分配清位；最后消费者完成并发布 empty 后置位 |
| `barrier_free_bitmap` | 对应物理 ID 未被占用，可用于任何新 full/empty 事件 | 分配清位；成功 WAIT 自动回收时置位 |
| `barrier_ready_bitmap` | 对应已分配 ID 的事件已 SET | SET 置位；成功 WAIT 消费时清位 |

ID 已分配但尚未 SET 时，free=0、ready=0；因此不能用一个 bitmap 同时表达空闲与就绪。单个 ID 的 one-hot 编码是 `1 << slot`，多事件 WAIT/SET 的 mask 是多个 one-hot 的 OR。

full 和 empty 共用一个动态 ID free-list，没有永久固定的一对 ID。每条依赖边一个 full ID；多个消费者对应多个独立 full ID，生产者在完成时 SET 这些 ID，各消费者成功 WAIT 自己的 ID 后自动回收。

辅助元数据包括 buffer 的生产者/全部消费者/读者集合、`empty_ref`、逻辑依赖边到实际 ID 的绑定，以及 buffer/ID 的 generation。它们不额外算作物理 barrier ID。

## 2. 谁处理阻塞，谁继续运行

| 调用上下文 | 阻塞时保留什么 | 由什么唤醒 |
|---|---|---|
| 前端 `submit` | 当前程序游标，尚未入队的指令 | 目标队列释放一项 |
| 待发射指令的 `try_get_buffer` | 指令仍在原队列，不预占 engine、不消费 WAIT、不半分配 buffer | buffer 的 empty 发布，或依赖/engine 条件改变 |
| 完成发布的 `try_get_barrier_id` | 已运行指令、engine、输入读者与输出 buffer | 其他成功 WAIT 自动回收 ID |

阻塞的是当前协程/硬件请求上下文，**不是整个仿真器，也不是所有执行单元**。`suspend(ticket)` 交还控制权；调度器推进其他可运行上下文和计算完成事件。若所有请求都阻塞且不存在未来事件，则报告真实死锁；初始化不可能满足时直接报告初始化失败。

`subscribe(...)` 在检查资源的同一个临界区内登记等待，随后离开临界区再 `suspend(ticket)`。通知是持久的，若通知先于 suspend 到达，suspend 立即继续。唤醒后必须重新检查当前 bitmap，不能假定资源仍归自己；挂起期间不持有全局锁。

## 3. 资源初始化

```python
BUFFER_COUNT, BARRIER_COUNT, QUEUE_DEPTH = 9, 10, 5
ENGINES = [RDMA, WDMA, TENSOR, VECTOR]   # 每种各一个

buffer_free_bitmap  = 0                 # empty 初始化完成才公布可用
barrier_free_bitmap = (1 << BARRIER_COUNT) - 1
barrier_ready_bitmap = 0
running = {e: None for e in ENGINES}
queues  = {e: Queue(QUEUE_DEPTH) for e in ENGINES}

if BARRIER_COUNT < BUFFER_COUNT:
    raise InitializationFailure()

def initial_plan():
    return Plan(empty_events=[(slot, 0) for slot in range(BUFFER_COUNT)])

def commit_initial(plan, ids):
    for slot, id in zip(range(BUFFER_COUNT), ids):
        empty_ref[slot] = resolved_ref(id, buffer_generation=0)
        SET(id)
        buffer_free_bitmap |= 1 << slot

try_get_barrier_id(initial_plan, on_grant=commit_initial)
```

## 4. ID 的阻塞分配、SET 和 WAIT 自动回收

`try_get_barrier_id` 批量申请本次发布所需 ID，free-list 不足则挂起调用者。完成计划用回调在每次重试时重新计算，避免其他消费者完成后，本次需要新增的 empty 数量变化而沿用过期数量。

```python
def try_get_barrier_id(build_plan, on_grant):
    while True:
        with resource_atomic():
            plan = build_plan()             # 重新计算 full/empty 事件集合
            needed = len(plan.events)
            if popcount(barrier_free_bitmap) >= needed:
                slots = lowest_set_bits(barrier_free_bitmap, needed)
                barrier_free_bitmap &= ~mask(slots)
                ids = new_id_generations(slots)
                register_owners(ids, plan.events)

                # 不插入调度让步：分配与发布提交属于同一原子事务。
                on_grant(plan, ids)
                return ids                  # 仅成功返回；没有 BLOCKED/None

            ticket = subscribe(ID_RECYCLED)
        suspend(ticket)                     # 只挂起当前调用者，其他请求继续


def SET(id):
    assert current_generation(id) and id_is_allocated(id)
    assert not id_is_ready(id)
    barrier_ready_bitmap |= id.one_hot
    notify_subscribers(EVENT_SET, id)


def consume_ready_ids(ids):
    # 由发射事务调用；所有其他发射条件必须已经满足。
    assert all(current_generation(id) for id in ids)
    bits = OR(id.one_hot for id in ids)
    assert (barrier_ready_bitmap & bits) == bits
    barrier_ready_bitmap &= ~bits
    barrier_free_bitmap  |= bits
    retire_id_owners(ids)
    notify_subscribers(ID_RECYCLED)
    # 这就是成功 WAIT 的原子副作用，不要求调用者另外调用 free(id)。
```

`on_grant` 是同步提交回调，不得再次阻塞或让步。上述 API 和回调是对实际 `BarrierPool.allocate_many / signal / consume_and_release` 加调度等待的表达，不声称源文件已有同名封装。

## 5. Buffer 的阻塞分配：先查 free-list，不足则等释放 barrier

```python
def try_get_buffer(op, on_grant):
    needed = 1 + int(op.is_root)     # 新输出 + 首级 RDMA 的池内源输入

    while True:
        with resource_atomic():
            if popcount(buffer_free_bitmap) < needed:
                # 没有足够空闲项，等任一在用 buffer 的释放事件。
                ticket = subscribe_any(current_buffer_release_refs())
            elif not issue_admission_granted(op):
                # engine busy、full 尚未 SET，或尚未轮到本指令。
                # 不提前分配 buffer；否则会改变当前 v3 的资源调度规则。
                ticket = subscribe(ENGINE_RELEASED, FULL_PUBLISHED,
                                   BUFFER_EMPTY_PUBLISHED, ADMISSION_CHANGED)
            else:
                slots = lowest_set_bits(buffer_free_bitmap, needed)
                empties = [empty_ref[s].physical_id for s in slots]
                fulls = resolve_full_ids(op.wait_full)
                assert all_ready(fulls + empties)

                # 取得输入数据句柄后再回收其 full ID。
                inputs = resolve_upstream_buffers(op.wait_full)
                consume_ready_ids(fulls + empties)
                buffer_free_bitmap &= ~mask(slots)
                handles = claim_buffer_generations(slots)

                # 为本次占用建立新的逻辑释放引用，尚不分配物理 empty ID。
                for h in handles:
                    release_ref[h] = new_unbound_release_ref(h)

                on_grant(handles, inputs)    # 同一事务里占用 engine 并开始
                return handles              # 仅成功返回

        suspend(ticket)
```

**动态 empty 的必要细节：** buffer 在用时，下一次 empty 的物理 ID 尚未分配。等待者先订阅逻辑 `release_ref(slot,generation)`；最后消费者发布 empty 时绑定实际 ID 并通知。不能提前等待一个凭空假设存在的物理 ID，也不能为每个等待者额外分配一个 ID。

释放通知只是唤醒，不消费 empty。被唤醒者重新检查 free-list，只有真正取得对应 buffer 的新写入者才执行 WAIT 并自动回收那个 empty ID。即使另一个请求先取得该 buffer，后续等待者也不会重复消费旧 ID。

`issue_admission_granted` 属于统一调度器：检查唯一 engine 空闲、全部 full ready，且本请求是当前策略选中的可发射项。OoO 选最老的全部条件满足者；FIFO 只选队首。待发射请求均可登记，但不因某个请求挂起而禁止其他 engine 或合法的 OoO 候选执行。

## 6. 执行上下文与完成上下文

```python
def issue_context(op):
    def start_engine(handles, inputs):
        op.output = handles[0]
        op.inputs = inputs + (handles[1:] if op.is_root else [])
        if op.is_root:
            initialize_external_input(handles[1], consumer=op, ready=True)
        assert op.output not in op.inputs
        initialize_output_consumers(op.output, all_consumers_of(op))
        begin_read_all(op.inputs, op)
        queues[op.engine].remove(op)
        running[op.engine] = op
        op.start = now
        op.compute_end = now + latency[op]
        schedule_event(op.compute_end, completion_context, op)

    try_get_buffer(op, on_grant=start_engine)   # 调用本身可以挂起
    # 返回后已开始异步计算，本上下文不阻塞等待计算结束。


def completion_context(op):
    # 此处 now >= compute_end，但 engine 和 buffer 仍归 op。
    def current_publish_plan():
        released = [h for h in op.inputs if remaining_consumers(h) == {op}]
        if not op.set_full:                    # 外部 sink 立即接收末级结果
            released.append(op.output)
        return Plan(full_events=op.set_full, empty_events=released)

    def commit_publish(plan, ids):
        full_ids, empty_ids = split_ids(ids, plan)

        # 实际 BufferPool.finish_read/finish_write 已负责最后读者回收。
        # 不再对同一 lease 重复调用 release。
        for h in op.inputs:
            memory.finish_read(h, op, now)
        memory.finish_write(op.output, now)

        for edge, id in zip(plan.full_events, full_ids):
            bind_full(edge, id, op.output)
            SET(id)

        for h, id in zip(plan.empty_events, empty_ids):
            assert buffer_lease_released(h)
            release_ref[h].bind(id)
            empty_ref[h.slot] = release_ref[h]
            SET(id)
            buffer_free_bitmap |= 1 << h.slot
            notify_subscribers(BUFFER_EMPTY_PUBLISHED, release_ref[h])

        op.end = now
        mark_done(op)
        running[op.engine] = None
        notify_subscribers(ENGINE_RELEASED, op.engine)

    # 挂起时保留 engine、所有输入读者和输出，不提前释放或部分发布。
    try_get_barrier_id(current_publish_plan, on_grant=commit_publish)
```

## 7. 循环只提交描述，不在前端串行执行硬件任务

```python
def frontend_context():
    for i in range(100):
        F = new_dependency_refs(10, iteration=i)   # 逻辑引用，不占物理 ID

        # submit 是队列提交 API：队列满时本调用挂起前端；其他 engine 继续。
        submit(RDMA,   [],          load1R1(i),   [F[0]])
        submit(WDMA,   [F[0]],      wdma1R1(i),   [F[2]])
        submit(RDMA,   [],          load1R2(i),   [F[1]])
        submit(WDMA,   [F[1]],      wdma1R2(i),   [F[5]])
        submit(TENSOR, [F[2]],      tensor1R1(i), [F[3]])
        submit(VECTOR, [F[3]],      vector1R1(i), [F[4]])
        submit(VECTOR, [F[4]],      vector1R2(i), [F[6], F[7]])
        submit(TENSOR, [F[5],F[6]], tensor1R2(i), [F[8]])
        submit(VECTOR, [F[7]],      vector1R3(i), [F[9]])
        submit(VECTOR, [F[8],F[9]], vector1R4(i), [])

    drain_all()     # 挂起前端，等所有 engine 完成；调度器仍运行


def event_loop():
    start_context(frontend_context)
    while not all_operations_completed():
        # 同时可推进时：完成发布 → 前端入队 → 发射；各阶段的 engine 顺序
        # 均为 RDMA、WDMA、Tensor、Vector。通知先唤醒，再按该顺序仲裁。
        run_runnable_contexts_to_quiescence()

        if all_operations_completed():
            return
        if no_future_compute_event() and no_runnable_context():
            raise ResourceDeadlock(snapshot_all_waiters())

        advance_to_next_compute_event_and_wake_contexts()
```

`submit` 成功入队后登记该指令的唯一待发射请求，不重复创建上下文。调度器始终保留这些请求；挂起/唤醒不是返回失败后重新创建指令。队列满只阻塞全局前端当前游标，不能跳过它去提交后面的其他 engine 指令。

## 8. 与当前实现的对应关系及边界

| 本文表达 | 当前实现 |
|---|---|
| buffer free-list bitmap | `BufferPool.live` 补集及 `free_count` |
| ID free-list bitmap | `BarrierPool.live` 补集及 `free_count` |
| ID ready bitmap | `BarrierPool.ready_mask` |
| buffer 原子分配 | `BufferPool.allocate_many` |
| ID 原子分配 | `BarrierPool.allocate_many` |
| 成功 WAIT 自动回收 | `BarrierPool.consume_and_release` |
| SET | `BarrierPool.signal` |
| 逻辑依赖/empty 绑定 | `full_edges` / `empty_slots` |
| 挂起并重试 | 队列或 `running` 原位保留，同刻固定点重试和未来事件推进 |

源码现在用事件循环实现这些阻塞效果，不是 OS 线程阻塞 API。本文的上下文、订阅、回调和 bitmap 表达是为了把责任和调用顺序写清楚；没有声称新增了实际协程运行时或显式 bitmap 分配器。

保留原有约定：成功 WAIT 与实际发射绑定；同刻 SET/WAIT/回收没有额外延迟；完成发布整批原子申请 full+empty；首级 RDMA 有池内输入；末级输出立即被外部接收。如果改为“先消耗 full、再等 engine/buffer”或前端提前分配 buffer，资源生命周期和 barrier 最小数量都需要重新仿真，不能沿用旧结果。

完整实际绑定记录见 [out/instructions_b9_m10.txt](out/instructions_b9_m10.txt)，实现细节见 [simulator.py](simulator.py)、[barrier.py](barrier.py)、[BufferPool](../v2/memory.py)。
