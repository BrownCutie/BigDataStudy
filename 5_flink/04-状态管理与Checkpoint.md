# 04 - 状态管理与 Checkpoint：让流处理拥有"记忆"

## 一、为什么需要状态？

在上一篇中，我们用 `keyBy().sum(1)` 统计了每篇文章的浏览量。但你有没有想过：Flink 是怎么"记住"之前的数据的？

```
┌─────────────────────────────────────────────────────────────┐
│                 为什么需要状态？                                │
│                                                             │
│  场景：统计每篇文章的浏览量                                    │
│                                                             │
│  数据流：──●(文章1)──●(文章2)──●(文章1)──●(文章3)──→        │
│                                                             │
│  处理过程：                                                   │
│  ── 文章1 被浏览 → 文章1 浏览量 = 1                          │
│  ── 文章2 被浏览 → 文章2 浏览量 = 1                          │
│  ── 文章1 被浏览 → 文章1 浏览量 = 2  ← 需要记住上次是 1！    │
│  ── 文章3 被浏览 → 文章3 浏览量 = 1                          │
│                                                             │
│  这个"记住上次值"的机制，就是**状态（State）**。                │
│                                                             │
│  没有状态，流处理就只能做无状态的转换（如 map、filter），        │
│  无法做聚合、去重、复杂事件处理等有状态的计算。                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 二、Flink 中的状态分类

Flink 中的状态分为两大类：

```
┌─────────────────────────────────────────────────────────────────┐
│                     Flink 状态分类体系                           │
│                                                                 │
│                          State（状态）                          │
│                       ┌────────┴────────┐                      │
│                       │                 │                      │
│              Keyed State          Operator State                │
│           （按键分区状态）        （算子状态）                    │
│                       │                 │                      │
│      ┌────────────────┐    ┌────────────────────┐            │
│      │                │    │                    │            │
│      │ 只能在          │    │ 可以在任意算子      │            │
│      │ KeyedStream    │    │ 中使用              │            │
│      │ 上使用          │    │                    │            │
│      │                │    │                    │            │
│      │ 每个Key独立    │    │ 每个并行实例独立    │            │
│      │ 的状态          │    │ 的状态              │            │
│      │                │    │                    │            │
│      └────────────────┘    └────────────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.1 Keyed State（按键分区状态）

Keyed State 是最常用的状态类型，它绑定在 KeyedStream 上，每个 Key 都有自己独立的状态。

```
┌─────────────────────────────────────────────────────────────┐
│                    Keyed State 示意                           │
│                                                             │
│  KeyedStream (keyBy postId):                                │
│                                                             │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐ │
│  │  Key = 文章1   │   │  Key = 文章2   │   │  Key = 文章3   │ │
│  │               │   │               │   │               │ │
│  │  State:       │   │  State:       │   │  State:       │ │
│  │  viewCount=5  │   │  viewCount=3  │   │  viewCount=8  │ │
│  │  likeCount=2  │   │  likeCount=1  │   │  likeCount=4  │ │
│  │               │   │               │   │               │ │
│  └───────────────┘   └───────────────┘   └───────────────┘ │
│                                                             │
│  每个 Key 有自己独立的状态空间，互不干扰                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Keyed State 的类型：**

| 状态类型 | 说明 | 示例 |
|---------|------|------|
| `ValueState<T>` | 存储单个值 | 记录某个用户的最后访问时间 |
| `ListState<T>` | 存储一个列表 | 记录某个用户的最近浏览记录 |
| `MapState<K, V>` | 存储键值对 | 记录每篇文章的各维度指标 |
| `ReducingState<T>` | 自动聚合 | 自动求最大值/最小值 |
| `AggregatingState<I, O>` | 自定义聚合 | 计算平均值 |

### 2.2 Operator State（算子状态）

Operator State 绑定在算子实例上，每个并行实例有自己的状态。常用于 Source（记录读取偏移量）等场景。

```
┌─────────────────────────────────────────────────────────────┐
│                    Operator State 示意                       │
│                                                             │
│  Kafka Source 算子（并行度 = 3）：                            │
│                                                             │
│  ┌───────────────────┐                                      │
│  │  并行实例 1         │                                      │
│  │  State: offset=100 │  ← 记录 Partition 0 的消费偏移量     │
│  └───────────────────┘                                      │
│  ┌───────────────────┐                                      │
│  │  并行实例 2         │                                      │
│  │  State: offset=250 │  ← 记录 Partition 1 的消费偏移量     │
│  └───────────────────┘                                      │
│  ┌───────────────────┐                                      │
│  │  并行实例 3         │                                      │
│  │  State: offset=380 │  ← 记录 Partition 2 的消费偏移量     │
│  └───────────────────┘                                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 三、状态后端（State Backend）

状态需要存储在哪里？这就是状态后端负责的事情。

```
┌─────────────────────────────────────────────────────────────────┐
│                    两种状态后端对比                               │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  HashMapStateBackend（默认）                                │ │
│  │                                                           │ │
│  │  存储位置：JVM 堆内存                                       │ │
│  │  读写速度：极快（纳秒级）                                    │ │
│  │  状态大小：受限于 TaskManager 内存                           │ │
│  │  适用场景：状态较小（GB 级以内）                              │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────┐                  │ │
│  │  │         JVM Heap Memory             │                  │ │
│  │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐  │                  │ │
│  │  │  │State│ │State│ │State│ │State│  │                  │ │
│  │  │  └─────┘ └─────┘ └─────┘ └─────┘  │                  │ │
│  │  └─────────────────────────────────────┘                  │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  EmbeddedRocksDBStateBackend                                │ │
│  │                                                           │ │
│  │  存储位置：本地磁盘（RocksDB 嵌入式数据库）                   │ │
│  │  读写速度：较快（微秒级，有磁盘 IO）                          │ │
│  │  状态大小：可支持 TB 级别                                    │ │
│  │  适用场景：状态较大（超过内存容量）                            │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────┐                  │ │
│  │  │         RocksDB (磁盘)               │                  │ │
│  │  │  ┌──────────────────────────────┐   │                  │ │
│  │  │  │  SST Files (有序数据文件)     │   │                  │ │
│  │  │  │  WAL (预写日志)              │   │                  │ │
│  │  │  │  Block Cache (读缓存)        │   │                  │ │
│  │  │  └──────────────────────────────┘   │                  │ │
│  │  └─────────────────────────────────────┘                  │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  如何选择？                                                      │
│  - 状态 < 几 GB → HashMapStateBackend（更快）                    │
│  - 状态 > 几 GB → EmbeddedRocksDBStateBackend（更大）            │
│  - 不确定 → 先用 HashMap，内存不够再切换                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**配置状态后端：**

```yaml
# flink-conf.yaml
state.backend.type: hashmap

# 或者使用 RocksDB（需要额外依赖）
# state.backend.type: rocksdb
# state.backend.rocksdb.localdir: /opt/flink/rocksdb
```

```python
# 也可以在代码中配置
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import CheckpointingMode

env = StreamExecutionEnvironment.get_execution_environment()

# 启用 Checkpoint
env.enable_checkpointing(30000)  # 间隔 30 秒

# 设置状态后端
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
```

## 四、Checkpoint 机制

### 4.1 什么是 Checkpoint？

```
┌─────────────────────────────────────────────────────────────┐
│                    Checkpoint 的作用                          │
│                                                             │
│  流处理是 7x24 小时运行的，但程序可能因为各种原因失败：          │
│  - 机器故障                                                 │
│  - 网络问题                                                 │
│  - 代码 Bug                                                 │
│  - OOM（内存溢出）                                          │
│                                                             │
│  如果没有 Checkpoint：                                       │
│  ──●──●──●──●──●── [故障] ── 从头重新处理 ──→              │
│                                    ↑                       │
│                              丢失了大量计算进度！              │
│                                                             │
│  有了 Checkpoint：                                           │
│  ──●──●──●──●──●── [故障] ── 从最近的 Checkpoint 恢复 ──→  │
│                          ↑                                  │
│                    从断点继续，不丢数据！                      │
│                                                             │
│  Checkpoint 就是给流处理拍"快照"，定期保存当前状态。            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Chandy-Lamport 算法原理

Flink 的 Checkpoint 基于 **Chandy-Lamport 分布式快照算法**。我们来通俗地理解一下：

```
┌─────────────────────────────────────────────────────────────────┐
│               Checkpoint 执行流程（简化版）                       │
│                                                                 │
│  JobManager 定期向 Source 注入 Checkpoint Barrier                │
│                                                                 │
│  Step 1: JobManager 发起 Checkpoint #5                          │
│                                                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                    │
│  │ Source   │    │  Map    │    │  Sink   │                    │
│  │         │    │         │    │         │                    │
│  │ ●──●──● │    │ ●──●──● │    │ ●──●──● │                    │
│  │       [5]──→ │       [5]──→ │       [5]  ← Barrier 到达    │
│  │ ●──●──● │    │ ●──●──● │    │ ●──●──● │      各算子保存状态  │
│  └─────────┘    └─────────┘    └─────────┘                    │
│      │              │              │                           │
│      │              │              │                           │
│      ▼              ▼              ▼                           │
│  保存 offset    保存中间状态    确认写入完成                     │
│                                                                 │
│  Step 2: 所有算子向 JobManager 报告快照完成                      │
│                                                                 │
│  Step 3: JobManager 确认 Checkpoint #5 完成                     │
│                                                                 │
│  ─────────────────────────────────────────────────────         │
│                                                                 │
│  关键概念：Barrier（栅栏/屏障）                                  │
│  - Barrier 随着数据流一起流动                                    │
│  - Barrier 之前的数据属于 Checkpoint #5                         │
│  - Barrier 之后的数据属于 Checkpoint #6                         │
│  - 当 Barrier 经过某个算子时，该算子保存自己的状态                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────┐
│               Barrier 在数据流中的传递                          │
│                                                             │
│  Source 输出：                                               │
│  ── data ── data ── [B5] ── data ── data ── [B6] ──→       │
│                                              ↑               │
│                                         Barrier 标记         │
│                                         表示 Checkpoint #6   │
│                                                             │
│  Barrier 的对齐（Alignment）：                                 │
│                                                             │
│  假设 Map 有 2 个输入：                                       │
│                                                             │
│  输入 1：── data ── [B5] ── data ── data ──→               │
│  输入 2：── data ── data ── [B5] ── data ──→               │
│                                ↑                            │
│  Map 必须等到两个输入的 B5 都到达后，才能保存状态并继续处理     │
│  这就是 Barrier 对齐，保证了状态的一致性                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 Checkpoint 配置

```yaml
# flink-conf.yaml

# 启用 Checkpoint，间隔 30 秒
execution.checkpointing.interval: 30s

# Checkpoint 模式：精确一次（EXACTLY_ONCE）或至少一次（AT_LEAST_ONCE）
execution.checkpointing.mode: EXACTLY_ONCE

# Checkpoint 超时时间（默认 10 分钟）
execution.checkpointing.timeout: 10min

# 最小间隔（两个 Checkpoint 之间至少间隔多久）
execution.checkpointing.min-pause: 1s

# 最大并发 Checkpoint 数量（默认 1）
execution.checkpointing.max-concurrent-checkpoints: 1

# Checkpoint 存储路径
state.checkpoints.dir: file:///opt/flink/checkpoints

# 外部化 Checkpoint（作业取消后保留）
state.checkpoints.dir: file:///opt/flink/externalized-checkpoints
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

```python
# 代码中配置
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import CheckpointingMode, RestartStrategies

env = StreamExecutionEnvironment.get_execution_environment()

# 启用 Checkpoint，间隔 30 秒
env.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)

# Checkpoint 超时时间
env.get_checkpoint_config().set_checkpoint_timeout(60000)

# 最小间隔
env.get_checkpoint_config().set_min_pause_between_checkpoints(1000)

# 并发数
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

# 外部化
env.get_checkpoint_config().set_externalized_checkpoint_cleanup(
    True  # 作业取消时保留 Checkpoint
)

# 重启策略：固定延迟重启
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))
# 最多重启 3 次，每次间隔 10 秒
```

## 五、Savepoint 与 Checkpoint 的区别

```
┌─────────────────────────────────────────────────────────────────┐
│              Savepoint vs Checkpoint                             │
│                                                                 │
│  ┌──────────────────┬──────────────────┬─────────────────────┐ │
│  │       对比项       │   Checkpoint    │    Savepoint         │ │
│  ├──────────────────┼──────────────────┼─────────────────────┤ │
│  │ 触发方式          │ 自动（定时）      │ 手动（命令触发）       │ │
│  │ 用途              │ 故障恢复          │ 版本升级/作业迁移      │ │
│  │ 生命周期          │ 作业取消后默认删除 │ 永久保存              │ │
│  │ 格式              │ 增量（更快）      │ 完整（更兼容）         │ │
│  │ 修改代码后恢复     │ 可能失败          │ 可以恢复（推荐）       │ │
│  └──────────────────┴──────────────────┴─────────────────────┘ │
│                                                                 │
│  使用建议：                                                      │
│  - Checkpoint：用于自动故障恢复，Flink 自动管理                   │
│  - Savepoint：在以下场景手动创建：                                │
│    1. 修改代码/调整并行度前，先创建 Savepoint                     │
│    2. 从 Savepoint 恢复作业                                     │
│    3. 将作业从一个集群迁移到另一个集群                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Savepoint 常用命令：**

```bash
# 创建 Savepoint
flink savepoint <jobId> /opt/flink/savepoints

# 从 Savepoint 恢复作业
flink run -s /opt/flink/savepoints/savepoint-xxxxx -py demo-job.py

# 取消作业并创建 Savepoint
flink cancel -s /opt/flink/savepoints <jobId>
```

## 六、精确一次（Exactly-Once）语义

### 6.1 内部精确一次

Flink 通过 Checkpoint 机制保证了**内部**的精确一次：算子状态在故障恢复后可以回到一致的状态。

### 6.2 端到端精确一次

但只保证内部还不够，还需要保证从 Source 到 Sink 整条链路的精确一次：

```
┌─────────────────────────────────────────────────────────────────┐
│              端到端精确一次的三个保证                              │
│                                                                 │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐              │
│  │  Source   │ ──→ │  Flink   │ ──→ │   Sink   │              │
│  └──────────┘     └──────────┘     └──────────┘              │
│       │                 │                 │                    │
│       ▼                 ▼                 ▼                    │
│  1. 可重放          2. Checkpoint      3. 事务/幂等           │
│  Source 支持        保证内部状态        Sink 支持事务          │
│  重置偏移量         精确一次            或幂等写入              │
│                                                                 │
│  ─────────────────────────────────────────────────────         │
│                                                                 │
│  具体实现（以 Kafka → Flink → Kafka 为例）：                    │
│                                                                 │
│  1. Kafka Source：将消费偏移量保存到 Checkpoint 中               │
│     → 故障恢复时从 Checkpoint 中恢复偏移量                      │
│                                                                 │
│  2. Flink 内部：Checkpoint 保证算子状态的精确一次               │
│                                                                 │
│  3. Kafka Sink：通过两阶段提交（2PC）协议                       │
│     → 开始事务 → 写入数据 → Checkpoint 完成 → 提交事务          │
│     → 如果 Checkpoint 失败，事务会被回滚                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────┐
│               两阶段提交（2PC）流程                             │
│                                                             │
│  1. Checkpoint 协调器发起 Checkpoint                          │
│                                                             │
│  2. Source 保存偏移量到 Checkpoint 状态                       │
│                                                             │
│  3. 各算子保存自己的状态                                       │
│                                                             │
│  4. Sink 开始一个新的事务                                     │
│                                                             │
│  5. Sink 将数据写入事务（此时数据不可见）                      │
│                                                             │
│  6. Sink 将事务预提交（Pre-commit）                           │
│                                                             │
│  7. Checkpoint 协调器确认所有算子都已完成                      │
│                                                             │
│  8. Sink 正式提交事务（数据对外可见）                          │
│                                                             │
│  如果任何一步失败：                                           │
│  - Checkpoint 丢弃，所有事务回滚                               │
│  - 下一次 Checkpoint 重新开始                                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 七、数据分析项目：用状态统计每篇文章的实时浏览量

```python
# demo_view_counter.py
"""
使用状态管理统计每篇文章的实时浏览量
"""
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class ViewCountProcess(KeyedProcessFunction):
    """使用 ValueState 统计每篇文章的浏览量"""

    def __init__(self):
        self.count_state = None

    def open(self, parameters):
        # 初始化状态描述符
        descriptor = ValueStateDescriptor("view-count", int)
        self.count_state = self.get_runtime_context().get_state(descriptor)

    def process_element(self, value, ctx):
        # 获取当前计数
        current_count = self.count_state.value()
        if current_count is None:
            current_count = 0

        # 更新计数
        new_count = current_count + 1
        self.count_state.update(new_count)

        # 输出结果
        yield (value["postId"], new_count)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # 启用 Checkpoint
    env.enable_checkpointing(30000)

    # 创建 Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("blog-events") \
        .set_group_id("flink-view-counter") \
        .set_value_deserialization_schema(SimpleStringSchema()) \
        .build()

    # 从 Kafka 读取
    stream = env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name="Kafka-BlogEvents"
    )

    # 解析 JSON，过滤浏览事件，按 postId 分组，统计浏览量
    result = stream \
        .map(lambda v: json.loads(v)) \
        .filter(lambda e: e.get("type") == "view") \
        .key_by(lambda e: e["postId"]) \
        .process(ViewCountProcess())

    result.print("文章浏览量:")

    env.execute("文章实时访问量统计")


if __name__ == "__main__":
    main()
```

```
┌─────────────────────────────────────────────────────────────┐
│              状态管理的工作流程                                 │
│                                                             │
│  Kafka ──→ Source ──→ Map(JSON) ──→ Filter(view) ──→       │
│                                                        keyBy │
│                                                            ↓ │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              ViewCountProcess                        │   │
│  │                                                     │   │
│  │  open(): 初始化 ValueState                           │   │
│  │                                                     │   │
│  │  process_element():                                 │   │
│  │    1. 从状态中读取当前计数                             │   │
│  │    2. 计数 + 1                                       │   │
│  │    3. 将新计数写回状态                                │   │
│  │    4. 输出结果                                       │   │
│  │                                                     │   │
│  │  ┌─────────────────────────────────┐                │   │
│  │  │  postId=1: ValueState(5)       │ ← 每个Key独立   │   │
│  │  │  postId=2: ValueState(3)       │                │   │
│  │  │  postId=3: ValueState(8)       │                │   │
│  │  └─────────────────────────────────┘                │   │
│  │                                                     │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Checkpoint 定期保存这些状态到磁盘                            │
│  → 即使程序崩溃，恢复后状态不会丢失                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 八、面试题

### Q1：Checkpoint 失败的常见原因有哪些？如何排查？

**参考答案：**

常见原因：

1. **Checkpoint 超时**：某个算子处理太慢，导致 Barrier 对齐等待时间超过超时阈值。排查方式：查看 Web UI 的 Checkpoint 详情，看哪个 Task 耗时最长。

2. **状态太大**：状态数据量过大导致快照写入磁盘太慢。解决方案：使用增量 Checkpoint（RocksDB）、增大 Checkpoint 间隔。

3. **背压（Backpressure）**：下游处理速度跟不上上游，导致数据积压。排查方式：查看 Web UI 的 Backpressure 页面。

4. **Checkpoint 并发冲突**：多个 Checkpoint 同时进行，竞争资源。解决方案：增大 `min-pause` 间隔。

5. **存储问题**：Checkpoint 存储目录不可用或磁盘 IO 太慢。解决方案：检查存储配置和磁盘性能。

### Q2：Checkpoint 间隔设为多少合适？

**参考答案：**

Checkpoint 间隔需要在**故障恢复速度**和**正常处理性能**之间取平衡：

- **间隔太短**：Checkpoint 太频繁，占用大量 CPU 和网络带宽，影响正常数据处理
- **间隔太长**：故障恢复时需要重新处理大量数据

一般建议：
- **开发/测试**：10-30 秒
- **生产环境**：30 秒 - 5 分钟（根据业务可容忍的数据丢失量决定）
- **高可靠性场景**（如金融）：10-30 秒
- **普通场景**：1-5 分钟

经验公式：Checkpoint 间隔 = 可容忍的最大数据丢失时间 / 2

### Q3：状态后端从 HashMap 切换到 RocksDB 需要注意什么？

**参考答案：**

1. **性能差异**：RocksDB 有磁盘 IO，读写延迟比 HashMap 高（微秒级 vs 纳秒级），但可以支持更大的状态
2. **序列化**：RocksDB 需要序列化状态数据，状态类型必须支持序列化
3. **增量 Checkpoint**：RocksDB 支持增量 Checkpoint，只保存变化的部分，Checkpoint 速度更快
4. **本地磁盘**：需要确保 TaskManager 有足够的本地磁盘空间
5. **依赖**：使用 RocksDB 需要额外的 native 依赖库

## 九、总结

本篇我们深入学习了 Flink 的状态管理和 Checkpoint 机制，关键要点：

1. **状态**：让流处理拥有"记忆"，分为 Keyed State 和 Operator State
2. **状态后端**：HashMap（内存快、容量小）vs RocksDB（磁盘慢、容量大）
3. **Checkpoint**：基于 Chandy-Lamport 算法的分布式快照机制，用于故障恢复
4. **Savepoint**：手动创建的快照，用于版本升级和作业迁移
5. **精确一次**：通过 Checkpoint + 两阶段提交实现端到端精确一次

> **下一篇：[05-窗口与时间.md](05-窗口与时间.md) — 窗口与时间**
