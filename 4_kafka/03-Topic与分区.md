# 03 - Topic 与分区：Kafka 数据组织的核心

## 一、Topic 是什么？

上一节我们创建了 `blog-events` 和 `demo-logs` 两个 Topic。那么 Topic 到底是什么？

**Topic（主题）是消息的逻辑分类，是 Kafka 中最基本的组织单位。** 你可以把它理解为数据库中的"表"：

```
┌──────────────────────────────────────────────────────────┐
│                 Topic 与数据库表的类比                      │
│                                                          │
│  数据库                    Kafka                          │
│  ───────                  ─────                          │
│  Database        →        Kafka Cluster                  │
│  Table           →        Topic                          │
│  Row             →        Message（消息）                 │
│  Column          →        Message 中的字段                │
│  Index           →        Partition（分区）               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

在我们的数据分析项目中：

| Topic | 说明 | 消息示例 |
|-------|------|---------|
| `blog-events` | 数据平台核心事件 | 文章发布、评论、点赞 |
| `demo-logs` | 用户行为日志 | 页面浏览、搜索、点击 |
| `demo-notifications` | 通知消息 | 评论通知、系统通知 |

## 二、Partition 的作用

一个 Topic 被分成多个 **Partition（分区）**，这是 Kafka 实现高吞吐的关键设计。

### 2.1 为什么要分区？

```
┌─────────────────────────────────────────────────────────────────────┐
│                    分区的三大作用                                      │
│                                                                     │
│  1. 水平扩展（提升吞吐）                                              │
│                                                                     │
│     不分区（单分区）：                                                 │
│     ┌─────────────────────┐                                        │
│     │  Topic: blog-events  │  ← 所有消息挤在一个分区                  │
│     │  [msg1][msg2][...N] │  ← 一个消费者处理，速度有上限              │
│     └─────────────────────┘                                        │
│                                                                     │
│     分区后（3 分区）：                                                 │
│     ┌──────────────────────────────────────────┐                    │
│     │  Topic: blog-events                       │                    │
│     │  ┌─────────┐ ┌─────────┐ ┌─────────┐    │                    │
│     │  │Partition0│ │Partition1│ │Partition2│    │                    │
│     │  │[msg1][4] │ │[msg2][5] │ │[msg3][6] │    │                    │
│     │  └────┬─────┘ └────┬─────┘ └────┬─────┘    │                    │
│     │       │            │            │           │                    │
│     │  Consumer A   Consumer B   Consumer C      │                    │
│     └──────────────────────────────────────────┘                    │
│     ← 3 个消费者并行处理，吞吐量 x3                                    │
│                                                                     │
│  2. 并行处理（提升消费速度）                                            │
│     不同分区可以被不同消费者同时消费                                    │
│                                                                     │
│  3. 数据分布（提升存储容量）                                            │
│     不同分区可以存储在不同的 Broker 上                                  │
│     一个 Topic 的数据分散在多台机器上                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 分区内部结构

每个 Partition 在物理上就是一个**追加写入的日志文件**：

```
┌─────────────────────────────────────────────────────────────────────┐
│                Partition 内部结构（日志文件）                           │
│                                                                     │
│  Partition 0 的日志文件：                                             │
│                                                                     │
│  ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┐         │
│  │ Msg 0 │ Msg 1 │ Msg 2 │ Msg 3 │ Msg 4 │ Msg 5 │ Msg 6 │         │
│  │off=0  │off=1  │off=2  │off=3  │off=4  │off=5  │off=6  │         │
│  └───────┴───────┴───────┴───────┴───────┴───────┴───────┘         │
│  ↑                                                                   │
│  旧消息                                                          新消息│
│                                                                     │
│  特点：                                                              │
│  - 消息按 offset 严格递增排列（有序）                                  │
│  - 只能从尾部追加新消息（append-only）                                │
│  - 每条消息有唯一的 offset（从 0 开始）                                │
│  - 消费者通过 offset 追踪消费进度                                      │
│                                                                     │
│  物理文件：                                                          │
│  /opt/kafka/4.2.0/data/blog-events-0/                               │
│  ├── 00000000000000000000.log      ← 消息数据文件                    │
│  ├── 00000000000000000000.index    ← offset 索引文件                 │
│  ├── 00000000000000000000.timeindex ← 时间戳索引文件                  │
│  └── ...                                                            │
│                                                                     │
│  日志段（Log Segment）：                                               │
│  当日志文件达到一定大小（默认 1GB）时，会滚动创建新的日志段               │
│                                                                     │
│  [Segment 1] ──→ [Segment 2] ──→ [Segment 3] ──→ [活跃段]          │
│   0 ~ 1GB         1GB ~ 2GB      2GB ~ 3GB     写入中...             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 三、分区策略（生产者如何选择分区）

当生产者发送一条消息到 Topic 时，Kafka 需要决定这条消息发到哪个 Partition。这就是**分区策略**。

### 3.1 指定分区

```python
# 显式指定分区号
producer.send('blog-events', partition=0, value={'type': 'post_published'})
```

### 3.2 按 Key 分区（最常用）

当消息带有 Key 时，Kafka 对 Key 做 Hash，然后对分区数取模：

```
┌──────────────────────────────────────────────────────────┐
│                  Key Hash 分区策略                         │
│                                                          │
│  公式：partition = hash(key) % num_partitions             │
│                                                          │
│  示例（3 个分区）：                                        │
│                                                          │
│  Key = "post_1"  → hash = 58372 → 58372 % 3 = 1         │
│  Key = "post_1"  → hash = 58372 → 58372 % 3 = 1  (相同!) │
│  Key = "post_2"  → hash = 91827 → 91827 % 3 = 0         │
│  Key = "post_3"  → hash = 23456 → 23456 % 3 = 2         │
│                                                          │
│  作用：                                                    │
│  - 相同 Key 的消息总是进入同一个分区                        │
│  - 同一分区内消息有序                                       │
│  - 保证同一篇文章的事件有序（先发布、后评论、再点赞）          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

```python
# 用 postId 作为 Key，保证同一篇文章的事件有序
producer.send('blog-events',
    key=str(post_id).encode('utf-8'),   # Key = "1"
    value={'type': 'post_published', 'postId': 1}
)
producer.send('blog-events',
    key=str(post_id).encode('utf-8'),   # Key = "1"，同一 Key
    value={'type': 'comment_added', 'postId': 1}
)
# 这两条消息一定在同一个分区，且按发送顺序排列
```

### 3.3 无 Key 轮询（Round Robin）

当消息没有 Key 时，Kafka 使用**轮询（Round Robin）**策略，轮流发送到各个分区：

```
┌──────────────────────────────────────────────────────────┐
│                  轮询分区策略                               │
│                                                          │
│  消息序列：msg1, msg2, msg3, msg4, msg5, msg6             │
│                                                          │
│  Partition 0: [msg1] [msg4]                              │
│  Partition 1: [msg2] [msg5]                              │
│  Partition 2: [msg3] [msg6]                              │
│                                                          │
│  每条消息轮流分配到下一个分区，保证负载均衡                   │
│                                                          │
│  注意：无 Key 时，同一类消息可能分散到不同分区                │
│        不保证全局有序，只保证分区内有序                      │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 3.4 自定义分区器

你也可以实现自定义的分区逻辑：

```python
from kafka import KafkaProducer
from kafka.partitioner.default import DefaultPartitioner

class BlogEventPartitioner(DefaultPartitioner):
    """用户事件自定义分区器：按事件类型分区"""
    def __call__(self, key, all_partitions, available_partitions):
        if key:
            event_type = key.decode('utf-8')
            # 浏览事件 → 分区 0，评论事件 → 分区 1，其他 → 分区 2
            if event_type == 'view':
                return all_partitions[0]
            elif event_type == 'comment':
                return all_partitions[1]
            else:
                return all_partitions[2]
        return super().__call__(key, all_partitions, available_partitions)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    partitioner=BlogEventPartitioner()
)

# 按 Key 的值选择分区
producer.send('blog-events', key='view', value={'postId': 1})
```

### 3.5 分区策略总结

```
┌──────────────────────────────────────────────────────────┐
│                  分区策略决策树                             │
│                                                          │
│  生产者发送消息                                            │
│       │                                                  │
│       ▼                                                  │
│  指定了 partition 参数？                                   │
│    ├── 是 → 使用指定的分区                                 │
│    └── 否 → 有 Key？                                      │
│             ├── 是 → hash(Key) % 分区数                    │
│             └── 否 → Sticky Partitioner（粘性分区）         │
│                      （优先填满一个分区，再换下一个）        │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

## 四、分区数量选择

分区数不是越多越好，需要综合考虑：

```
┌─────────────────────────────────────────────────────────────────────┐
│                    分区数量选择建议                                     │
│                                                                     │
│  分区数量 = max(目标吞吐量 / 单分区吞吐量, 消费者数量)                 │
│                                                                     │
│  示例：                                                              │
│  - 目标吞吐量：100,000 条/秒                                         │
│  - 单分区吞吐量：10,000 条/秒（经验值）                                │
│  - 消费者数量：3                                                     │
│                                                                     │
│  分区数 = max(100000/10000, 3) = max(10, 3) = 10                    │
│                                                                     │
│  注意事项：                                                           │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ 分区太少的问题：                                            │     │
│  │ - 吞吐量不够，成为瓶颈                                       │     │
│  │ - 消费者组中的消费者闲置（分区数 < 消费者数时）                │     │
│  │                                                            │     │
│  │ 分区太多的问题：                                             │     │
│  │ - 文件句柄数增加（每个分区对应多个文件）                       │     │
│  │ - 内存占用增加（每个分区需要内存维护索引）                     │     │
│  │ - 故障恢复时间变长（Leader 切换时需要更多时间）                │     │
│  │ - 元数据管理压力大（Controller 压力）                         │     │
│  │                                                            │     │
│  │ 经验值：单个 Broker 的分区数建议不超过 2000                   │     │
│  │ 集群总分区数建议不超过 100,000                                │     │
│  └────────────────────────────────────────────────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 五、副本机制（Leader/Follower）

为了保证数据可靠性，每个 Partition 可以有多个副本。副本分为 **Leader** 和 **Follower**：

```
┌─────────────────────────────────────────────────────────────────────┐
│                    副本机制示意图（3 副本）                             │
│                                                                     │
│  Topic: blog-events, Partition 0, Replication Factor = 3            │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   Broker 1   │  │   Broker 2   │  │   Broker 3   │             │
│  │              │  │              │  │              │             │
│  │  P0-Leader   │  │  P0-Follower │  │  P0-Follower │             │
│  │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │             │
│  │  │msg0    │  │  │  │msg0    │  │  │  │msg0    │  │             │
│  │  │msg1    │  │  │  │msg1    │  │  │  │msg1    │  │             │
│  │  │msg2    │  │  │  │msg2    │  │  │  │msg2    │  │             │
│  │  └────────┘  │  │  └────────┘  │  │  └────────┘  │             │
│  │              │  │              │  │              │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
│         │                 │                 │                       │
│         │    ┌────────────┘                 │                       │
│         │    │  Follower 从 Leader 拉取数据  │                       │
│         │    └────────────┐                 │                       │
│         │                 │                 │                       │
│  生产者/消费者             │                 │                       │
│  只与 Leader 交互         │                 │                       │
│                                                                     │
│  关键规则：                                                          │
│  1. 生产者只向 Leader 写入消息                                        │
│  2. 消费者只从 Leader 读取消息                                        │
│  3. Follower 从 Leader 异步拉取数据进行同步                            │
│  4. Leader 挂了，从 Follower 中选举新的 Leader                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 六、ISR（In-Sync Replicas）

ISR 是一组与 Leader 保持同步的副本集合，是 Kafka 可靠性的核心概念：

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ISR 机制详解                                       │
│                                                                     │
│  所有副本：{Broker1, Broker2, Broker3, Broker4}                     │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │                     ISR（同步副本集）                       │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │      │
│  │  │ Broker1  │  │ Broker2  │  │ Broker3  │               │      │
│  │  │ Leader   │  │ Follower │  │ Follower │               │      │
│  │  │ offset=5 │  │ offset=5 │  │ offset=4 │  ← 落后 1 条   │      │
│  │  └──────────┘  └──────────┘  └──────────┘               │      │
│  └──────────────────────────────────────────────────────────┘      │
│                                                                     │
│  ┌──────────┐                                                       │
│  │ Broker4  │  ← 不在 ISR 中（严重落后，比如 offset=1）               │
│  │ Follower │                                                       │
│  │ offset=1 │                                                       │
│  └──────────┘                                                       │
│                                                                     │
│  ISR 动态变化规则：                                                   │
│  - Follower 拉取速度跟得上 → 加入 ISR                                │
│  - Follower 落后太多（超过 replica.lag.time.max.ms，默认 30s）        │
│    → 移出 ISR                                                        │
│  - Leader 挂了 → 只从 ISR 中选举新 Leader（保证数据不丢失）            │
│                                                                     │
│  查看命令：                                                          │
│  kafka-topics.sh --describe --topic blog-events \                    │
│    --bootstrap-server localhost:9092                                │
│  # Isr 字段就是当前 ISR 列表                                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 七、分区再分配

当集群扩容或缩容时，需要重新分配分区：

```bash
# 生成分配方案（json 格式）
kafka-reassign-partitions.sh \
  --generate \
  --topics-to-move-json-file topics.json \
  --broker-list "0,1,2"

# topics.json 内容：
# {
#   "topics": [{"topic": "blog-events"}, {"topic": "demo-logs"}],
#   "version": 1
# }

# 执行分配方案
kafka-reassign-partitions.sh \
  --execute \
  --reassignment-json-file assignment.json \
  --bootstrap-server localhost:9092

# 验证分配状态
kafka-reassign-partitions.sh \
  --verify \
  --reassignment-json-file assignment.json \
  --bootstrap-server localhost:9092
```

## 八、数据分析项目：设计用户事件 Topic 的分区策略

### 场景分析

| 事件类型 | 日均量级 | 是否需要有序 | 建议 |
|---------|---------|------------|------|
| 文章发布 | ~10 条 | 否（不同文章独立） | 轮询分区 |
| 文章浏览 | ~10,000 条 | 否 | 轮询分区 |
| 评论 | ~100 条 | 是（同一文章的评论有序） | 按 postId 分区 |
| 点赞 | ~500 条 | 否 | 轮询分区 |

### Topic 设计

```bash
# blog-events：核心事件，3 分区
# 用 postId 作为 Key，保证同一文章的事件有序
kafka-topics.sh --create \
  --topic blog-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# demo-logs：行为日志，6 分区（量级更大）
# 不需要 Key，轮询分配即可
kafka-topics.sh --create \
  --topic demo-logs \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1
```

### 分区策略代码

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 场景 1：用户事件 —— 用 postId 作为 Key
# 保证同一篇文章的事件（发布、评论、点赞）在同一个分区，有序处理
producer.send('blog-events',
    key=str(post_id).encode('utf-8'),
    value={'type': 'comment_added', 'postId': post_id, 'content': '写得好！'}
)

# 场景 2：行为日志 —— 不需要 Key，轮询分配
# 浏览量、搜索记录之间没有顺序关系
producer.send('demo-logs',
    value={'action': 'page_view', 'url': '/posts/1', 'userId': 100}
)

producer.flush()
```

## 九、面试题

### Q1：Kafka 如何保证消息顺序？

Kafka 只能保证**同一 Partition 内的消息有序**，不保证全局有序。

如果需要全局有序，有两种方案：
1. 只用 1 个分区（牺牲吞吐量，通常不推荐）
2. 把需要有序的消息通过相同的 Key 发送到同一分区（推荐）

在数据分析项目中，我们用 `postId` 作为 Key，保证同一篇文章的事件有序，同时不同文章的事件可以并行处理。

### Q2：分区数可以修改吗？增加和减少分别有什么影响？

- **可以增加分区**：`kafka-topics.sh --alter --partitions 6`，但无法减少
- **增加分区的影响**：
  - 已有消息不会重新分配到新分区
  - 新消息会按新的分区策略分配
  - 如果使用了 Key，可能导致相同 Key 的消息分布在不同分区（Hash 取模的基数变了）
- **减少分区不支持**：因为数据已经按分区存储，减少分区意味着需要合并数据，Kafka 没有实现这个功能

### Q3：ISR 和 AR 的区别？

- **AR（All Replicas）**：所有副本的集合，包括 Leader 和所有 Follower
- **ISR（In-Sync Replicas）**：与 Leader 保持同步的副本子集

当 Leader 挂了时，新 Leader 只从 ISR 中选举。如果 ISR 中只剩 Leader 自己（`min.insync.replicas=1`），那么 Leader 挂了就会导致数据丢失。

---

> **上一篇：[02-安装与配置.md](02-安装与配置.md) — 安装与配置**
>
> **下一篇：[04-生产者详解.md](04-生产者详解.md) — 生产者详解**
