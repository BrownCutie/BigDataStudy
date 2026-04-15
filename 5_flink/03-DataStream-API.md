# 03 - DataStream API 基础：用代码构建你的第一条数据流

## 一、Flink 程序的基本结构

在开始写代码之前，我们先理解 Flink 程序的骨架。不管多复杂的 Flink 作业，都可以归结为三步：

```
┌─────────────────────────────────────────────────────────────────┐
│                   Flink 程序的基本结构                            │
│                                                                 │
│  ┌──────────┐     ┌──────────────────┐     ┌──────────┐       │
│  │          │     │                  │     │          │       │
│  │  Source   │ ──→ │  Transformation  │ ──→ │   Sink   │       │
│  │ (数据源)  │     │   (数据转换)      │     │ (数据输出) │       │
│  │          │     │                  │     │          │       │
│  └──────────┘     └──────────────────┘     └──────────┘       │
│                                                                 │
│  Source：数据从哪来？                                            │
│    - Kafka、文件、集合、Socket...                                │
│                                                                 │
│  Transformation：对数据做什么处理？                               │
│    - 过滤、映射、聚合、分组...                                   │
│                                                                 │
│  Sink：数据输出到哪里？                                          │
│    - Kafka、文件、Elasticsearch、控制台...                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 二、执行环境（ExecutionEnvironment）

一切代码的起点是创建执行环境：

```python
from pyflink.datastream import StreamExecutionEnvironment

# 获取执行环境（最常用）
env = StreamExecutionEnvironment.get_execution_environment()

# 设置并行度
env.set_parallelism(4)

# 启用 Checkpoint（间隔 30 秒）
env.enable_checkpointing(30000)
```

```
┌─────────────────────────────────────────────────────────────┐
│                  执行环境类型                                   │
│                                                             │
│  StreamExecutionEnvironment                                  │
│  ├── get_execution_environment()    自动判断（推荐）          │
│  ├── create_local_environment()     本地模式（开发调试用）     │
│  └── create_remote_environment()    远程集群模式              │
│                                                             │
│  注意：Flink 是懒执行的（Lazy Evaluation）                    │
│  所有转换操作只是在构建执行计划，                              │
│  只有调用 env.execute() 时才会真正执行！                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 三、Source（数据源）

### 3.1 从集合读取

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

# 从 Python 列表创建数据流
stream = env.from_collection([1, 2, 3, 4, 5])

# 从元素创建数据流
stream = env.from_elements("hello", "flink", "world")
```

### 3.2 从 Kafka 读取（最重要）

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource, KafkaSourceBuilder
from pyflink.common.serialization import SimpleStringSchema

env = StreamExecutionEnvironment.get_execution_environment()

# 创建 Kafka Source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("blog-events") \
    .set_group_id("flink-demo-consumer") \
    .set_value_deserialization_schema(SimpleStringSchema()) \
    .build()

# 从 Kafka 创建数据流
stream = env.from_source(
    kafka_source,
    watermark_strategy=None,
    source_name="Kafka-BlogEvents"
)
```

### 3.3 从文件读取

```python
# 从文本文件读取（每行作为一条记录）
stream = env.read_text_file("/path/to/blog-events.log")

# 从 CSV 文件读取
stream = env.read_csv_file(
    path="/path/to/blog-events.csv",
    types=[str, int, str]  # type, postId, userId
)
```

## 四、常用算子（Transformation）

这是 Flink 最核心的部分，算子决定了你如何处理数据。

### 4.1 map — 一对一转换

```python
# map：每条输入数据 → 一条输出数据
# 类似于 Python 的 map() 函数

# 示例：将每个数字乘以 2
result = data_stream.map(lambda x: x * 2)

# 示例：提取用户事件中的 postId
result = demo_stream.map(lambda e: e["postId"])
```

```
┌─────────────────────────────────────────────────────────────┐
│                      map 算子                                 │
│                                                             │
│  输入：  [1, 2, 3, 4, 5]                                    │
│           │  │  │  │  │                                     │
│           ▼  ▼  ▼  ▼  ▼                                     │
│         (*2)(*2)(*2)(*2)(*2)                                │
│           │  │  │  │  │                                     │
│  输出：  [2, 4, 6, 8, 10]                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 filter — 过滤

```python
# filter：保留满足条件的数据，丢弃不满足条件的

# 示例：只保留浏览事件
view_events = demo_stream.filter(lambda e: e["type"] == "view")

# 示例：只保留浏览量大于 100 的文章
popular = article_views.filter(lambda x: x[1] > 100)
```

```
┌─────────────────────────────────────────────────────────────┐
│                     filter 算子                               │
│                                                             │
│  输入：  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                   │
│           │  │  │  │  │  │  │  │  │  │                     │
│           ✓  ✗  ✓  ✗  ✓  ✗  ✓  ✗  ✓  ✗    条件：偶数？      │
│           │           │           │           │              │
│  输出：  [2,          4,          6,          8, 10]         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 flatMap — 一对多转换

```python
# flatMap：一条输入 → 零条或多条输出
# 适合拆分一条数据为多条的场景

# 示例：将一句话拆分成单词
sentences = env.from_collection(["hello flink", "stream computing"])

words = sentences.flat_map(
    lambda s: s.split()  # 每个句子拆分成多个单词
)
# 输出：["hello", "flink", "stream", "computing"]

# 示例：从数据平台内容中提取所有标签
tags = demo_stream.flat_map(
    lambda e: e["tags"].split(",") if e["tags"] else []
)
```

```
┌─────────────────────────────────────────────────────────────┐
│                    flatMap 算子                               │
│                                                             │
│  输入：  ["hello flink", "stream computing"]                │
│           │                    │                            │
│           ▼                    ▼                            │
│     ["hello", "flink"]   ["stream", "computing"]           │
│           │                    │                            │
│           └────────┬───────────┘                            │
│                    │                                        │
│  输出：  ["hello", "flink", "stream", "computing"]         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.4 keyBy — 分组

```python
# keyBy：按指定的 Key 将数据流分组
# 分组后，相同 Key 的数据会被发送到同一个 Task 处理
# 这是状态管理和窗口计算的前提！

# 示例：按 postId 分组
keyed_stream = demo_stream.key_by(lambda e: e["postId"])

# 示例：按事件类型分组
typed_stream = demo_stream.key_by(lambda e: e["type"])
```

```
┌─────────────────────────────────────────────────────────────────┐
│                      keyBy 分组示意                              │
│                                                                 │
│  输入数据流（未分组）：                                           │
│  ──●(postId=1)──●(postId=2)──●(postId=1)──●(postId=3)──→     │
│                                                                 │
│  keyBy(lambda e: e["postId"]) 之后：                            │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │ Key = 1     │  │ Key = 2     │  │ Key = 3     │            │
│  │             │  │             │  │             │            │
│  │ ●(postId=1) │  │ ●(postId=2) │  │ ●(postId=3) │            │
│  │ ●(postId=1) │  │             │  │             │            │
│  └─────────────┘  └─────────────┘  └─────────────┘            │
│                                                                 │
│  相同 postId 的数据会被路由到同一个分区                           │
│  这样后续的 reduce、sum 等聚合操作才有意义                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.5 reduce — 聚合

```python
# reduce：对 KeyedStream 中的数据进行增量聚合
# 每次用新的值和当前聚合结果合并

# 示例：计算每篇文章的最大浏览时长
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

events = env.from_collection([
    {"postId": 1, "duration": 30},
    {"postId": 2, "duration": 45},
    {"postId": 1, "duration": 60},
    {"postId": 2, "duration": 20},
])

# 按 postId 分组，求最大 duration
max_duration = events \
    .key_by(lambda e: e["postId"]) \
    .reduce(lambda a, b: {"postId": a["postId"], "duration": max(a["duration"], b["duration"])})

max_duration.print()
env.execute("最大浏览时长")
```

```
┌─────────────────────────────────────────────────────────────┐
│                    reduce 聚合示意                            │
│                                                             │
│  Key = 1 的数据：                                            │
│  ──●(30)──●(60)──→                                         │
│     │       │                                              │
│     ▼       ▼                                              │
│   max(30, 60) = 60                                         │
│                                                             │
│  Key = 2 的数据：                                            │
│  ──●(45)──●(20)──→                                         │
│     │       │                                              │
│     ▼       ▼                                              │
│   max(45, 20) = 45                                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.6 union — 合并流

```python
# union：将多个 DataStream 合并成一个
# 注意：union 要求所有流的类型必须一致

view_stream = demo_stream.filter(lambda e: e["type"] == "view")
comment_stream = demo_stream.filter(lambda e: e["type"] == "comment")
like_stream = demo_stream.filter(lambda e: e["type"] == "like")

# 合并三种事件流
all_events = view_stream.union(comment_stream, like_stream)
```

```
┌─────────────────────────────────────────────────────────────┐
│                    union 合并流                               │
│                                                             │
│  ┌───────────┐                                              │
│  │ view 流    │ ──●──●──●──→                                │
│  └─────┬─────┘                                              │
│        │                                                    │
│  ┌─────┼─────┐                                              │
│  │comment 流 │ ──●──●──→                                    │
│  └─────┬─────┘                                              │
│        │                                                    │
│  ┌─────┼─────┐                                              │
│  │ like 流   │ ──●──●──●──●──→                             │
│  └─────┬─────┘                                              │
│        │                                                    │
│        ▼                                                    │
│  ──●──●──●──●──●──●──●──●──●──●──→                        │
│  (所有数据混合在一起)                                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 五、常用算子速查表

| 算子 | 输入 → 输出 | 说明 | 示例场景 |
|------|------------|------|---------|
| **map** | 1 → 1 | 对每条数据做转换 | 提取字段、类型转换 |
| **filter** | 1 → 0 或 1 | 过滤数据 | 过滤掉无效事件 |
| **flatMap** | 1 → 0~N | 拆分/展开数据 | 分词、提取标签 |
| **keyBy** | 分组 | 按 Key 分区 | 按文章 ID 分组 |
| **reduce** | 聚合 | 增量聚合 | 累加、求最大值 |
| **sum/min/max** | 聚合 | 简单聚合 | 统计浏览量 |
| **union** | 合并 | 合并多个流 | 合并多种事件 |
| **split** | 分流 | 按条件拆分流 | 按事件类型分流 |

## 六、自定义函数：Lambda vs 匿名类

在 Flink 中，你可以用 Lambda 表达式或自定义类来定义处理逻辑。

### 6.1 Lambda 表达式（简单场景）

```python
# 适合简单的转换逻辑
stream.map(lambda x: x * 2)
stream.filter(lambda x: x > 0)
stream.key_by(lambda x: x["id"])
```

### 6.2 自定义类（复杂场景）

```python
from pyflink.datastream.functions import MapFunction, FilterFunction

# 自定义 MapFunction
class ParseBlogEvent(MapFunction):
    def map(self, value):
        import json
        data = json.loads(value)
        return {
            "type": data["type"],
            "postId": data["postId"],
            "userId": data["userId"],
            "timestamp": data["timestamp"]
        }

# 自定义 FilterFunction
class ViewEventFilter(FilterFunction):
    def filter(self, value):
        return value["type"] == "view"

# 使用
parsed_stream = raw_stream.map(ParseBlogEvent())
view_stream = parsed_stream.filter(ViewEventFilter())
```

```
┌─────────────────────────────────────────────────────────────┐
│                Lambda vs 自定义类 选择建议                     │
│                                                             │
│  Lambda 表达式：                                              │
│  ✓ 简单、简洁                                                │
│  ✓ 适合一行代码能搞定的逻辑                                   │
│  ✗ 逻辑复杂时可读性差                                         │
│  ✗ 调试不方便                                               │
│                                                             │
│  自定义类：                                                   │
│  ✓ 逻辑清晰，可读性好                                        │
│  ✓ 可以添加初始化逻辑（open 方法）                            │
│  ✓ 可以使用富函数（RichFunction）获取运行时上下文              │
│  ✓ 方便单元测试                                              │
│  ✗ 代码量多一些                                              │
│                                                             │
│  建议：简单逻辑用 Lambda，复杂逻辑用自定义类                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 七、Sink（数据输出）

### 7.1 打印到控制台

```python
# 最简单的 Sink，用于开发调试
result.print()

# 带前缀的打印
result.print("demo-stats")
```

### 7.2 写入 Kafka

```python
from pyflink.datastream.connectors import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
import json

kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("processed-events")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()

# 将结果写入 Kafka
result.map(lambda x: json.dumps(x)).sink_to(kafka_sink)
```

### 7.3 写入文件

```python
# 写入文本文件
result.write_as_text("/tmp/demo-stats-output")

# 写入 CSV 文件
result.write_as_csv("/tmp/demo-stats-csv")
```

## 八、实战：从 Kafka 读取用户事件并做简单统计

现在让我们把前面学的知识串起来，完成一个完整的实战案例。

### 8.1 准备 Kafka Topic

```bash
# 创建用户事件 Topic
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic blog-events \
  --partitions 3 --replication-factor 1

# 发送一些测试数据
kafka-console-producer --bootstrap-server localhost:9092 --topic blog-events
```

在 producer 控制台中输入以下测试数据（JSON 格式）：

```json
{"type":"view","postId":1,"userId":"alice","timestamp":1713200000000}
{"type":"view","postId":2,"userId":"bob","timestamp":1713200001000}
{"type":"comment","postId":1,"userId":"charlie","timestamp":1713200002000}
{"type":"view","postId":1,"userId":"david","timestamp":1713200003000}
{"type":"like","postId":2,"userId":"alice","timestamp":1713200004000}
{"type":"view","postId":3,"userId":"eve","timestamp":1713200005000}
```

### 8.2 编写 Flink 作业

```python
# demo_event_stats.py
"""
用户事件实时统计
- 从 Kafka 读取用户事件
- 按事件类型统计数量
- 按文章统计浏览量
"""
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.serialization import SimpleStringSchema


class ParseEvent:
    """解析 JSON 格式的用户事件"""
    def map(self, value):
        return json.loads(value)


def main():
    # 1. 创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. 创建 Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("blog-events") \
        .set_group_id("flink-demo-stats") \
        .set_value_deserialization_schema(SimpleStringSchema()) \
        .build()

    # 3. 从 Kafka 读取数据
    stream = env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name="Kafka-BlogEvents"
    )

    # 4. 解析 JSON
    events = stream.map(ParseEvent(), output_type=json.loads('{}'))

    # 5. 统计一：按事件类型统计
    type_counts = events \
        .map(lambda e: (e.get("type", "unknown"), 1)) \
        .key_by(lambda x: x[0]) \
        .sum(1)

    type_counts.print("事件类型统计:")

    # 6. 统计二：按文章统计浏览量
    post_views = events \
        .filter(lambda e: e.get("type") == "view") \
        .map(lambda e: (e.get("postId", 0), 1)) \
        .key_by(lambda x: x[0]) \
        .sum(1)

    post_views.print("文章浏览量:")

    # 7. 执行作业
    env.execute("用户事件实时统计")


if __name__ == "__main__":
    main()
```

### 8.3 提交并运行

```bash
# 提交作业
flink run -py demo_event_stats.py

# 在 Web UI (http://localhost:8081) 中观察作业运行状态
# 在终端中查看输出结果

# 预期输出类似：
# 事件类型统计:> (view, 4)
# 文章浏览量:> (1, 3)
# 事件类型统计:> (comment, 1)
# 文章浏览量:> (2, 1)
```

### 8.4 作业执行流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                 用户事件统计作业执行流程                           │
│                                                                 │
│  Kafka                                                            │
│  blog-events                                                     │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────┐     ┌──────────┐     ┌──────────────────┐        │
│  │  Source   │ ──→ │  Map     │ ──→ │    分支           │        │
│  │ (Kafka)  │     │ (解析JSON) │     │                  │        │
│  └──────────┘     └──────────┘     └────────┬─────────┘        │
│                                             │                   │
│                           ┌─────────────────┼──────────┐      │
│                           │                 │          │      │
│                           ▼                 │          │      │
│                    ┌──────────────┐         │          │      │
│                    │  Map(类型,1)  │         │          │      │
│                    └──────┬───────┘         │          │      │
│                           │                 │          │      │
│                           ▼                 │          │      │
│                    ┌──────────────┐         │          │      │
│                    │   keyBy      │         │          │      │
│                    └──────┬───────┘         │          │      │
│                           │                 │          │      │
│                           ▼                 │          │      │
│                    ┌──────────────┐         │          │      │
│                    │    sum(1)    │         │          │      │
│                    └──────┬───────┘         │          │      │
│                           │                 │          │      │
│                           ▼                 ▼          ▼      │
│                    ┌──────────────┐  ┌──────────┐  ┌──────┐  │
│                    │   Print      │  │  Filter   │  │ ...  │  │
│                    │(事件类型统计) │  │(只保留view)│  │      │  │
│                    └──────────────┘  └──────────┘  └──────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────────┘
```

## 九、面试题

### Q1：Flink 的惰性求值（Lazy Evaluation）是什么意思？

**参考答案：**

Flink 程序采用惰性求值机制。当你调用 `map()`、`filter()`、`keyBy()` 等转换操作时，Flink 并不会立即执行这些操作，而是将它们构建成一个执行计划（Execution Plan / DAG）。只有当调用 `env.execute()` 时，Flink 才会将这个执行计划提交给 JobManager 进行优化和执行。

这种设计的好处：
1. **优化机会**：Flink 可以看到整个执行计划，进行算子链（Operator Chain）优化，将多个算子合并成一个 Task 减少数据传输开销
2. **错误提前发现**：在执行前就能发现类型不匹配等问题

### Q2：keyBy 和 groupBy 有什么区别？

**参考答案：**

在 Flink 中，`keyBy` 用于 DataStream API，`groupBy` 用于 Table API / SQL。两者的核心思想相同——都是按 Key 分组，但实现层面不同：

- `keyBy` 返回的是 `KeyedStream`，后续可以使用 `reduce()`、`sum()` 等聚合操作和窗口操作
- `groupBy` 是声明式的，Flink 会自动选择最优的执行计划
- `keyBy` 的底层实现是哈希分区（Hash Partitioning），相同 Key 的数据会被发送到同一个 Task

### Q3：DataStream API 和 Table API / SQL 应该怎么选择？

**参考答案：**

| 场景 | 推荐 API | 原因 |
|------|---------|------|
| 简单的 ETL | DataStream API | 灵活、直观 |
| 需要精细控制状态 | DataStream API | 可以直接操作 State |
| 复杂的聚合分析 | Table API / SQL | 声明式，代码简洁 |
| 不熟悉 Flink | Table API / SQL | SQL 门槛低 |
| 需要自定义窗口逻辑 | DataStream API | 更灵活 |
| 日报/周报类聚合 | Table API / SQL | 内置丰富的窗口函数 |

实际项目中，两者经常混合使用：用 DataStream API 做数据清洗，用 SQL 做聚合分析。

## 十、总结

本篇我们学习了 DataStream API 的基础知识，关键要点：

1. **三步结构**：Source → Transformation → Sink
2. **核心算子**：map（转换）、filter（过滤）、flatMap（拆分）、keyBy（分组）、reduce（聚合）
3. **数据源**：集合、Kafka、文件等
4. **函数选择**：简单用 Lambda，复杂用自定义类
5. **懒执行**：所有转换操作构建 DAG，`env.execute()` 才真正执行

> **下一篇：[04-状态管理与Checkpoint.md](04-状态管理与Checkpoint.md) — 状态管理与 Checkpoint**
