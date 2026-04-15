# RDD 编程模型

## 1. RDD 是什么

### 1.1 RDD 定义

RDD（Resilient Distributed Dataset，弹性分布式数据集）是 Spark 最底层的数据抽象。它是一个不可变、分区的、容错的分布式元素集合。

```
RDD 的五大特性：

┌──────────────────────────────────────────────────────────┐
│                    RDD 特性                               │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. 包含一系列分区（Partition）                           │
│     数据被切分为多个分区，分布在不同节点上                   │
│                                                          │
│  2. 每个分区有一个计算函数                                │
│     Spark 知道如何对每个分区的数据执行计算                  │
│                                                          │
│  3. 依赖其他 RDD（血缘关系）                              │
│     每个 RDD 记录了它是如何从父 RDD 转换而来               │
│                                                          │
│  4. (可选) KV RDD 有分区器（Partitioner）                 │
│     用于控制数据如何分布到各个分区                         │
│                                                          │
│  5. (可选) 为每个分区提供首选位置（Preferred Locations）    │
│     用于数据本地性优化（将计算分配到数据所在节点）          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 1.2 RDD vs DataFrame

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  DataFrame (推荐大多数场景)                                    │
│  ┌──────────────────────────────────────────────────┐       │
│  │  +--------+--------+--------+-----+              │       │
│  │  | title  | author | views  | ... |              │       │
│  │  +--------+--------+--------+-----+              │       │
│  │  | Spark  | 张三   | 1500   |     |              │       │
│  │  | Hadoop | 李四   | 2300   |     │              │       │
│  │  +--------+--------+--------+-----+              │       │
│  │                                                  │       │
│  │  优点：有 Schema，Catalyst 优化，API 友好         │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
│  RDD (底层 API)                                              │
│  ┌──────────────────────────────────────────────────┐       │
│  │  ["Spark,张三,1500", "Hadoop,李四,2300", ...]    │       │
│  │                                                  │       │
│  │  优点：底层控制，非结构化数据，自定义分区           │       │
│  │  缺点：没有优化器，API 不够友好                    │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
│  选择建议：                                                   │
│  - 结构化/半结构化数据 -> DataFrame（首选）                   │
│  - 非结构化数据（如文本、图片） -> RDD                         │
│  - 需要底层分区控制 -> RDD                                    │
│  - 使用 DataFrame 不支持的函数 -> rdd.map() 降级              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 1.3 什么时候需要用 RDD

1. **处理非结构化数据**：纯文本、二进制文件等
2. **底层分区控制**：需要自定义分区策略
3. **使用 DataFrame 不支持的 API**：某些底层操作
4. **与遗留系统集成**：旧代码使用 RDD API
5. **需要细粒度控制**：如 mapPartitions 需要对每个分区做批处理

## 2. 创建 RDD

### 2.1 parallelize — 从集合创建

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDCreate")

# 从 list 创建 RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 指定分区数
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=4)

# 从 list of tuples 创建
demo_data = [
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
    ("Hive数据仓库", "王五", 800),
]
rdd = sc.parallelize(demo_data)

# 查看分区数
print(f"分区数: {rdd.getNumPartitions()}")

# 查看每个分区的数据
def print_partition(index, iterator):
    for item in iterator:
        print(f"分区 {index}: {item}")
    yield None

rdd.foreachPartition(lambda it: list(print_partition(0, it)))
```

### 2.2 textFile — 从文本文件创建

```python
# 从本地文件创建
rdd = sc.textFile("file:///path/to/demos.txt")

# 从 HDFS 创建
rdd = sc.textFile("hdfs:///user/data/demos.txt")

# 从目录创建（读取目录下所有文件）
rdd = sc.textFile("hdfs:///user/data/demos/")

# 指定最小分区数
rdd = sc.textFile("hdfs:///user/data/demos.txt", minPartitions=10)

# wholeTextFiles：读取整个文件为一个记录（文件名, 内容）
rdd = sc.wholeTextFiles("hdfs:///user/data/demos/")
```

### 2.3 从现有集合创建

```python
# 从 range 创建
rdd = sc.range(0, 100)         # 0 到 99
rdd = sc.range(0, 100, step=2)  # 0, 2, 4, ..., 98

# 从 DataFrame 转换
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("a", 1), ("b", 2)], ["col1", "col2"])
rdd = df.rdd

# RDD 的元素类型是 Row
rdd.map(lambda row: (row.col1, row.col2)).collect()
```

## 3. RDD Transformations

### 3.1 map

```python
# map：对每个元素应用函数，一对一转换
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 每个元素乘以 2
doubled = rdd.map(lambda x: x * 2)
print(doubled.collect())  # [2, 4, 6, 8, 10]

# 转换为字符串
str_rdd = rdd.map(lambda x: f"number_{x}")
print(str_rdd.collect())  # ['number_1', 'number_2', ...]

# 处理业务数据
demo_rdd = sc.parallelize([
    "Spark入门|张三|1500",
    "Hadoop教程|李四|2300",
    "Hive数据仓库|王五|800",
])

# 解析每行数据
parsed = demo_rdd.map(lambda line: {
    "title": line.split("|")[0],
    "author": line.split("|")[1],
    "views": int(line.split("|")[2])
})
print(parsed.collect())
```

```
map 执行过程：

输入 RDD（3 个分区）：
┌──────────┐  ┌──────────┐  ┌──────────┐
│ [1, 2]   │  │ [3, 4]   │  │ [5]      │
│ 分区 0   │  │ 分区 1   │  │ 分区 2   │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     v             v             v
  map(x*2)      map(x*2)      map(x*2)
     │             │             │
     v             v             v
┌──────────┐  ┌──────────┐  ┌──────────┐
│ [2, 4]   │  │ [6, 8]   │  │ [10]     │
│ 分区 0   │  │ 分区 1   │  │ 分区 2   │
└──────────┘  └──────────┘  └──────────┘

注意：map 是窄依赖，数据不需要跨分区移动
```

### 3.2 flatMap

```python
# flatMap：对每个元素应用函数，结果展平（一对多转换）
rdd = sc.parallelize(["hello world", "spark rdd", "big data"])

# 按空格分词并展平
words = rdd.flatMap(lambda line: line.split(" "))
print(words.collect())
# ['hello', 'world', 'spark', 'rdd', 'big', 'data']

# 处理数据平台标签
tags_rdd = sc.parallelize([
    ("Spark入门", "spark,大数据,hadoop"),
    ("Hadoop教程", "hadoop,大数据"),
    ("Flink入门", "flink,流计算"),
])

# 展开标签
tag_pairs = tags_rdd.flatMap(lambda x: [(tag.strip(), x[0]) for tag in x[1].split(",")])
print(tag_pairs.collect())
# [('spark', 'Spark入门'), ('大数据', 'Spark入门'), ('hadoop', 'Spark入门'), ...]
```

```
flatMap 执行过程：

输入：
  ["hello world", "spark rdd", "big data"]

flatMap(line -> line.split(" ")):
  "hello world" -> ["hello", "world"]  ──┐
  "spark rdd"   -> ["spark", "rdd"]    ──┼──> 展平
  "big data"    -> ["big", "data"]     ──┘

输出：
  ["hello", "world", "spark", "rdd", "big", "data"]
```

### 3.3 filter

```python
# filter：过滤元素
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# 保留偶数
evens = rdd.filter(lambda x: x % 2 == 0)
print(evens.collect())  # [2, 4, 6, 8, 10]

# 保留大于 5 的元素
big = rdd.filter(lambda x: x > 5)
print(big.collect())  # [6, 7, 8, 9, 10]

# 过滤业务数据
demo_rdd = sc.parallelize([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
    ("Hive数据仓库", "王五", 800),
])

# 浏览量大于 1000 的文章
popular = demo_rdd.filter(lambda x: x[2] > 1000)
print(popular.collect())  # [('Spark入门', '张三', 1500), ('Hadoop教程', '李四', 2300)]
```

### 3.4 mapPartitions

```python
# mapPartitions：对整个分区进行操作（而非单个元素）
# 优点：可以复用分区内的资源（如数据库连接、模型对象等）

# 示例：为每个分区创建一个数据库连接，批量处理数据
def process_partition(iterator):
    # 在分区级别创建资源（每个分区只创建一次）
    results = []
    for item in iterator:
        # 处理每个元素
        results.append(item * 10)
    return iter(results)

rdd = sc.parallelize(range(100), numSlices=4)
result = rdd.mapPartitions(process_partition)
print(result.take(10))

# 实际应用：读取分区数据并批量写入数据库
def batch_insert(iterator):
    import pymysql  # 假设使用 pymysql
    # connection = pymysql.connect(...)  # 每个分区一个连接
    batch = []
    for row in iterator:
        batch.append(row)
        if len(batch) >= 100:
            # connection.executemany("INSERT INTO ...", batch)
            batch = []
    # if batch:
    #     connection.executemany("INSERT INTO ...", batch)
    # connection.close()
    return iter([])

# 示例：计算每个分区的平均值
def partition_avg(iterator):
    values = list(iterator)
    if values:
        avg = sum(values) / len(values)
        yield (avg, len(values))
    else:
        yield (0, 0)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], numSlices=3)
rdd.mapPartitions(partition_avg).collect()
# [(2.0, 3), (5.0, 3), (8.5, 4)]
```

### 3.5 mapPartitionsWithIndex

```python
# 带分区索引的 mapPartitions
def process_with_index(index, iterator):
    values = list(iterator)
    yield (index, len(values), sum(values))

rdd = sc.parallelize(range(10), numSlices=3)
rdd.mapPartitionsWithIndex(process_with_index).collect()
# [(0, 4, 6), (1, 3, 12), (2, 3, 21)]  -- (分区号, 元素数, 元素和)
```

### 3.6 sortByKey

```python
# sortByKey：按 Key 排序（KV RDD 专用）
kv_rdd = sc.parallelize([
    ("banana", 3),
    ("apple", 5),
    ("cherry", 1),
    ("date", 2),
    ("elderberry", 4),
])

# 升序排序
asc_sorted = kv_rdd.sortByKey()
print(asc_sorted.collect())
# [('apple', 5), ('banana', 3), ('cherry', 1), ('date', 2), ('elderberry', 4)]

# 降序排序
desc_sorted = kv_rdd.sortByKey(ascending=False)
print(desc_sorted.collect())
# [('elderberry', 4), ('date', 2), ('cherry', 1), ('banana', 3), ('apple', 5)]

# 按浏览量排序博客文章
demo_rdd = sc.parallelize([
    ("Spark入门", 1500),
    ("Hadoop教程", 2300),
    ("Hive数据仓库", 800),
])

sorted_demos = demo_rdd.sortBy(lambda x: x[1], ascending=False)
print(sorted_demos.collect())
# [('Hadoop教程', 2300), ('Spark入门', 1500), ('Hive数据仓库', 800)]
```

### 3.7 join / leftOuterJoin / rightOuterJoin

```python
# KV RDD 的 Join 操作
articles = sc.parallelize([
    (1, "Spark入门"),
    (2, "Hadoop教程"),
    (3, "Hive数据仓库"),
    (4, "Flink入门"),
])

authors = sc.parallelize([
    (1, "张三"),
    (2, "李四"),
    (4, "王五"),
    (5, "赵六"),  # 没有对应文章
])

# Inner Join
inner = articles.join(authors)
print(inner.collect())
# [(1, ('Spark入门', '张三')), (2, ('Hadoop教程', '李四')), (4, ('Flink入门', '王五'))]

# Left Outer Join
left = articles.leftOuterJoin(authors)
print(left.collect())
# [(1, ('Spark入门', '张三')), (2, ('Hadoop教程', '李四')),
#  (3, ('Hive数据仓库', None)), (4, ('Flink入门', '王五'))]

# Right Outer Join
right = articles.rightOuterJoin(authors)
print(right.collect())
# [(1, ('Spark入门', '张三')), (2, ('Hadoop教程', '李四')),
#  (4, ('Flink入门', '王五')), (5, (None, '赵六'))]

# Full Outer Join
full = articles.fullOuterJoin(authors)
print(full.collect())
```

### 3.8 union

```python
# union：合并两个 RDD（不去重）
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([3, 4, 5])

merged = rdd1.union(rdd2)
print(merged.collect())  # [1, 2, 3, 3, 4, 5]

# 去重
distinct = rdd1.union(rdd2).distinct()
print(distinct.collect())  # [1, 2, 3, 4, 5]
```

### 3.9 coalesce / repartition

```python
rdd = sc.parallelize(range(100), numSlices=10)

# coalesce：减少分区数（不触发 Shuffle，性能好）
rdd_small = rdd.coalesce(3)
print(f"原始分区数: {rdd.getNumPartitions()}")    # 10
print(f"coalesce 后: {rdd_small.getNumPartitions()}")  # 3

# repartition：增加或减少分区数（触发 Shuffle）
rdd_large = rdd.repartition(20)
print(f"repartition 后: {rdd_large.getNumPartitions()}")  # 20

# 选择建议：
# - 减少分区：用 coalesce（不 Shuffle）
# - 增加分区：用 repartition（必须 Shuffle）
```

```
coalesce vs repartition：

coalesce（减少分区，无 Shuffle）：
  ┌───┐ ┌───┐ ┌───┐ ┌───┐
  │P0 │ │P1 │ │P2 │ │P3 │     ┌───────┐
  └───┘ └───┘ └───┘ └───┘ ──> │ P0+P1 │
  ┌───┐ ┌───┐ ┌───┐ ┌───┐     └───────┘
  │P4 │ │P5 │ │P6 │ │P7 │     ┌───────┐
  └───┘ └───┘ └───┘ └───┘ ──> │ P2+P3 │
  ┌───┐ ┌───┐                   └───────┘
  │P8 │ │P9 │
  └───┘ └───┘

repartition（改变分区，有 Shuffle）：
  ┌───┐ ┌───┐ ┌───┐     ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐
  │P0 │ │P1 │ │P2 │     │P0 │ │P1 │ │P2 │ │P3 │ │P4 │
  └─┬─┘ └─┬─┘ └─┬─┘     └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘
    └───┬───┘   │           │     │     │     │     │
        v       v           v     v     v     v     v
    ┌──────────────┐     Shuffle + 重分区
    │  Shuffle     │
    └──────────────┘
```

## 4. RDD Actions

### 4.1 collect

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 收集所有数据到 Driver（慎用！数据量大时会 OOM）
result = rdd.collect()
print(result)  # [1, 2, 3, 4, 5]
```

### 4.2 count

```python
# 元素总数
print(rdd.count())  # 5

# 近似计数（大数据量时更快）
print(rdd.countApprox(1000))  # 超时时间 1000ms
```

### 4.3 reduce

```python
# reduce：聚合所有元素
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 求和
total = rdd.reduce(lambda a, b: a + b)
print(total)  # 15

# 求最大值
max_val = rdd.reduce(lambda a, b: a if a > b else b)
print(max_val)  # 5

# 求最小值
min_val = rdd.reduce(lambda a, b: a if a < b else b)
print(min_val)  # 1

# 字符串拼接
words = sc.parallelize(["Spark", "is", "awesome"])
result = words.reduce(lambda a, b: a + " " + b)
print(result)  # "Spark is awesome"
```

```
reduce 执行过程（reduce(lambda a, b: a + b)）：

  [1, 2, 3, 4, 5]
      │
      v
  分区内 reduce：
  分区0: [1,2] -> 3
  分区1: [3,4,5] -> 12
      │
      v
  分区间 reduce：
  3 + 12 = 15
```

### 4.4 first / take / takeOrdered / top

```python
rdd = sc.parallelize([5, 3, 1, 4, 2])

# first：第一个元素
print(rdd.first())  # 5

# take(n)：前 n 个元素
print(rdd.take(3))  # [5, 3, 1]

# takeOrdered(n)：最小的 n 个元素（升序）
print(rdd.takeOrdered(3))  # [1, 2, 3]

# takeOrdered(n, key)：按自定义排序取最小的 n 个
demo_rdd = sc.parallelize([
    ("Spark入门", 1500),
    ("Hadoop教程", 2300),
    ("Hive数据仓库", 800),
])
print(demo_rdd.takeOrdered(2, key=lambda x: x[1]))  # [('Hive数据仓库', 800), ('Spark入门', 1500)]

# top(n)：最大的 n 个元素（降序）
print(rdd.top(3))  # [5, 4, 3]
```

### 4.5 foreach

```python
# foreach：对每个元素执行操作（在 Executor 端执行）
# 不会将数据返回给 Driver
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 打印每个元素（输出在 Executor 的日志中，不在 Driver 端）
rdd.foreach(lambda x: print(f"Processing: {x}"))

# 写入外部系统
def write_to_file(item):
    with open("/tmp/rdd_output.txt", "a") as f:
        f.write(f"{item}\n")

# rdd.foreach(write_to_file)

# foreachPartition：对每个分区执行（更高效，可以批量操作）
def write_batch(iterator):
    items = list(iterator)
    # 批量写入数据库/文件
    # batch_insert(items)
    for item in items:
        print(f"Batch item: {item}")

rdd.foreachPartition(write_batch)
```

### 4.6 saveAsTextFile

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

# 保存为文本文件
rdd.saveAsTextFile("output/rdd_result")

# 保存复杂对象
demo_rdd = sc.parallelize([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
])
demo_rdd.saveAsTextFile("output/demo_rdd")
```

## 5. KV RDD 操作

### 5.1 reduceByKey

```python
# reduceByKey：按 Key 聚合（相同 Key 的 Value 合并）

# WordCount 示例
words = sc.parallelize([
    "spark", "hadoop", "spark", "hive",
    "spark", "flink", "hadoop", "spark"
])

# 将每个单词映射为 (word, 1)
pairs = words.map(lambda word: (word, 1))

# 按单词聚合计数
word_counts = pairs.reduceByKey(lambda a, b: a + b)
print(word_counts.collect())
# [('spark', 4), ('hadoop', 2), ('hive', 1), ('flink', 1)]
```

```
reduceByKey 执行过程：

输入 KV RDD：
  分区0: [("spark", 1), ("hadoop", 1), ("spark", 1)]
  分区1: [("hive", 1), ("spark", 1), ("flink", 1), ("hadoop", 1)]

Step 1：分区内局部聚合（Map 端 Combiner）
  分区0: [("spark", 2), ("hadoop", 1)]
  分区1: [("hive", 1), ("spark", 1), ("flink", 1), ("hadoop", 1)]

Step 2：Shuffle（按 Key 重新分区）
  "spark" 分区: [2, 1]  (来自分区0和分区1)
  "hadoop" 分区: [1, 1]
  "hive" 分区: [1]
  "flink" 分区: [1]

Step 3：分区间最终聚合
  [("spark", 3), ("hadoop", 2), ("hive", 1), ("flink", 1)]

注意：reduceByKey 在 Shuffle 前会先做分区内聚合（Combiner），
这是它比 groupByKey 高效的关键！
```

### 5.2 groupByKey

```python
# groupByKey：按 Key 分组（Value 不做聚合）
pairs = sc.parallelize([
    ("spark", 1), ("hadoop", 1), ("spark", 1),
    ("hive", 1), ("spark", 1), ("flink", 1)
])

grouped = pairs.groupByKey()
print(grouped.mapValues(list).collect())
# [('spark', [1, 1, 1]), ('hadoop', [1]), ('hive', [1]), ('flink', [1])]

# 聚合
word_counts = grouped.mapValues(sum)
print(word_counts.collect())
# [('spark', 3), ('hadoop', 1), ('hive', 1), ('flink', 1)]
```

```
reduceByKey vs groupByKey：

reduceByKey：
  ┌─────┐     ┌───────────┐     ┌──────────┐
  │(s,1)│     │(s,2)      │     │(s,3)     │
  │(h,1)│ ──> │(h,1)      │ ──> │(h,2)     │
  │(s,1)│     │           │     │(f,1)     │
  │(f,1)│     │(f,1)      │     │           │
  └─────┘     └───────────┘     └──────────┘
  分区内聚合     Shuffle            分区间聚合
  （Combiner）  数据量更小         最终结果

groupByKey：
  ┌─────┐                      ┌──────────────┐
  │(s,1)│                      │(s,[1,1,1])   │
  │(h,1)│ ───────────────────> │(h,[1,1])     │
  │(s,1)│   Shuffle 全部数据   │(f,[1])       │
  │(f,1)│   （不预先聚合）     │              │
  └─────┘                      └──────────────┘

  结论：聚合场景下，reduceByKey 比 groupByKey 高效得多！
  groupByKey 会导致所有数据在 Shuffle 传输，内存压力大。
```

### 5.3 aggregateByKey

```python
# aggregateByKey：更灵活的聚合函数（可以返回不同类型）

# 示例：计算每个作者的 (总浏览量, 文章数)
demo_rdd = sc.parallelize([
    ("张三", 1500),
    ("张三", 2800),
    ("张三", 4500),
    ("李四", 2300),
    ("李四", 800),
    ("王五", 3200),
])

# aggregateByKey(zeroValue, seqFunc, combFunc)
# zeroValue: 初始值 (0, 0) -- (总浏览量, 文章数)
# seqFunc: 分区内聚合 (sum_views, count_articles)
# combFunc: 分区间合并
result = demo_rdd.aggregateByKey(
    (0, 0),                                # 初始值
    lambda acc, val: (acc[0] + val, acc[1] + 1),  # 分区内聚合
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 分区间合并
)

print(result.collect())
# [('张三', (8800, 3)), ('李四', (3100, 2)), ('王五', (3200, 1))]

# 计算平均值
avg_result = result.mapValues(lambda x: x[0] / x[1])
print(avg_result.collect())
# [('张三', 2933.33...), ('李四', 1550.0), ('王五', 3200.0)]
```

### 5.4 sortByKey / sortBy

```python
# sortByKey：按 Key 排序（KV RDD）
kv_rdd = sc.parallelize([
    ("cherry", 1), ("apple", 5), ("banana", 3)
])
sorted_kv = kv_rdd.sortByKey()
print(sorted_kv.collect())
# [('apple', 5), ('banana', 3), ('cherry', 1)]

# sortBy：按任意条件排序（普通 RDD）
demo_rdd = sc.parallelize([
    ("Spark入门", 1500), ("Hadoop教程", 2300), ("Hive数据仓库", 800)
])
sorted_by_views = demo_rdd.sortBy(lambda x: x[1], ascending=False)
print(sorted_by_views.collect())
# [('Hadoop教程', 2300), ('Spark入门', 1500), ('Hive数据仓库', 800)]
```

## 6. 广播变量（Broadcast Variable）

### 6.1 原理

```
普通方式 vs 广播变量：

普通方式（每个 Task 都会发送一份数据）：
  Driver ──> Task1: [小表数据]  ──> Executor1
  Driver ──> Task2: [小表数据]  ──> Executor1
  Driver ──> Task3: [小表数据]  ──> Executor2
  Driver ──> Task4: [小表数据]  ──> Executor2
  总共传输 4 次

广播变量方式（每个 Executor 只发送一次）：
  Driver ──> Broadcast Variable ──> Executor1 (一次)
                                    Executor2 (一次)
  总共传输 2 次

适用场景：
  - 需要在多个 Task 中共享的大只读数据
  - 如配置信息、字典表、机器学习模型等
```

### 6.2 使用方法

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "BroadcastDemo")

# 定义需要广播的数据（字典表）
author_info = {
    "张三": {"city": "北京", "role": "大数据工程师"},
    "李四": {"city": "上海", "role": "数据仓库工程师"},
    "王五": {"city": "深圳", "role": "流计算工程师"},
}

# 创建广播变量
broadcast_author = sc.broadcast(author_info)

# 在 RDD 操作中使用广播变量
demo_rdd = sc.parallelize([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
    ("Flink入门", "王五", 3200),
])

# 使用 .value 获取广播变量的值
enriched = demo_rdd.map(lambda x: (
    x[0],
    x[1],
    x[2],
    broadcast_author.value.get(x[1], {}).get("city", "未知"),
    broadcast_author.value.get(x[1], {}).get("role", "未知"),
))

print(enriched.collect())
# [('Spark入门', '张三', 1500, '北京', '大数据工程师'), ...]

# 释放广播变量（不再需要时）
broadcast_author.unpersist()
```

## 7. 累加器（Accumulator）

### 7.1 原理

```
累加器原理：

  Driver:
    ┌──────────────────────┐
    │ accumulator = 0      │
    └──────────┬───────────┘
               │ 初始化
    ┌──────────┼───────────┐
    v          v           v
  Executor1  Executor2  Executor3
  ┌────────┐ ┌────────┐ ┌────────┐
  │ acc += │ │ acc += │ │ acc += │
  │   5    │ │   3    │ │   7    │
  └───┬────┘ └───┬────┘ └───┬────┘
      └──────┬───┘──────┬───┘
             v          v
         局部结果: 5, 3, 7
             │          │
             v          v
           Driver 端合并
             │
             v
         最终结果: 15

特点：
  - 只能被 Executor 添加（+=），不能读取
  - 只有 Driver 可以读取最终值
  - 用于计数、求和等场景
```

### 7.2 使用方法

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorDemo")

# 创建累加器
total_views = sc.accumulator(0)
article_count = sc.accumulator(0)
error_count = sc.accumulator(0)

demo_rdd = sc.parallelize([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
    ("Hive数据仓库", "王五", 800),
    (None, "赵六", 950),  # 无标题文章
    ("Flink入门", "王五", 3200),
])

def process_demo(item):
    title, author, views = item
    if title is None:
        error_count.add(1)  # 记录异常
        return None
    article_count.add(1)    # 计数
    total_views.add(views)  # 累加
    return item

demo_rdd.map(process_demo).filter(lambda x: x is not None).collect()

# 在 Driver 端读取累加器的值
print(f"有效文章数: {article_count.value}")   # 4
print(f"总浏览量: {total_views.value}")       # 7850
print(f"异常文章数: {error_count.value}")     # 1
```

## 8. RDD 持久化

### 8.1 cache / persist

```python
from pyspark import SparkContext, StorageLevel

sc = SparkContext("local[*]", "PersistenceDemo")

rdd = sc.parallelize(range(1000000))

# cache = persist(MEMORY_ONLY)
rdd_cached = rdd.cache()
rdd_cached.count()  # 触发缓存

# persist 指定存储级别
rdd.persist(StorageLevel.MEMORY_AND_DISK)    # 内存 + 磁盘
rdd.persist(StorageLevel.MEMORY_ONLY_SER)    # 序列化存储
rdd.persist(StorageLevel.DISK_ONLY)          # 仅磁盘
```

### 8.2 checkpoint

```python
# checkpoint：将 RDD 持久化到可靠存储（如 HDFS）
# 与 cache/persist 的区别：checkpoint 会截断血缘关系

sc.setCheckpointDir("hdfs:///checkpoint")

rdd = sc.textFile("hdfs:///data/demos.txt")
rdd = rdd.map(parse_line).filter(valid_record)

# checkpoint（会截断血缘关系）
rdd.checkpoint()
rdd.count()  # 触发 checkpoint

# 适用场景：血缘关系很长时，防止任务重试时间过长
```

```
cache vs checkpoint：

cache：
  保留血缘关系
  ┌───┐   ┌───┐   ┌───┐   ┌───┐
  │RDD│──>│RDD│──>│RDD│──>│RDD│ (cached)
  └───┘   └───┘   └───┘   └───┘
  如果缓存丢失，可以从源头重新计算

checkpoint：
  截断血缘关系
  ┌───┐   ┌───┐   ┌───┐   ┌───┐
  │RDD│──>│RDD│──>│RDD│──>│RDD│ (checkpointed)
  └───┘   └───┘   └───┘   └───┘
                           │
  ┌───┐   ┌───┐   ┌───┐   │
  │   │   │   │   │   │ <──┘  血缘被截断
  └───┘   └───┘   └───┘
  如果 checkpoint 数据丢失，无法从源头恢复
  但不需要维护长血缘关系
```

## 9. 实战：WordCount（RDD vs DataFrame）

### 9.1 RDD 方式

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "WordCountRDD")

# 模拟博客文章内容
articles = [
    "Spark是一个快速的大数据处理引擎，Spark支持批处理和流处理",
    "Hadoop是大数据的基础设施，Hadoop包含HDFS和MapReduce",
    "Spark比MapReduce快很多倍，Spark使用内存计算提高性能",
    "Hive是建立在Hadoop之上的数据仓库工具",
    "Flink是一个流处理框架，Flink支持事件时间处理",
]

# Step 1：创建 RDD
rdd = sc.parallelize(articles)

# Step 2：分词
words = rdd.flatMap(lambda line: line.replace("，", " ").replace("。", " ").split())

# Step 3：映射为 (word, 1)
pairs = words.map(lambda word: (word, 1))

# Step 4：按 word 聚合
word_counts = pairs.reduceByKey(lambda a, b: a + b)

# Step 5：排序
sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)

# Step 6：收集结果
print("=== RDD WordCount ===")
for word, count in sorted_counts.collect():
    print(f"  {word}: {count}")
```

### 9.2 DataFrame 方式

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count as _count, desc

spark = SparkSession.builder.appName("WordCountDF").getOrCreate()

# 创建 DataFrame
df = spark.createDataFrame([(line,) for line in articles], ["content"])

# DataFrame 方式实现 WordCount
result = df.withColumn("word", explode(split(col("content"), "[，。 ]"))) \
    .filter(col("word") != "") \
    .groupBy("word") \
    .count() \
    .orderBy(desc("count"))

print("\n=== DataFrame WordCount ===")
result.show(truncate=False)
```

### 9.3 两种方式对比

```
RDD vs DataFrame WordCount 对比：

RDD 方式：
  rdd.flatMap(split)       -- 1 个 Transformation
     .map(to_pair)          -- 1 个 Transformation
     .reduceByKey(add)      -- 1 个 Transformation (Shuffle)
     .sortBy(views)         -- 1 个 Transformation (Shuffle)
     .collect()             -- 1 个 Action

  特点：步骤清晰，每个操作都是手动控制
  缺点：需要自己处理分词、空字符串过滤等

DataFrame 方式：
  df.withColumn(explode(split))
     .filter(not empty)
     .groupBy.count()
     .orderBy
     .show()

  特点：代码更简洁，Catalyst 自动优化
  优点：SQL 函数丰富，不需要自己处理很多细节
```

## 10. 总结

```
┌──────────────────────────────────────────────────────────┐
│               RDD 编程模型总结                             │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Transformations：                                       │
│  - map, flatMap, filter, mapPartitions                  │
│  - reduceByKey, groupByKey, aggregateByKey              │
│  - sortByKey, join, leftOuterJoin, union                │
│  - coalesce, repartition, distinct, sample              │
│                                                          │
│  Actions：                                               │
│  - collect, count, reduce, first, take, top             │
│  - foreach, foreachPartition, saveAsTextFile            │
│                                                          │
│  高级特性：                                               │
│  - 广播变量：共享只读大表，减少网络传输                    │
│  - 累加器：Executor 端计数，Driver 端读取                 │
│  - 持久化：cache, persist, checkpoint                    │
│                                                          │
│  最佳实践：                                               │
│  - 优先使用 DataFrame API                                │
│  - 聚合场景用 reduceByKey（不用 groupByKey）              │
│  - 减少分区用 coalesce（不用 repartition）               │
│  - 大只读数据用广播变量                                  │
│  - 血缘很长时用 checkpoint                               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[07-Spark与Hive集成.md](07-Spark与Hive集成.md) — Spark 与 Hive 集成，学习读写 Hive 表和元数据。
