# DataFrame API 详解

## 1. Transformations 分类详解

Transformations 是惰性操作，定义了数据如何从一种形式转换为另一种形式。只有遇到 Action 时才会真正执行。

```
DataFrame Transformations 分类：

┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  过滤类       │  │  选择类       │  │  排序类       │      │
│  │  filter      │  │  select      │  │  orderBy     │      │
│  │  where       │  │  selectExpr  │  │  sort        │      │
│  │  distinct    │  │  drop        │  │  sortWithin  │      │
│  │  dropDup     │  │              │  │  Partitions  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  分组类       │  │  合并类       │  │  重命名类     │      │
│  │  groupBy     │  │  union       │  │  withCol     │      │
│  │  rollup      │  │  join        │  │  Renamed     │      │
│  │  cube        │  │  crossJoin   │  │  alias       │      │
│  │              │  │              │  │              │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  类型转换     │  │  缺失值处理   │  │  字符串操作   │      │
│  │  cast        │  │  fillna      │  │  split       │      │
│  │  astype      │  │  dropna      │  │  substring   │      │
│  │              │  │  na.fill     │  │  trim        │      │
│  │              │  │  replace     │  │  lower/upper │      │
│  │              │  │              │  │  contains    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 1.1 准备示例数据

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DataFrameAPIDemo") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("tags", StringType(), True),
    StructField("publish_date", StringType(), True),
])

demo_data = [
    (1, "Spark入门教程", "张三", 1500, 50, "spark,大数据,hadoop", "2024-01-15"),
    (2, "Spark SQL详解", "张三", 2800, 120, "spark,sql", "2024-02-28"),
    (3, "Spark性能调优", "张三", 4500, 200, "spark,调优", "2024-05-18"),
    (4, "Hadoop核心原理", "李四", 2300, 80, "hadoop,大数据", "2024-02-20"),
    (5, "Hive数据仓库", "李四", 800, 30, "hive,数据仓库", "2024-03-10"),
    (6, "Flink流处理", "王五", 3200, 150, "flink,流计算", "2024-04-05"),
    (7, "Kafka消息队列", "王五", 1800, 60, "kafka,中间件", "2024-06-22"),
    (8, "Python数据分析", "赵六", 950, 40, "python,数据分析", "2024-07-30"),
    (9, None, "赵六", None, 20, "docker,运维", "2024-08-12"),
    (10, "K8s实战", "王五", 2600, 100, "k8s,运维", "2024-09-01"),
]

df = spark.createDataFrame(demo_data, schema)
df.createOrReplaceTempView("demos")
```

## 2. 过滤类 Transformations

### 2.1 filter / where

```python
# filter 和 where 完全等价

# 字符串表达式
df.filter("views > 1000").show(3)
df.where("views > 1000").show(3)

# 列对象表达式
df.filter(col("views") > 1000).show(3)

# 多条件 AND
df.filter((col("views") > 1000) & (col("author") == "张三")).show()

# 多条件 OR
df.filter((col("views") > 3000) | (col("likes") > 100)).show()

# NOT 条件
df.filter(~(col("author") == "赵六")).show()

# 条件过滤组合
df.filter(
    (col("views") > 1000) &
    (col("likes") > 50) &
    (col("title").isNotNull())
).show()

# 字符串匹配
df.filter(col("title").like("%Spark%")).show()         # SQL LIKE
df.filter(col("title").contains("Spark")).show()        # 包含
df.filter(col("title").startswith("Spark")).show()      # 以...开头
df.filter(col("title").endswith("教程")).show()         # 以...结尾

# 正则匹配
import pyspark.sql.functions as F
df.filter(F.col("title").rlike("^Spark.*")).show()

# isNull / isNotNull
df.filter(col("title").isNull()).show()
df.filter(col("title").isNotNull()).show()

# isin
df.filter(col("author").isin("张三", "王五")).show()

# between
df.filter(col("views").between(1000, 3000)).show()
```

### 2.2 distinct

```python
# 去除完全相同的行
df.distinct().show()

# 对特定列去重（使用 select + distinct）
df.select("author").distinct().show()

# 使用 dropDuplicates 对指定列去重
df.dropDuplicates(["author"]).show()  # 每个 author 保留第一行

# 多列去重
df.dropDuplicates(["author", "tags"]).show()
```

### 2.3 dropDuplicates

```python
# dropDuplicates 和 distinct 的区别：
# distinct：所有列都相同的行才去重
# dropDuplicates：可以指定按哪些列去重

# 按 author 去重，保留每组第一条记录
df.dropDuplicates(["author"]).select("id", "title", "author").show()

# 按多列去重
df.dropDuplicates(["author", "publish_date"]).show()
```

## 3. 选择类 Transformations

### 3.1 select

```python
# 选择列名
df.select("id", "title", "views").show(3)

# 选择列对象
df.select(col("id"), col("title"), col("views")).show(3)

# 使用表达式
df.select(
    col("id"),
    col("title"),
    col("views"),
    (col("views") + col("likes")).alias("engagement"),
    (col("views") * 0.01).alias("score")
).show(3)

# 使用 selectExpr（SQL 表达式）
df.selectExpr(
    "id",
    "title",
    "views",
    "views + likes as engagement",
    "views * 0.01 as score",
    "UPPER(author) as author_upper",
    "SUBSTRING(publish_date, 1, 7) as month"
).show(3)
```

### 3.2 drop

```python
# 删除单列
df.drop("tags").show(3)

# 删除多列
df.drop("tags", "likes", "publish_date").show(3)

# 链式删除
df.drop("tags").drop("likes").drop("publish_date").show(3)

# 删除不存在的列不会报错
df.drop("nonexistent").show(3)
```

## 4. 排序类 Transformations

### 4.1 orderBy / sort

```python
# orderBy 和 sort 完全等价

# 升序（默认）
df.orderBy("views").show(5)

# 降序
df.orderBy(col("views").desc()).show(5)
df.orderBy("views", ascending=False).show(5)

# 多列排序
df.orderBy(["author", "views"], ascending=[True, False]).show(5)

# 使用表达式排序
df.sort(col("views").desc(), col("likes").desc()).show(5)
```

### 4.2 sortWithinPartitions

```python
# 在每个分区内排序（不触发 Shuffle，性能更好）
# 适用于已经按某列分区的数据

# 先按 author 分区，再在每个分区内按 views 排序
result = df.repartition("author") \
    .sortWithinPartitions(col("views").desc())

result.show()
```

## 5. 分组类 Transformations

### 5.1 groupBy

```python
from pyspark.sql.functions import sum as _sum, avg as _avg, \
    count as _count, max as _max, min as _min, countDistinct

# 单列分组
df.groupBy("author").count().show()

# 多列分组
df.groupBy("author", "publish_date").count().show()

# groupBy + agg
result = df.groupBy("author").agg(
    _count("*").alias("article_count"),
    _sum("views").alias("total_views"),
    _avg("views").alias("avg_views"),
    _max("views").alias("max_views"),
    _min("views").alias("min_views"),
    countDistinct("publish_date").alias("active_days"),
).orderBy("total_views", ascending=False)

result.show()
```

### 5.2 rollup

```python
# rollup：层级聚合，包含小计和总计
# 按 author -> category 层级聚合

# 添加分类列
df_with_cat = df.withColumn("category",
    F.when(F.col("title").like("%Spark%"), "Spark")
     .when(F.col("title").like("%Hadoop%"), "Hadoop")
     .otherwise("其他")
)

result = df_with_cat.rollup("author", "category").agg(
    _sum("views").alias("total_views")
).orderBy("author", "category")

result.show()
```

输出：
```
+------+--------+-----------+
|author|category|total_views|
+------+--------+-----------+
|  null|    null|      20450|  -- 总计
|  张三|    null|      8800|  -- 张三小计
|  张三|  Spark |      8800|  -- 张三-Spark
|  李四|    null|      3100|  -- 李四小计
|  李四| Hadoop|      2300|  -- 李四-Hadoop
|  李四|   其他 |       800|  -- 李四-其他
|  王五|    null|      7600|  -- 王五小计
|  王五|   其他 |      7600|  -- 王五-其他
|  赵六|    null|       950|  -- 赵六小计
|  赵六|   其他 |       950|  -- 赵六-其他
+------+--------+-----------+
```

### 5.3 cube

```python
# cube：所有维度组合的聚合
# rollup 只做层级聚合，cube 做所有组合

result = df_with_cat.cube("author", "category").agg(
    _sum("views").alias("total_views")
).orderBy("author", "category")

result.show()
```

## 6. 合并类 Transformations

### 6.1 union / unionAll

```python
df1 = df.filter(col("author") == "张三")
df2 = df.filter(col("author") == "王五")

# unionAll：保留重复行（Spark 2.0+ 中 union 和 unionAll 相同）
combined = df1.unionAll(df2)
combined.select("title", "author").show()

# union：去重
combined_distinct = df1.union(df2)
```

### 6.2 join

```python
# 创建关联表
categories = spark.createDataFrame([
    ("spark", "Spark生态"), ("hadoop", "Hadoop生态"),
    ("flink", "流计算"), ("kafka", "消息队列"),
], ["tag", "category_name"])

# Inner Join
df.join(categories, df["tags"].contains(categories["tag"]), "inner").show(3)

# Left Join
df.join(categories, df["tags"].contains(categories["tag"]), "left").show(3)

# Cross Join（笛卡尔积，慎用）
df.limit(3).crossJoin(categories.limit(2)).show()
```

## 7. 重命名 Transformations

### 7.1 withColumnRenamed

```python
# 重命名单列
df_renamed = df.withColumnRenamed("views", "view_count")

# 链式重命名多列
df_renamed = df \
    .withColumnRenamed("views", "view_count") \
    .withColumnRenamed("likes", "like_count") \
    .withColumnRenamed("publish_date", "pub_date")

print(df_renamed.columns)
# ['id', 'title', 'author', 'view_count', 'like_count', 'tags', 'pub_date']
```

### 7.2 alias

```python
# 列别名（在 select 中使用）
df.select(
    col("author").alias("作者"),
    col("views").alias("浏览量"),
    col("likes").alias("点赞数")
).show(3)

# DataFrame 别名（在 join 中使用）
df.alias("a").join(
    categories.alias("c"),
    F.col("a.tags").contains(F.col("c.tag"))
).select("a.title", "c.category_name").show(3)
```

## 8. 类型转换

### 8.1 cast

```python
# 使用 cast 转换类型
df_typed = df.withColumn("views_str", col("views").cast("string")) \
    .withColumn("id_long", col("id").cast("long")) \
    .withColumn("views_double", col("views").cast("double"))

# 查看 Schema 验证类型转换
df_typed.printSchema()

# 字符串转数字（处理可能的 null 值）
df_typed = df.withColumn("views_int", col("views").cast(IntegerType()))

# 安全的类型转换（如果转换失败返回 null）
from pyspark.sql.functions import to_date, to_timestamp

df = df.withColumn("pub_date",
    F.to_date(col("publish_date"), "yyyy-MM-dd")
)

df = df.withColumn("pub_timestamp",
    F.to_timestamp(col("publish_date"), "yyyy-MM-dd HH:mm:ss")
)
```

## 9. 缺失值处理

### 9.1 查看缺失值

```python
# 检查每列的缺失值数量
from pyspark.sql.functions import isnull, when, count as _count

missing_stats = df.select([
    _count(when(isnull(c), c)).alias(c)
    for c in df.columns
])
missing_stats.show()

# 检查缺失值比例
total = df.count()
missing_ratio = df.select([
    (_count(when(isnull(c), c)) / total).alias(c)
    for c in df.columns
])
missing_ratio.show()
```

### 9.2 dropna（删除缺失值）

```python
# dropna：删除包含 null 值的行

# any：任意列有 null 就删除（默认）
df.dropna().show()

# all：所有列都为 null 才删除
df.dropna(how="all").show()

# thresh：至少有 N 个非 null 值才保留
df.dropna(thresh=5).show()  # 至少 5 列非 null

# subset：只检查指定列
df.dropna(subset=["title", "views"]).show()
```

### 9.3 fillna / na.fill（填充缺失值）

```python
# 对所有列填充相同值
df.fillna("未知").show()       # 字符串列填充 "未知"
df.fillna(0).show()            # 数值列填充 0

# 对指定列填充不同值
df.fillna({
    "title": "无标题",
    "views": 0,
    "likes": 0,
    "author": "匿名"
}).show()

# 使用 na.fill（与 fillna 等价）
df.na.fill("未知").show()
```

### 9.4 replace（替换值）

```python
# 替换指定值
df.replace("张三", "作者A").show()

# 按列替换
df.replace("张三", "作者A", subset=["author"]).show()

# 替换多个值
df.replace(["张三", "李四"], ["作者A", "作者B"], subset=["author"]).show()

# 数值替换
df.replace(0, -1, subset=["views"]).show()
```

## 10. 字符串操作

### 10.1 split

```python
import pyspark.sql.functions as F

# split：将字符串按分隔符拆分为数组
df_split = df.withColumn("tag_array", F.split(col("tags"), ","))

# 展开数组（explode）
df_exploded = df.withColumn("tag_array", F.split(col("tags"), ",")) \
    .withColumn("single_tag", F.explode(col("tag_array")))

df_exploded.select("title", "single_tag").show()
```

### 10.2 substring

```python
# substring(col, start, length) — 注意 start 从 1 开始
df.withColumn("year", F.substring(col("publish_date"), 1, 4)).show(3)
df.withColumn("month", F.substring(col("publish_date"), 6, 2)).show(3)

# 截取前 N 个字符
df.withColumn("short_title", F.substring(col("title"), 1, 10)).show(3)
```

### 10.3 trim / ltrim / rtrim

```python
# 去除首尾空格
df.withColumn("title_trimmed", F.trim(col("title"))).show(3)

# 去除左/右空格
df.withColumn("title_ltrim", F.ltrim(col("title"))).show(3)
df.withColumn("title_rtrim", F.rtrim(col("title"))).show(3)

# 去除指定字符
df.withColumn("tags_clean", F.trim(F.col("tags"), ",")).show(3)
```

### 10.4 lower / upper

```python
# 转小写
df.withColumn("title_lower", F.lower(col("title"))).show(3)

# 转大写
df.withColumn("title_upper", F.upper(col("title"))).show(3)

# 首字母大写（initcap）
df.withColumn("title_cap", F.initcap(col("title"))).show(3)
```

### 10.5 contains

```python
# 包含子字符串
df.filter(col("title").contains("Spark")).show()

# 等价于
df.filter(col("title").like("%Spark%")).show()

# 正则匹配
df.filter(col("title").rlike("^Spark.*")).show()
```

### 10.6 concat / concat_ws

```python
# 拼接字符串
df.withColumn("full_info", F.concat(col("title"), F.lit(" - "), col("author"))).show(3)

# 使用分隔符拼接
df.withColumn("full_info", F.concat_ws(" | ", col("title"), col("author"), col("views").cast("string"))).show(3)

# 从数组拼接回字符串
df.withColumn("tag_array", F.split(col("tags"), ",")) \
  .withColumn("tag_str", F.concat_ws(";", col("tag_array"))).show(3)
```

### 10.7 其他常用字符串函数

```python
# 字符串长度
df.withColumn("title_len", F.length(col("title"))).show(3)

# 反转字符串
df.withColumn("title_rev", F.reverse(col("title"))).show(3)

# 重复字符串
df.withColumn("title_repeat", F.repeat(col("title"), 2)).show(3)

# 左/右填充
df.withColumn("title_padded", F.lpad(col("title"), 20, "*")).show(3)

# 字符串截取（从右边）
df.withColumn("year", F.right(col("publish_date"), 4)).show(3)

# 提取正则匹配组
df.withColumn("first_tag", F.regexp_extract(col("tags"), r"^([^,]+)", 1)).show(3)

# 正则替换
df.withColumn("tags_clean", F.regexp_replace(col("tags"), r"[\s,]+", "|")).show(3)
```

## 11. Actions 详解

### 11.1 show

```python
# show 是最常用的 Action
df.show()                    # 默认显示 20 行
df.show(5)                   # 显示 5 行
df.show(5, truncate=False)   # 不截断
df.show(5, vertical=True)    # 垂直显示
df.show(5, truncate=30)      # 最多显示 30 个字符
```

### 11.2 collect

```python
# collect：将所有数据收集到 Driver（慎用，数据量大时会 OOM）
rows = df.collect()
print(type(rows))  # list
print(type(rows[0]))  # pyspark.sql.types.Row

# 遍历结果
for row in rows:
    print(f"{row['title']} - {row['views']}")

# 只收集需要的列（减少内存使用）
titles = df.select("title").collect()
```

### 11.3 count

```python
# 总行数
total = df.count()
print(f"总行数: {total}")

# 过滤后的行数
filtered_count = df.filter(col("views") > 1000).count()
print(f"浏览量 > 1000 的文章数: {filtered_count}")
```

### 11.4 take / first / head

```python
# take(n)：返回前 n 行（list）
rows = df.take(3)
print(rows)

# first()：返回第一行（Row）
row = df.first()
print(row["title"], row["views"])

# head(n)：与 take 类似
rows = df.head(3)
print(rows)

# limit 是 Transformation，take/head 是 Action
# df.limit(3)  -- Transformation，返回 DataFrame
# df.take(3)   -- Action，返回 list
```

### 11.5 foreach

```python
# foreach：对每行执行操作（不返回结果）
# 在 Executor 端执行，不在 Driver 端

df.foreach(lambda row: print(row.title))

# 使用 foreachPartition（对每个分区执行）
def process_partition(iterator):
    for row in iterator:
        # 在每个分区中处理数据
        pass
    yield None

df.foreachPartition(process_partition)
```

### 11.6 write — 写入数据

```python
# 写入 Parquet（推荐）
df.write.parquet("output/demos.parquet")

# 写入 JSON
df.write.json("output/demos.json")

# 写入 CSV
df.write.csv("output/demos.csv", header=True)

# 写入 ORC
df.write.orc("output/demos.orc")

# 覆盖写入（Overwrite）
df.write.mode("overwrite").parquet("output/demos.parquet")

# 追加写入（Append）
df.write.mode("append").parquet("output/demos.parquet")

# 忽略（如果已存在则不写入）
df.write.mode("ignore").parquet("output/demos.parquet")

# 报错（如果已存在则报错，默认）
df.write.mode("error").parquet("output/demos.parquet")

# 写入时分区
df.write.partitionBy("author").parquet("output/demos_partitioned")

# 写入时排序和分桶
df.write.bucketBy(4, "author").sortBy("views").saveAsTable("demo_bucketed")

# 写入单个文件（合并输出）
df.coalesce(1).write.csv("output/demos_single.csv", header=True)
```

## 12. cache / persist / unpersist

### 12.1 原理

```
缓存原理：

没有缓存：
  ┌──────┐   ┌──────┐   ┌──────┐
  │ 查询1 │   │ 查询2 │   │ 查询3 │
  │读取源 │   │读取源 │   │读取源 │
  └──────┘   └──────┘   └──────┘
       v         v         v
  重新计算   重新计算   重新计算

使用缓存：
  ┌──────┐   ┌──────┐   ┌──────┐
  │ 查询1 │   │ 查询2 │   │ 查询3 │
  │首次计算│   │读缓存 │   │读缓存 │
  └──────┘   └──────┘   └──────┘
       v         v         v
     ┌───────────────────────┐
     │     内存/磁盘缓存      │
     │   (第一次计算后存储)    │
     └───────────────────────┘
```

### 12.2 cache vs persist

```python
# cache = persist(MEMORY_AND_DISK) — 仅内存缓存
df_cached = df.cache()
df_cached.count()  # 触发缓存
df_cached.count()  # 从缓存读取，速度快

# persist — 可以指定存储级别
from pyspark.sql import StorageLevel

# 仅内存
df.persist(StorageLevel.MEMORY_ONLY)

# 内存 + 磁盘（推荐）
df.persist(StorageLevel.MEMORY_AND_DISK)

# 仅磁盘
df.persist(StorageLevel.DISK_ONLY)

# 内存，序列化（省内存但费 CPU）
df.persist(StorageLevel.MEMORY_ONLY_SER)

# 内存 + 磁盘，序列化
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# 内存中保留 2 份副本（更可靠）
df.persist(StorageLevel.MEMORY_ONLY_2)
```

存储级别说明：

```
┌──────────────────────────────────────────────────────────────┐
│                    StorageLevel 说明                          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  MEMORY_ONLY         仅内存，不溢写磁盘。空间不足时部分丢失   │
│  MEMORY_AND_DISK     内存优先，空间不足溢写磁盘（推荐）        │
│  DISK_ONLY           仅磁盘，不使用内存                       │
│  MEMORY_ONLY_SER     内存中序列化存储（省空间，费 CPU）        │
│  MEMORY_AND_DISK_SER 内存+磁盘，序列化                       │
│  MEMORY_ONLY_2       内存中保留 2 份副本                     │
│  OFF_HEAP            使用堆外内存                             │
│                                                              │
│  选择建议：                                                   │
│  - 数据量小、内存充足：MEMORY_ONLY                           │
│  - 数据量大、内存不足：MEMORY_AND_DISK（默认）               │
│  - 数据量大、需要频繁复用：MEMORY_AND_DISK_SER              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 12.3 unpersist

```python
# 释放缓存
df.unpersist()

# 在链式操作中使用
result = df.cache() \
    .filter(col("views") > 1000) \
    .count()  # 使用缓存

df.unpersist()  # 释放缓存
```

### 12.4 缓存陷阱

```python
# 陷阱 1：缓存后修改数据，缓存会失效
df_cached = df.cache()
df_cached.count()  # 缓存数据

# 对缓存后的 DataFrame 做新的 Transformation
df_new = df_cached.filter(col("views") > 1000)
# df_new 不会使用 df_cached 的缓存，因为产生了新的 DataFrame

# 正确做法：在最终需要的 DataFrame 上缓存
result = df.filter(col("views") > 1000).cache()
result.count()  # 缓存过滤后的结果
result.groupBy("author").sum("views").show()  # 使用缓存

# 陷阱 2：缓存过多导致内存不足
# 解决方案：及时 unpersist 不再需要的缓存
# 陷阱 3：缓存小数据集反而拖慢性能
# 解决方案：只缓存数据量大且需要多次使用的数据集
```

## 13. 实战：业务数据清洗完整流程

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, split, \
    explode, length, regexp_replace, to_date, round as _round, \
    sum as _sum, avg as _avg, count as _count, countDistinct
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DemoDataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# ============ 1. 读取原始数据 ============
raw_data = [
    (1, "  Spark入门教程  ", "张三", 1500, 50, " spark, 大数据 ,hadoop ", "2024-01-15"),
    (2, "  Spark SQL详解  ", " 张三", 2800, 120, " spark,sql ", "2024-02-28"),
    (3, "Spark性能调优", "张三", 4500, 200, "spark,调优", "2024-05-18"),
    (4, "Hadoop核心原理", "李四", 2300, 80, "hadoop,大数据", "2024-02-20"),
    (5, "Hive数据仓库", "李四", None, 30, "hive,数据仓库", "2024-03-10"),
    (6, "Flink流处理", "王五", 3200, 150, "flink,流计算", "2024-04-05"),
    (7, "Kafka消息队列", "王五", 1800, 60, "kafka,中间件", "2024-06-22"),
    (8, "Python数据分析", "赵六", 950, None, "python,数据分析", "2024-07-30"),
    (9, None, "赵六", None, 20, "docker,运维", "2024-08-12"),
    (10, "K8s实战", "王五", -100, 100, "k8s,运维", "invalid-date"),
    (11, "", "钱七", 600, 10, "", "2024-10-01"),
]

columns = ["id", "title", "author", "views", "likes", "tags", "publish_date"]
df_raw = spark.createDataFrame(raw_data, columns)

print(f"原始数据行数: {df_raw.count()}")
print(f"原始数据 Schema:")
df_raw.printSchema()

# ============ 2. 数据清洗 ============

# Step 1：去除空标题和空内容的行
df_clean = df_raw.filter(col("title").isNotNull() & (col("title") != ""))

# Step 2：去除前后空格
df_clean = df_clean.withColumn("title", trim(col("title")))
df_clean = df_clean.withColumn("author", trim(col("author")))

# Step 3：填充缺失值
df_clean = df_clean.fillna({
    "views": 0,
    "likes": 0
})

# Step 4：处理异常值（负数浏览量设为 0）
df_clean = df_clean.withColumn("views",
    when(col("views") < 0, 0).otherwise(col("views"))
)

# Step 5：转换日期类型（无效日期设为 null）
df_clean = df_clean.withColumn("publish_date",
    to_date(col("publish_date"), "yyyy-MM-dd")
)

# Step 6：过滤无效日期
df_clean = df_clean.filter(col("publish_date").isNotNull())

# Step 7：清理标签（去除空格，转小写，转数组）
df_clean = df_clean.withColumn("tags",
    regexp_replace(col("tags"), r"\s+", "")
)
df_clean = df_clean.withColumn("tags",
    lower(col("tags"))
)
df_clean = df_clean.withColumn("tag_array",
    split(col("tags"), ",")
)
df_clean = df_clean.withColumn("tag_count",
    length(col("tags")) - length(regexp_replace(col("tags"), ",", "")) + 1
)

# Step 8：添加衍生列
df_clean = df_clean.withColumn("engagement",
    col("views") + col("likes")
)
df_clean = df_clean.withColumn("popularity_level",
    when(col("views") >= 3000, "热门")
    .when(col("views") >= 1000, "普通")
    .otherwise("冷门")
)
df_clean = df_clean.withColumn("year",
    col("publish_date").substr(1, 4)
)

print(f"\n清洗后数据行数: {df_clean.count()}")
print(f"清洗后数据 Schema:")
df_clean.printSchema()

# ============ 3. 数据验证 ============
print("\n=== 清洗后数据预览 ===")
df_clean.select("id", "title", "author", "views", "likes", "publish_date",
               "tag_count", "engagement", "popularity_level").show()

# ============ 4. 数据分析 ============
print("\n=== 作者统计 ===")
author_stats = df_clean.groupBy("author").agg(
    _count("*").alias("文章数"),
    _sum("views").alias("总浏览量"),
    _round(_avg("views"), 2).alias("平均浏览量"),
    _sum("engagement").alias("总互动量")
).orderBy("总浏览量", ascending=False)
author_stats.show()

# ============ 5. 展开标签统计 ============
print("\n=== 标签使用统计 ===")
tag_stats = df_clean.withColumn("single_tag", explode(col("tag_array"))) \
    .filter(col("single_tag") != "") \
    .groupBy("single_tag") \
    .count() \
    .orderBy("count", ascending=False)
tag_stats.show()

# ============ 6. 写入清洗后的数据 ============
df_clean.select("id", "title", "author", "views", "likes",
               "publish_date", "tags", "popularity_level") \
    .write.mode("overwrite") \
    .parquet("output/demos_clean.parquet")

print("数据清洗完成，已写入 output/demos_clean.parquet")

spark.stop()
```

## 14. 总结

```
┌──────────────────────────────────────────────────────────┐
│              DataFrame API 知识总结                        │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Transformations（惰性执行）：                            │
│  - 过滤：filter, where, distinct, dropDuplicates         │
│  - 选择：select, selectExpr, drop                        │
│  - 排序：orderBy, sort, sortWithinPartitions             │
│  - 分组：groupBy, rollup, cube                           │
│  - 合并：union, join, crossJoin                         │
│  - 重命名：withColumnRenamed, alias                      │
│  - 类型转换：cast, to_date, to_timestamp                │
│  - 缺失值：fillna, dropna, replace                       │
│  - 字符串：split, substring, trim, lower, upper, concat  │
│                                                          │
│  Actions（立即执行）：                                    │
│  - show, collect, count, take, first, head               │
│  - foreach, foreachPartition                             │
│  - write（parquet/json/csv/orc）                         │
│                                                          │
│  缓存策略：                                              │
│  - cache() = persist(MEMORY_AND_DISK)                    │
│  - persist() 可指定存储级别                               │
│  - unpersist() 释放缓存                                  │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[06-RDD编程.md](06-RDD编程.md) — RDD 编程模型，学习 Spark 最底层的数据抽象和 API。
