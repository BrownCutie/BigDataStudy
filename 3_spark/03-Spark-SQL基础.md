# Spark SQL 基础

## 1. 创建 SparkSession

`SparkSession` 是 Spark 2.0+ 引入的统一入口，它整合了之前的 `SparkContext`、`SQLContext` 和 `HiveContext`。

### 1.1 SparkSession 架构

```
SparkSession 统一入口：

┌─────────────────────────────────────────────────────────┐
│                    SparkSession                         │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │                   SparkContext                    │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │  │
│  │  │DAG 调度器│  │Task 调度器│  │Broadcast 管理 │  │  │
│  │  └──────────┘  └──────────┘  └──────────────┘  │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   SQLContext  │  │ HiveContext  │  │ Streaming    │ │
│  │  (SQL 引擎)   │  │ (Hive 支持) │  │ (流处理)     │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 1.2 创建 SparkSession

```python
from pyspark.sql import SparkSession

# 基本创建方式
spark = SparkSession.builder \
    .appName("DemoAnalysis") \
    .getOrCreate()

# 带完整配置的创建方式
spark = SparkSession.builder \
    .appName("DemoAnalysis") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .enableHiveSupport() \  # 启用 Hive 支持
    .getOrCreate()

# 获取底层的 SparkContext
sc = spark.sparkContext

# 获取 Spark 版本
print(f"Spark 版本: {spark.version}")

# 获取 Spark UI 地址
print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
```

### 1.3 SparkSession 常用配置

```python
# 动态设置配置
spark.conf.set("spark.sql.shuffle.partitions", "100")

# 获取配置值
print(spark.conf.get("spark.sql.shuffle.partitions"))  # 100

# 获取所有配置
config_df = spark.sql("SET").filter("key LIKE 'spark.sql%'")
config_df.show(truncate=False)
```

## 2. 创建 DataFrame 的 5 种方式

### 2.1 方式一：从 Python 集合创建

```python
# 从 list of tuples 创建
demo_data = [
    ("Spark入门教程", "张三", 1500, "2024-01-15", "大数据"),
    ("Hadoop核心原理", "张三", 2300, "2024-02-20", "大数据"),
    ("Hive数据仓库", "李四", 800, "2024-03-10", "大数据"),
    ("Flink流处理", "王五", 3200, "2024-04-05", "流计算"),
    ("Spark性能调优", "张三", 4500, "2024-05-18", "大数据"),
    ("Kafka消息队列", "王五", 1800, "2024-06-22", "中间件"),
    ("Python数据分析", "赵六", 950, "2024-07-30", "数据分析"),
]

columns = ["title", "author", "views", "publish_date", "category"]
df = spark.createDataFrame(demo_data, columns)
df.show()
```

输出：
```
+----------------+------+-----+------------+--------+
|           title|author|views|publish_date|category|
+----------------+------+-----+------------+--------+
|  Spark入门教程  |  张三| 1500|  2024-01-15|    大数据|
| Hadoop核心原理  |  张三| 2300|  2024-02-20|    大数据|
|  Hive数据仓库  |  李四|  800|  2024-03-10|    大数据|
|   Flink流处理  |  王五| 3200|  2024-04-05|    流计算|
| Spark性能调优  |  张三| 4500|  2024-05-18|    大数据|
| Kafka消息队列  |  王五| 1800|  2024-06-22|    中间件|
| Python数据分析 |  赵六|  950|  2024-07-30|  数据分析|
+----------------+------+-----+------------+--------+
```

### 2.2 方式二：从 Row 对象创建（带 Schema）

```python
from pyspark.sql import Row
from pyspark.sql.types import *

# 定义 Schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), False),
    StructField("views", IntegerType(), True),
    StructField("publish_date", DateType(), True),
    StructField("is_published", BooleanType(), True),
])

# 使用 Row 对象
from datetime import date
data = [
    Row("Spark入门", "张三", 1500, date(2024, 1, 15), True),
    Row("Hadoop教程", "李四", 2300, date(2024, 2, 20), True),
]

df = spark.createDataFrame(data, schema)
df.printSchema()
```

输出：
```
root
 |-- title: string (nullable = true)
 |-- author: string (nullable = false)
 |-- views: integer (nullable = true)
 |-- publish_date: date (nullable = true)
 |-- is_published: boolean (nullable = true)
```

### 2.3 方式三：从 RDD 创建

```python
# 从 RDD 创建 DataFrame
rdd = spark.sparkContext.parallelize([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
])

# 方式 1：指定列名
df1 = rdd.toDF(["title", "author", "views"])

# 方式 2：通过 Row + Schema
from pyspark.sql import Row
row_rdd = rdd.map(lambda x: Row(title=x[0], author=x[1], views=x[2]))
df2 = spark.createDataFrame(row_rdd)
```

### 2.4 方式四：从文件创建

```python
# 从 CSV 文件创建
df_csv = spark.read.csv(
    "demos.csv",
    header=True,          # 第一行作为列名
    inferSchema=True,     # 自动推断数据类型
    sep=",",              # 分隔符
    encoding="UTF-8",     # 编码
    nullValue="NA",       # 空值表示
    timestampFormat="yyyy-MM-dd HH:mm:ss"
)

# 从 JSON 文件创建
df_json = spark.read.json("demos.json")
# 或
df_json = spark.read.format("json").load("demos.json")

# 从 Parquet 文件创建（推荐，列式存储，性能最好）
df_parquet = spark.read.parquet("demos.parquet")

# 从 ORC 文件创建
df_orc = spark.read.orc("demos.orc")

# 从文本文件创建
df_text = spark.read.text("demos.txt")
```

### 2.5 方式五：从数据库创建

```python
# 从 JDBC 数据库读取
df_jdbc = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/blog_db") \
    .option("dbtable", "articles") \
    .option("user", "root") \
    .option("password", "password") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# 从 Hive 表读取（需要启用 Hive 支持）
df_hive = spark.table("blog_db.articles")

# 使用 spark.read.table()
df_hive2 = spark.read.table("blog_db.articles")
```

## 3. 基本 DataFrame 操作

### 3.1 select — 选择列

```python
# 选择单列
df.select("title").show()

# 选择多列
df.select("title", "author", "views").show()

# 使用 col 函数
from pyspark.sql.functions import col
df.select(col("title"), col("views")).show()

# 使用表达式
df.select("title", df["views"] * 2).show()

# selectExpr — 使用 SQL 表达式
df.selectExpr(
    "title",
    "author",
    "views * 1.5 as estimated_views",
    "UPPER(category) as category_upper"
).show()
```

### 3.2 filter / where — 过滤行

```python
# filter 和 where 完全等价，只是别名

# 字符串表达式
df.filter("views > 1000").show()
df.where("views > 1000").show()

# 列对象表达式
df.filter(col("views") > 1000).show()
df.filter(df["views"] > 1000).show()

# 多条件过滤
df.filter((col("views") > 1000) & (col("category") == "大数据")).show()
df.filter((col("views") > 1000) | (col("author") == "张三")).show()
df.filter(~(col("category") == "数据分析")).show()

# isin 过滤
df.filter(col("author").isin("张三", "王五")).show()

# isNull / isNotNull
df.filter(col("views").isNull()).show()
df.filter(col("views").isNotNull()).show()

# like 模糊匹配
df.filter(col("title").like("%Spark%")).show()

# between 区间过滤
df.filter(col("views").between(1000, 3000)).show()

# startswith / endswith
df.filter(col("title").startswith("Spark")).show()
df.filter(col("title").endswith("教程")).show()
```

### 3.3 orderBy / sort — 排序

```python
# 升序排序（默认）
df.orderBy("views").show()

# 降序排序
df.orderBy("views", ascending=False).show()
df.orderBy(col("views").desc()).show()

# 多列排序
df.orderBy(["category", "views"], ascending=[True, False]).show()

# sort 是 orderBy 的别名
df.sort("views", ascending=False).show()
```

### 3.4 limit — 限制行数

```python
# 取前 3 行
df.limit(3).show()

# 结合排序，取浏览量最高的 3 篇文章
df.orderBy("views", ascending=False).limit(3).show()
```

## 4. 列操作

### 4.1 withColumn — 新增或替换列

```python
from pyspark.sql.functions import col, lit, upper, lower, length

# 新增列
df = df.withColumn("likes", lit(0))
df = df.withColumn("title_length", length(col("title")))

# 替换列（同名列会覆盖）
df = df.withColumn("views", col("views") + 100)

# 基于条件新增列
from pyspark.sql.functions import when
df = df.withColumn("popularity_level",
    when(col("views") >= 3000, "热门")
    .when(col("views") >= 1000, "普通")
    .otherwise("冷门")
)

df.select("title", "views", "popularity_level").show()
```

### 4.2 withColumnRenamed — 重命名列

```python
# 重命名单列
df = df.withColumnRenamed("views", "view_count")
df = df.withColumnRenamed("publish_date", "pub_date")

# 链式重命名
df = df \
    .withColumnRenamed("views", "view_count") \
    .withColumnRenamed("publish_date", "pub_date") \
    .withColumnRenamed("category", "cat")
```

### 4.3 drop — 删除列

```python
# 删除单列
df = df.drop("likes")

# 删除多列
df = df.drop("likes", "comments")

# 删除不存在的列不会报错
df = df.drop("nonexistent_column")
```

### 4.4 alias — 列别名

```python
from pyspark.sql.functions import col, sum as _sum, count as _count

# 在 select 中使用别名
df.select(
    col("author").alias("作者"),
    col("views").alias("浏览量")
).show()

# 在聚合中使用别名
result = df.groupBy("author") \
    .agg(
        _sum("views").alias("总浏览量"),
        _count("*").alias("文章数")
    )

result.show()
```

## 5. 基本聚合

### 5.1 groupBy + agg

```python
from pyspark.sql.functions import sum as _sum, count as _count, \
    avg as _avg, max as _max, min as _min

# 单列分组
df.groupBy("author").count().show()

# 多列分组
df.groupBy("author", "category").count().show()

# 使用 agg 进行多种聚合
result = df.groupBy("author").agg(
    _count("*").alias("article_count"),           # 文章数
    _sum("views").alias("total_views"),           # 总浏览量
    _avg("views").alias("avg_views"),             # 平均浏览量
    _max("views").alias("max_views"),             # 最大浏览量
    _min("views").alias("min_views"),             # 最小浏览量
).orderBy("total_views", ascending=False)

result.show()
```

输出：
```
+------+-------------+-----------+---------+---------+---------+
|author|article_count|total_views|avg_views|max_views|min_views|
+------+-------------+-----------+---------+---------+---------+
|  张三|            3|      8300|2766.6667|     4500|     1500|
|  王五|            2|      5000|   2500.0|     3200|     1800|
|  李四|            1|       800|    800.0|      800|      800|
|  赵六|            1|       950|    950.0|      950|      950|
+------+-------------+-----------+---------+---------+---------+
```

### 5.2 简单聚合函数

```python
from pyspark.sql.functions import sum as _sum, count as _count, \
    avg as _avg, countDistinct

# 直接对整个 DataFrame 聚合
df.agg(
    _count("*").alias("total_articles"),
    _sum("views").alias("total_views"),
    _avg("views").alias("avg_views"),
    countDistinct("author").alias("unique_authors")
).show()

# 输出：
# +--------------+-----------+---------+---------------+
# |total_articles|total_views|avg_views|unique_authors|
# +--------------+-----------+---------+---------------+
# |             7|      14050|2007.1429|              4|
# +--------------+-----------+---------+---------------+
```

### 5.3 pivot — 透视表

```python
# 按作者和分类统计文章数，将分类转为列
pivot_result = df.groupBy("author") \
    .pivot("category") \
    .count()

pivot_result.show()
```

输出：
```
+------+----+----+--------+
|author|中间件|大数据|数据分析|
+------+----+----+--------+
|  张三|null|   3|    null|
|  王五|   1|   1|    null|
|  李四|null|   1|    null|
|  赵六|null|null|       1|
+------+----+----+--------+
```

## 6. SQL 查询方式

### 6.1 注册临时视图

```python
# 创建临时视图（Session 级别，Session 结束后消失）
df.createOrReplaceTempView("demos")

# 创建全局临时视图（跨 Session 可用，但需要带 global_temp 前缀访问）
df.createOrReplaceGlobalTempView("global_demos")

# 使用 SQL 查询
result = spark.sql("""
    SELECT author, COUNT(*) as article_count, SUM(views) as total_views
    FROM demos
    WHERE views > 1000
    GROUP BY author
    ORDER BY total_views DESC
""")
result.show()

# 访问全局临时视图
spark.sql("SELECT * FROM global_temp.global_demos").show()
```

### 6.2 完整 SQL 示例

```python
# 复杂 SQL 查询
result = spark.sql("""
    -- 业务数据分析报表
    WITH author_stats AS (
        SELECT
            author,
            COUNT(*) as article_count,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            MAX(views) as max_views
        FROM demos
        GROUP BY author
        HAVING COUNT(*) >= 1
    )
    SELECT
        author,
        article_count,
        total_views,
        ROUND(avg_views, 2) as avg_views,
        max_views,
        CASE
            WHEN total_views >= 5000 THEN '头部作者'
            WHEN total_views >= 2000 THEN '腰部作者'
            ELSE '新晋作者'
        END as author_level
    FROM author_stats
    ORDER BY total_views DESC
""")
result.show()
```

### 6.3 DataFrame API vs SQL 对比

```python
# 同样的查询，两种写法

# DataFrame API 方式
df_result = df.filter(col("views") > 1000) \
    .groupBy("author") \
    .agg(_sum("views").alias("total_views")) \
    .orderBy(col("total_views").desc())

# SQL 方式
sql_result = spark.sql("""
    SELECT author, SUM(views) as total_views
    FROM demos
    WHERE views > 1000
    GROUP BY author
    ORDER BY total_views DESC
""")

# 两种方式性能相同，底层都经过 Catalyst 优化器
# 选择哪种取决于个人偏好和场景复杂度
```

## 7. 查看方法

### 7.1 show() — 显示数据

```python
# 默认显示前 20 行
df.show()

# 显示前 5 行
df.show(5)

# 不截断列内容
df.show(truncate=False)

# 垂直显示（适合列数多的表）
df.show(3, vertical=True)

# 设置列显示最大字符数
df.show(5, truncate=30)
```

### 7.2 printSchema() — 显示 Schema

```python
df.printSchema()
```

输出：
```
root
 |-- title: string (nullable = true)
 |-- author: string (nullable = true)
 |-- views: long (nullable = true)
 |-- publish_date: string (nullable = true)
 |-- category: string (nullable = true)
```

### 7.3 describe() — 统计摘要

```python
# 对所有数值列计算统计信息
df.describe().show()

# 对指定列计算统计信息
df.describe("views").show()

# 输出：
# +-------+------------------+
# |summary|              views|
# +-------+------------------+
# |  count|                 7|
# |   mean| 2007.142857142857|
# | stddev|1320.6973922491137|
# |    min|               800|
# |    max|              4500|
# +-------+------------------+
```

### 7.4 其他查看方法

```python
# 列名列表
print(df.columns)
# ['title', 'author', 'views', 'publish_date', 'category']

# 数据类型
print(df.dtypes)
# [('title', 'string'), ('author', 'string'), ...]

# 行数
print(df.count())

# 前 N 行（返回 list）
print(df.take(3))

# 第一行（返回 Row）
print(df.first())

# 转换为 Pandas DataFrame（注意：数据量不能太大）
pdf = df.toPandas()
print(pdf)

# 查看 DataFrame 的执行计划
df.explain()
df.explain(True)   # 详细执行计划

# 判断 DataFrame 是否为空
print(df.rdd.isEmpty())

# 列信息
from pyspark.sql.functions import countDistinct
# 某列的去重值数量
df.select(countDistinct("author")).show()
```

## 8. 实战：创建数据平台 DataFrame 并查询

### 8.1 完整示例

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, \
    count as _count, when, length, upper, month as _month

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("DemoAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# ============ 准备数据 ============
demo_data = [
    ("Spark入门教程", "张三", 1500, "2024-01-15", "大数据"),
    ("Spark SQL详解", "张三", 2800, "2024-02-28", "大数据"),
    ("Spark性能调优", "张三", 4500, "2024-05-18", "大数据"),
    ("Hadoop核心原理", "李四", 2300, "2024-02-20", "大数据"),
    ("Hive数据仓库", "李四", 800, "2024-03-10", "大数据"),
    ("Flink流处理入门", "王五", 3200, "2024-04-05", "流计算"),
    ("Kafka消息队列", "王五", 1800, "2024-06-22", "中间件"),
    ("Python数据分析", "赵六", 950, "2024-07-30", "数据分析"),
    ("Docker容器化部署", "赵六", 1200, "2024-08-12", "运维"),
    ("Kubernetes实战", "王五", 2600, "2024-09-01", "运维"),
]

columns = ["title", "author", "views", "publish_date", "category"]
df = spark.createDataFrame(demo_data, columns)

# ============ 数据增强 ============
df = df.withColumn("title_length", length(col("title")))
df = df.withColumn("popularity",
    when(col("views") >= 3000, "热门")
    .when(col("views") >= 1500, "普通")
    .otherwise("冷门")
)

# ============ 注册临时视图 ============
df.createOrReplaceTempView("demos")

# ============ 查询 1：各作者文章统计 ============
print("=== 各作者文章统计 ===")
author_stats = df.groupBy("author").agg(
    _count("*").alias("文章数"),
    _sum("views").alias("总浏览量"),
    _avg("views").alias("平均浏览量")
).orderBy("总浏览量", ascending=False)
author_stats.show()

# ============ 查询 2：各分类文章数 ============
print("=== 各分类文章数 ===")
category_stats = df.groupBy("category") \
    .count() \
    .orderBy("count", ascending=False)
category_stats.show()

# ============ 查询 3：热门文章（浏览量 >= 3000） ============
print("=== 热门文章 ===")
hot_articles = df.filter(col("popularity") == "热门") \
    .select("title", "author", "views", "category")
hot_articles.show()

# ============ 查询 4：张三的文章 ============
print("=== 张三的文章 ===")
zhangsan_articles = df.filter(col("author") == "张三") \
    .select("title", "views", "publish_date")
zhangsan_articles.show()

# ============ 查询 5：SQL 方式 - 综合分析 ============
print("=== SQL 综合分析 ===")
analysis = spark.sql("""
    SELECT
        category,
        COUNT(*) as article_count,
        SUM(views) as total_views,
        ROUND(AVG(views), 2) as avg_views,
        MAX(views) as max_views,
        MIN(views) as min_views
    FROM demos
    GROUP BY category
    ORDER BY total_views DESC
""")
analysis.show()

# ============ 查询 6：浏览量最高的 3 篇文章 ============
print("=== Top 3 文章 ===")
df.orderBy("views", ascending=False).limit(3).show()

# ============ 查看执行计划 ============
print("=== 执行计划 ===")
df.filter(col("views") > 1000).groupBy("author").sum("views").explain()

# 停止 SparkSession
spark.stop()
```

### 8.2 预期输出

```
=== 各作者文章统计 ===
+------+--------+-----------+------------+
|author|文章数  |总浏览量   |平均浏览量   |
+------+--------+-----------+------------+
|  张三|       3|      8800|2933.33333333|
|  王五|       3|      7600|2533.33333333|
|  李四|       2|      3100|      1550.0|
|  赵六|       2|      2150|      1075.0|
+------+--------+-----------+------------+

=== 各分类文章数 ===
+--------+-----+
|category|count|
+--------+-----+
|    大数据|    5|
|    流计算|    1|
|    中间件|    1|
|  数据分析|    1|
|      运维|    2|
+--------+-----+

=== 热门文章 ===
+----------------+------+-----+--------+
|           title|author|views|category|
+----------------+------+-----+--------+
| Spark性能调优  |  张三| 4500|    大数据|
|Flink流处理入门 |  王五| 3200|    流计算|
| Spark SQL详解  |  张三| 2800|    大数据|
+----------------+------+-----+--------+
```

## 9. 总结

```
┌──────────────────────────────────────────────────────────┐
│                Spark SQL 基础知识总结                      │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. SparkSession 是统一入口，替代了旧的 SQLContext          │
│                                                          │
│  2. 创建 DataFrame 的 5 种方式：                          │
│     - Python 集合 (createDataFrame)                      │
│     - Row 对象 + Schema                                  │
│     - RDD (toDF)                                         │
│     - 文件 (read.csv/json/parquet)                       │
│     - 数据库 (JDBC / Hive)                               │
│                                                          │
│  3. 基本操作：select, filter, where, orderBy, limit       │
│                                                          │
│  4. 列操作：withColumn, withColumnRenamed, drop           │
│                                                          │
│  5. 聚合：groupBy + agg，支持 sum/avg/count/max/min       │
│                                                          │
│  6. SQL 查询：createOrReplaceTempView + spark.sql()      │
│                                                          │
│  7. 查看方法：show, printSchema, describe, explain        │
│                                                          │
│  8. DataFrame API 和 SQL 性能相同，都经过 Catalyst 优化    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[04-Spark-SQL进阶.md](04-Spark-SQL进阶.md) — Spark SQL 进阶，学习 Join、窗口函数、CTE 等高级特性。
