# PySpark 实战

## 1. PySpark vs Scala Spark

### 1.1 对比分析

```
PySpark vs Scala Spark：

┌──────────────────┬──────────────────┬──────────────────────┐
│ 特性              │ PySpark          │ Scala Spark           │
├──────────────────┼──────────────────┼──────────────────────┤
│ 语言              │ Python           │ Scala                 │
│ API 完整度        │ 完整             │ 完整 + 底层 API       │
│ DataFrame API     │ 完整支持         │ 完整支持             │
│ Dataset API       │ 不支持           │ 完整支持             │
│ 类型安全          │ 运行时           │ 编译时               │
│ 性能              │ 略低（序列化开销）│ 最佳                 │
│ 生态集成          │ Pandas/NumPy/ML  │ JVM 生态             │
│ 学习曲线          │ 低               │ 中等                 │
│ 开发效率          │ 高               │ 中等                 │
│ UDF 性能          │ 普通较慢         │ 快                   │
│ Pandas UDF        │ 快（Arrow 优化） │ 不适用               │
│ 部署              │ 需要 Python 环境  │ 单一 JAR 包          │
│ 社区/招聘          │ 数据分析师/工程师 │ 大数据工程师         │
└──────────────────┴──────────────────┴──────────────────────┘
```

### 1.2 性能差异原因

```
PySpark 性能开销来源：

┌──────────────────────────────────────────────────────────┐
│  Python 进程                    JVM 进程                 │
│                                                          │
│  ┌──────────┐    Socket     ┌──────────────────────┐   │
│  │ Python   │ <===========> │   Spark Executor     │   │
│  │ Worker   │   序列化/      │   (JVM)             │   │
│  │ (Py4J)   │   反序列化     │                      │   │
│  │          │    开销         │  实际计算在这里执行   │   │
│  │ UDF 在   │               │                      │   │
│  │ 这里运行  │               │  DataFrame API 在    │   │
│  │          │               │  JVM 中执行（无开销） │   │
│  └──────────┘               └──────────────────────┘   │
│                                                          │
│  结论：                                                   │
│  - DataFrame/SQL API：PySpark 和 Scala 性能接近          │
│  - Python UDF：有 Python-JVM 通信开销，性能较差         │
│  - Pandas UDF：使用 Arrow 优化，性能接近 Scala           │
│  - RDD map：每个元素都有序列化开销                        │
│                                                          │
│  优化建议：                                               │
│  - 优先使用 DataFrame/SQL API（而非 RDD）                │
│  - 优先使用内置函数（而非 Python UDF）                    │
│  - 复杂逻辑使用 Pandas UDF（而非普通 UDF）               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

## 2. PySpark 数据处理完整流程

### 2.1 标准流程

```
PySpark 数据处理标准流程：

┌──────────────────────────────────────────────────────────┐
│                                                          │
│  1. 初始化                                                │
│     └── 创建 SparkSession                                │
│                                                          │
│  2. 读取数据                                              │
│     └── spark.read.csv/json/parquet/jdbc/hive            │
│                                                          │
│  3. 数据探索                                              │
│     └── printSchema/show/describe/summary                │
│                                                          │
│  4. 数据清洗                                              │
│     └── 过滤空值/异常值/类型转换/去重                    │
│                                                          │
│  5. 数据转换                                              │
│     └── 新增列/Join/聚合/窗口函数                       │
│                                                          │
│  6. 数据分析                                              │
│     └── 统计/排名/趋势分析                              │
│                                                          │
│  7. 写出结果                                              │
│     └── write.parquet/csv/json/saveAsTable               │
│                                                          │
│  8. 释放资源                                              │
│     └── spark.stop()                                    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 2.2 完整示例

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, to_date, \
    sum as _sum, avg as _avg, count as _count, max as _max, \
    countDistinct, round as _round, desc, asc, lower, split, explode, \
    length, regexp_replace, month as _month, year as _year, \
    dayofweek as _dayofweek
from pyspark.sql.types import IntegerType, DoubleType

def create_spark_session(app_name="DemoAnalysis"):
    """创建 SparkSession 的标准模板"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_data(spark, path):
    """读取数据的标准模板"""
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True,
        encoding="UTF-8",
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )
    print(f"读取数据 {df.count()} 条，{len(df.columns)} 列")
    return df

def explore_data(df):
    """数据探索的标准模板"""
    print("\n=== Schema ===")
    df.printSchema()

    print("\n=== 前 5 行 ===")
    df.show(5, truncate=False)

    print("\n=== 统计摘要 ===")
    df.describe().show()

    print("\n=== 缺失值统计 ===")
    total = df.count()
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            print(f"  {c}: {null_count} ({null_count/total*100:.1f}%)")

def clean_data(df):
    """数据清洗的标准模板"""
    # 去除空行
    df = df.na.drop(how="all")

    # 去除字符串列的前后空格
    for c in df.dtypes:
        if c[1] == "string":
            df = df.withColumn(c[0], trim(col(c[0])))

    # 填充缺失值
    df = df.fillna({"views": 0, "likes": 0})

    # 处理异常值
    df = df.withColumn("views",
        when(col("views") < 0, 0).otherwise(col("views"))
    )

    # 去重
    df = df.dropDuplicates(["title", "author"])

    return df

# 主流程
if __name__ == "__main__":
    spark = create_spark_session()

    try:
        # 读取数据（这里使用示例数据代替文件读取）
        data = [
            ("Spark入门", "张三", 1500, 50, "2024-01-15", "spark"),
            ("Spark SQL", "张三", 2800, 120, "2024-02-28", "spark"),
            ("Spark调优", "张三", 4500, 200, "2024-05-18", "spark"),
            ("Hadoop", "李四", 2300, 80, "2024-02-20", "hadoop"),
            ("Hive", "李四", 800, 30, "2024-03-10", "hive"),
            ("Flink", "王五", 3200, 150, "2024-04-05", "flink"),
            ("Kafka", "王五", 1800, 60, "2024-06-22", "kafka"),
            ("Python", "赵六", 950, 40, "2024-07-30", "python"),
        ]

        df = spark.createDataFrame(data,
            ["title", "author", "views", "likes", "publish_date", "category"])

        explore_data(df)
        df_clean = clean_data(df)

        # 分析
        result = df_clean.groupBy("author").agg(
            _count("*").alias("文章数"),
            _sum("views").alias("总浏览量"),
            _round(_avg("views"), 2).alias("平均浏览量")
        ).orderBy(desc("总浏览量"))

        result.show()

    finally:
        spark.stop()
```

## 3. 读写 CSV、JSON、Parquet

### 3.1 CSV

```python
# 写入 CSV
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .option("sep", ",") \
    .option("quote", '"') \
    .option("escape", "\\") \
    .option("nullValue", "") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .csv("output/demos.csv")

# 读取 CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("sep", ",") \
    .option("nullValue", "NA") \
    .option("dateFormat", "yyyy-MM-dd") \
    .csv("input/demos.csv")

# 指定 Schema（避免推断开销，生产环境推荐）
from pyspark.sql.types import *

schema = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), False),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("publish_date", DateType(), True),
])

df = spark.read.schema(schema).option("header", "true").csv("input/demos.csv")

# 读取多个 CSV 文件
df = spark.read.csv("input/demos_*.csv", header=True, inferSchema=True)

# 读取整个目录
df = spark.read.csv("input/demos_dir/", header=True, inferSchema=True)
```

### 3.2 JSON

```python
# 写入 JSON
df.write.mode("overwrite").json("output/demos.json")

# 读取 JSON（自动推断 Schema）
df = spark.read.json("input/demos.json")

# 读取 JSON（指定 Schema）
df = spark.read.schema(schema).json("input/demos.json")

# 读取多行 JSON（每行一个 JSON 对象）
df = spark.read.option("multiline", "true").json("input/demos_array.json")

# JSON Lines 格式（每行一个 JSON，推荐）
df = spark.read.json("input/demos.jsonl")

# 读取 JSON 时处理嵌套字段
df.select("author.name", "author.city", "tags[0]").show()
```

### 3.3 Parquet

```python
# 写入 Parquet
df.write.mode("overwrite").parquet("output/demos.parquet")

# 写入时指定压缩
df.write.mode("overwrite") \
    .option("compression", "snappy") \  # snappy（默认，速度快）
    .parquet("output/demos.parquet")

# 其他压缩选项：gzip, lzo, lz4, zstd
# snappy：速度快，压缩比中等（推荐）
# gzip：压缩比高，速度慢
# zstd：压缩比和速度均衡（Spark 3.0+）

# 读取 Parquet
df = spark.read.parquet("output/demos.parquet")

# 读取时指定列（列裁剪，性能优化）
df = spark.read.parquet("output/demos.parquet") \
    .select("title", "author", "views")

# 读取时过滤（谓词下推，性能优化）
df = spark.read.parquet("output/demos.parquet") \
    .filter(col("views") > 1000)

# 读取分区目录
df = spark.read.parquet("output/demos/year=2024/month=05")

# 合并小文件为单个 Parquet
df.coalesce(1).write.mode("overwrite").parquet("output/demos_single.parquet")
```

### 3.4 文件格式对比

```
┌──────────────┬──────────┬──────────┬──────────┬──────────┐
│ 特性          │ CSV      │ JSON     │ Parquet  │ ORC      │
├──────────────┼──────────┼──────────┼──────────┼──────────┤
│ 可读性        │ 好       │ 好       │ 差       │ 差       │
│ 压缩比        │ 低       │ 低       │ 高       │ 很高     │
│ 读取速度      │ 慢       │ 慢       │ 快       │ 快       │
│ 列裁剪        │ 不支持   │ 不支持   │ 支持     │ 支持     │
│ 谓词下推      │ 不支持   │ 不支持   │ 支持     │ 支持     │
│ Schema 支持   │ 无       │ 有       │ 有       │ 有       │
│ 分区支持      │ 支持     │ 支持     │ 支持     │ 支持     │
│ 嵌套数据      │ 不支持   │ 支持     │ 支持     │ 支持     │
│ Spark 生态   │ 一般     │ 一般     │ 最佳     │ 好       │
│ Hive 生态    │ 好       │ 一般     │ 好       │ 最佳     │
│ 推荐场景      │ 外部交换 │ API 数据│ Spark   │ Hive     │
│              │          │          │ 内部存储│ 内部存储 │
└──────────────┴──────────┴──────────┴──────────┴──────────┘
```

## 4. UDF（用户自定义函数）

### 4.1 普通 Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, ArrayType

# 定义 UDF
@udf(StringType())
def classify_popularity(views):
    """根据浏览量分类文章热度"""
    if views is None:
        return "未知"
    elif views >= 3000:
        return "热门"
    elif views >= 1000:
        return "普通"
    else:
        return "冷门"

# 使用 UDF
df = df.withColumn("popularity", classify_popularity("views"))
df.select("title", "views", "popularity").show()

# 注册 UDF 以便在 SQL 中使用
spark.udf.register("classify_popularity", classify_popularity)

spark.sql("""
    SELECT title, views, classify_popularity(views) as popularity
    FROM demos
""").show()

# 返回复杂类型
@udf(ArrayType(StringType()))
def extract_tags(tags_str):
    """提取标签列表"""
    if tags_str is None:
        return []
    return [t.strip() for t in tags_str.split(",") if t.strip()]

df.withColumn("tag_list", extract_tags("tags")).show()
```

### 4.2 Pandas UDF（推荐）

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType

# ===== 标量 Pandas UDF（一对一映射） =====
@pandas_udf(StringType())
def classify_popularity_pandas(views: pd.Series) -> pd.Series:
    """使用 Pandas 向量化操作，性能远超普通 UDF"""
    return pd.cut(
        views,
        bins=[-float('inf'), 1000, 3000, float('inf')],
        labels=["冷门", "普通", "热门"]
    ).astype(str)

df.withColumn("popularity", classify_popularity_pandas("views")).show()

# ===== 分组聚合 Pandas UDF =====
from pyspark.sql import functions as F

# 定义 schema
result_schema = "author string, total_views long, avg_views double, level string"

@pandas_udf(result_schema)
def analyze_author(df: pd.DataFrame) -> pd.DataFrame:
    """对每个作者分组进行复杂分析"""
    total = df["views"].sum()
    avg = df["views"].mean()
    level = "头部" if total >= 5000 else ("腰部" if total >= 2000 else "新晋")
    return pd.DataFrame({
        "author": [df["author"].iloc[0]],
        "total_views": [total],
        "avg_views": [avg],
        "level": [level]
    })

result = df.groupBy("author").apply(analyze_author)
result.show()

# ===== 窗口聚合 Pandas UDF =====
from pyspark.sql.window import Window

@pandas_udf(DoubleType())
def moving_avg(views: pd.Series) -> pd.Series:
    """计算移动平均"""
    return views.rolling(window=3, min_periods=1).mean()

w = Window.partitionBy("author").orderBy("publish_date")
df = df.withColumn("views_ma", moving_avg("views").over(w))
```

### 4.3 RDD UDF（map 函数）

```python
# 当 DataFrame API 不够灵活时，可以降级到 RDD
from pyspark.sql import Row

# DataFrame -> RDD -> 自定义处理 -> DataFrame
def process_row(row):
    """自定义行处理逻辑"""
    title = row.title.strip() if row.title else "无标题"
    views = max(row.views, 0) if row.views else 0
    return Row(
        title=title,
        author=row.author,
        views=views,
        title_length=len(title)
    )

rdd_result = df.rdd.map(process_row)
df_result = spark.createDataFrame(rdd_result)
df_result.show()
```

### 4.4 UDF 性能对比

```
UDF 性能对比（处理 100 万行数据）：

┌──────────────────────┬──────────────┬──────────────┐
│ UDF 类型              │ 执行时间     │ 说明          │
├──────────────────────┼──────────────┼──────────────┤
│ Spark 内置函数        │ 5 秒         │ 最快          │
│ Pandas UDF           │ 8 秒         │ 接近内置函数   │
│ 普通 Python UDF      │ 60 秒        │ 很慢          │
│ RDD map              │ 80 秒        │ 最慢          │
└──────────────────────┴──────────────┴──────────────┘

选择建议：
1. 优先使用 Spark 内置函数
2. 内置函数不支持时，使用 Pandas UDF
3. 需要复杂逻辑时，使用普通 UDF
4. 只有在 DataFrame API 完全不支持时，才用 RDD map
```

## 5. 与 Pandas 互转

### 5.1 toPandas

```python
# Spark DataFrame -> Pandas DataFrame
pdf = df.toPandas()

# 注意事项：
# 1. 所有数据会收集到 Driver 内存
# 2. 数据量不能太大（Driver 内存限制）
# 3. 大约需要 Spark 数据 2-3 倍的内存（序列化开销）
# 4. 支持 Arrow 优化（减少内存使用）

# 启用 Arrow 优化
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 指定类型转换
pdf = df.toPandas()

# 只取部分列和行，减少内存使用
pdf = df.select("title", "author", "views") \
    .limit(10000) \
    .toPandas()
```

### 5.2 createDataFrame

```python
# Pandas DataFrame -> Spark DataFrame
import pandas as pd

pdf = pd.DataFrame({
    "title": ["Spark入门", "Hadoop教程"],
    "author": ["张三", "李四"],
    "views": [1500, 2300],
})

# 方式 1：从 Pandas 创建
df = spark.createDataFrame(pdf)
df.show()

# 方式 2：从 Pandas 创建（指定 Schema）
from pyspark.sql.types import *
schema = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("views", IntegerType(), True),
])
df = spark.createDataFrame(pdf, schema)

# 方式 3：启用 Arrow 优化（推荐大数据量时使用）
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
df = spark.createDataFrame(pdf)

# 从 list 创建
df = spark.createDataFrame([
    ("Spark入门", "张三", 1500),
    ("Hadoop教程", "李四", 2300),
], ["title", "author", "views"])
```

### 5.3 Arrow 优化

```
Arrow 优化的原理：

不使用 Arrow（传统方式）：
  Spark (JVM) → 序列化为 Java 对象 → 通过 Py4J 传输 →
  Python 反序列化为 Python 对象 → 转换为 Pandas

使用 Arrow：
  Spark (JVM) → Arrow 格式（列式内存格式）→ 零拷贝传输 →
  Python 直接读取 Arrow → 转换为 Pandas

  优势：减少序列化/反序列化开销，降低内存使用

启用 Arrow：
  spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

前提条件：
  pip install pyarrow
```

## 6. Spark + SQL 实战案例：业务数据分析

### 6.1 完整实战

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, to_date, \
    sum as _sum, avg as _avg, count as _count, max as _max, min as _min, \
    countDistinct, round as _round, desc, asc, lower, split, explode, \
    length, regexp_replace, month as _month, year as _year, \
    lit, collect_list, size, array_distinct, array_join
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DateType, \
    StructType, StructField, ArrayType
from pyspark.sql.functions import pandas_udf
import pandas as pd

def main():
    # ============ 1. 创建 SparkSession ============
    spark = SparkSession.builder \
        .appName("DemoDataAnalysis") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ============ 2. 准备数据 ============
        demo_data = [
            (1, "  Spark入门教程  ", " 张三", 1500, 50, " spark,大数据 ", "2024-01-15"),
            (2, "  Spark SQL详解  ", " 张三", 2800, 120, " spark,sql ", "2024-02-28"),
            (3, "Spark性能调优", "张三", 4500, 200, "spark,调优", "2024-05-18"),
            (4, "Hadoop核心原理", "李四", 2300, 80, "hadoop,大数据", "2024-02-20"),
            (5, "Hive数据仓库", "李四", 800, 30, "hive,数据仓库", "2024-03-10"),
            (6, "Flink流处理", "王五", 3200, 150, "flink,流计算", "2024-04-05"),
            (7, "Kafka消息队列", "王五", 1800, 60, "kafka,中间件", "2024-06-22"),
            (8, "Python数据分析", "赵六", 950, 40, "python,数据分析", "2024-07-30"),
            (9, "Docker容器化", "赵六", 1200, 55, "docker,运维", "2024-08-12"),
            (10, "K8s实战", "王五", 2600, 100, "k8s,运维", "2024-09-01"),
            (11, "Spark MLlib", "张三", 3800, 170, "spark,机器学习", "2024-10-15"),
            (12, "HBase入门", "李四", 600, 20, "hbase,nosql", "2024-11-20"),
        ]

        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("views", IntegerType(), True),
            StructField("likes", IntegerType(), True),
            StructField("tags", StringType(), True),
            StructField("publish_date", StringType(), True),
        ])

        df = spark.createDataFrame(demo_data, schema)
        print(f"原始数据: {df.count()} 条")

        # ============ 3. 数据清洗 ============
        df_clean = df \
            .withColumn("title", trim(col("title"))) \
            .withColumn("author", trim(col("author"))) \
            .withColumn("tags", trim(regexp_replace(col("tags"), r"\s+", ""))) \
            .withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd")) \
            .filter(col("title").isNotNull() & (col("title") != "")) \
            .filter(col("publish_date").isNotNull())

        # 新增衍生列
        df_clean = df_clean \
            .withColumn("year", _year(col("publish_date"))) \
            .withColumn("month", _month(col("publish_date"))) \
            .withColumn("engagement", col("views") + col("likes")) \
            .withColumn("popularity",
                when(col("views") >= 3000, "热门")
                .when(col("views") >= 1000, "普通")
                .otherwise("冷门")
            ) \
            .withColumn("title_length", length(col("title")))

        print(f"清洗后数据: {df_clean.count()} 条")

        # ============ 4. 注册视图 ============
        df_clean.createOrReplaceTempView("demos")

        # ============ 5. 分析 1：作者综合统计 ============
        print("\n=== 作者综合统计 ===")
        author_stats = spark.sql("""
            SELECT
                author,
                COUNT(*) as article_count,
                SUM(views) as total_views,
                ROUND(AVG(views), 2) as avg_views,
                MAX(views) as max_views,
                SUM(engagement) as total_engagement,
                ROUND(AVG(engagement), 2) as avg_engagement,
                COUNT(DISTINCT month) as active_months,
                CASE
                    WHEN SUM(views) >= 8000 THEN '头部作者'
                    WHEN SUM(views) >= 3000 THEN '腰部作者'
                    ELSE '新晋作者'
                END as author_level
            FROM demos
            GROUP BY author
            ORDER BY total_views DESC
        """)
        author_stats.show()

        # ============ 6. 分析 2：月度趋势 ============
        print("\n=== 月度发文趋势 ===")
        monthly_trend = spark.sql("""
            SELECT
                year,
                month,
                COUNT(*) as article_count,
                SUM(views) as total_views,
                ROUND(AVG(views), 2) as avg_views,
                LAG(SUM(views), 1) OVER (ORDER BY year, month) as prev_month_views,
                ROUND(
                    (SUM(views) - LAG(SUM(views), 1) OVER (ORDER BY year, month))
                    * 100.0 / LAG(SUM(views), 1) OVER (ORDER BY year, month),
                    2
                ) as growth_rate
            FROM demos
            GROUP BY year, month
            ORDER BY year, month
        """)
        monthly_trend.show()

        # ============ 7. 分析 3：标签分析 ============
        print("\n=== 标签使用统计 ===")
        tag_analysis = df_clean \
            .withColumn("tag_array", split(lower(col("tags")), ",")) \
            .withColumn("single_tag", explode(col("tag_array"))) \
            .filter(col("single_tag") != "") \
            .groupBy("single_tag") \
            .agg(
                _count("*").alias("usage_count"),
                _sum("views").alias("total_views"),
                _round(_avg("views"), 2).alias("avg_views"),
                collect_list("title").alias("articles")
            ) \
            .orderBy(desc("usage_count"))

        tag_analysis.select("single_tag", "usage_count", "total_views", "avg_views").show()

        # ============ 8. 分析 4：文章排名 ============
        print("\n=== 文章浏览量排名 ===")
        article_rank = spark.sql("""
            SELECT
                title,
                author,
                views,
                likes,
                engagement,
                popularity,
                RANK() OVER (ORDER BY views DESC) as overall_rank,
                RANK() OVER (PARTITION BY author ORDER BY views DESC) as author_rank
            FROM demos
            ORDER BY overall_rank
        """)
        article_rank.show()

        # ============ 9. 分析 5：作者文章标签多样性 ============
        print("\n=== 作者标签多样性 ===")
        author_tags = df_clean \
            .withColumn("tag_array", split(lower(col("tags")), ",")) \
            .groupBy("author") \
            .agg(
                array_distinct(
                    collect_list(col("tag_array"))
                ).alias("all_tags")
            ) \
            .withColumn("tag_count", size(col("all_tags")))

        author_tags.select("author", "tag_count", "all_tags").show(truncate=False)

        # ============ 10. 使用 Pandas UDF 优化 ============
        print("\n=== Pandas UDF 示例 ===")

        @pandas_udf("double")
        def engagement_score(views: pd.Series, likes: pd.Series) -> pd.Series:
            """计算综合互动分数"""
            return (views * 0.7 + likes * 10).round(2)

        df_with_score = df_clean.withColumn(
            "engagement_score",
            engagement_score("views", "likes")
        )

        df_with_score.select("title", "views", "likes", "engagement_score") \
            .orderBy(desc("engagement_score")).show(5)

        # ============ 11. 写出结果 ============
        author_stats.write.mode("overwrite") \
            .option("header", "true") \
            .csv("output/author_stats.csv")

        df_with_score.write.mode("overwrite") \
            .parquet("output/demos_with_score.parquet")

        print("\n分析完成！结果已写入 output/ 目录")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## 7. PySpark 常见错误和解决方案

### 7.1 错误排查表

```
┌──────────────────────────────────────────────────────────────────┐
│ 错误信息                        │ 原因和解决方案                   │
├─────────────────────────────────┼────────────────────────────────┤
│ Py4JError                       │ Py4J 通信异常                   │
│                                 │ 检查 Java/Python 版本兼容性     │
│                                 │ 确认 PySpark 和 Spark 版本一致   │
├─────────────────────────────────┼────────────────────────────────┤
│ AnalysisException               │ SQL 语法错误或列不存在          │
│                                 │ 检查列名拼写、表名是否正确      │
│                                 │ 使用 printSchema() 查看 Schema  │
├─────────────────────────────────┼────────────────────────────────┤
│ IllegalArgumentException        │ 类型不匹配                      │
│                                 │ 检查列类型是否一致（Join Key）  │
│                                 │ 使用 cast() 转换类型            │
├─────────────────────────────────┼────────────────────────────────┤
│ OutOfMemoryError                │ 内存不足                        │
│                                 │ 不要 collect() 大数据集         │
│                                 │ 增大 driver-memory/executor-mem │
├─────────────────────────────────┼────────────────────────────────┤
│ PythonWorkerFailed              │ Python Worker 崩溃             │
│                                 │ UDF 中有未捕获的异常            │
│                                 │ 检查 UDF 代码逻辑               │
│                                 │ 检查 PYSPARK_PYTHON 配置        │
├─────────────────────────────────┼────────────────────────────────┤
│ FileNotFoundException           │ 文件路径错误                    │
│                                 │ 检查路径是否存在                │
│                                 │ 本地文件用 file:/// 前缀        │
│                                 │ HDFS 文件用 hdfs:/// 前缀       │
├─────────────────────────────────┼────────────────────────────────┤
│ Container killed by YARN       │ YARN 容器被杀                   │
│                                 │ 内存超限（物理内存 > 申请内存）  │
│                                 │ 调大 executor-memory           │
│                                 │ 设置 spark.yarn.executor       │
│                                 │   .memoryOverhead              │
├─────────────────────────────────┼────────────────────────────────┤
│ Task failed / lost              │ Task 失败                       │
│                                 │ 数据倾斜（某些 Task 特别慢）    │
│                                 │ Executor 内存不足               │
│                                 │ 网络问题                        │
├─────────────────────────────────┼────────────────────────────────┤
│ SerializationException          │ 序列化失败                      │
│                                 │ UDF 返回了不可序列化的对象      │
│                                 │ 使用 Kryo 序列化               │
│                                 │ 检查 UDF 返回类型               │
├─────────────────────────────────┼────────────────────────────────┤
│ SparkUpgradeException           │ 版本升级兼容问题                │
│                                 │ 检查 Spark 版本是否兼容         │
│                                 │ 设置 spark.sql.legacy 配置      │
└─────────────────────────────────┴────────────────────────────────┘
```

### 7.2 常见陷阱

```python
# 陷阱 1：在 UDF 中使用非序列化对象
# 错误：UDF 中引用了不可序列化的对象
model = load_ml_model()  # 假设这是不可序列化的对象

@udf(StringType())
def predict(features):
    return model.predict(features)  # 会报错！

# 正确：使用广播变量
broadcast_model = spark.sparkContext.broadcast(model)

@udf(StringType())
def predict(features):
    return broadcast_model.value.predict(features)

# 陷阱 2：collect() 后的 DataFrame 不能再使用
df1 = spark.read.parquet("data.parquet")
result = df1.collect()  # 触发计算
df2 = df1.filter(...)   # df1 已经被 collect，可以继续使用
# 但 result 是一个 list，不是 DataFrame

# 陷阱 3：在循环中创建多个 SparkSession
# 错误：每个循环创建一个新的 SparkSession
for i in range(10):
    spark = SparkSession.builder.appName(f"app_{i}").getOrCreate()

# 正确：创建一个 SparkSession，在整个程序中复用
spark = SparkSession.builder.appName("main_app").getOrCreate()

# 陷阱 4：DataFrame 引用被覆盖
df = spark.read.parquet("data1.parquet")
df = df.filter(...)  # df 被覆盖，原始引用丢失
# 如果需要原始 df，使用不同变量名
df_raw = spark.read.parquet("data1.parquet")
df_filtered = df_raw.filter(...)

# 陷阱 5：忘记 cache 后的 Action
df = spark.read.parquet("large_data.parquet").cache()
# df 不会缓存！需要先触发 Action
df.count()  # 现在缓存了
```

## 8. 实战：从零搭建业务数据分析流水线

```python
#!/usr/bin/env python3
"""
业务数据分析流水线
功能：读取原始数据 → 清洗 → 分析 → 输出结果
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os
import sys


class DemoDataPipeline:
    """业务数据分析流水线"""

    def __init__(self, app_name="DemoPipeline"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def ingest(self, data_path=None, data=None):
        """数据接入"""
        if data_path:
            self.df = self.spark.read.csv(
                data_path, header=True, inferSchema=True, encoding="UTF-8"
            )
        elif data:
            self.df = self.spark.createDataFrame(data, [
                "id", "title", "author", "views", "likes", "tags", "publish_date"
            ])
        else:
            raise ValueError("请提供 data_path 或 data")

        print(f"[INGEST] 读取数据: {self.df.count()} 条")
        return self

    def clean(self):
        """数据清洗"""
        self.df = self.df \
            .filter(col("title").isNotNull() & (trim(col("title")) != "")) \
            .withColumn("title", trim(col("title"))) \
            .withColumn("author", trim(col("author"))) \
            .withColumn("tags", trim(regexp_replace(col("tags"), r"\s+", ""))) \
            .withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd")) \
            .withColumn("views", when(col("views").isNull() | (col("views") < 0), 0)
                       .otherwise(col("views"))) \
            .withColumn("likes", when(col("likes").isNull() | (col("likes") < 0), 0)
                       .otherwise(col("likes")))

        print(f"[CLEAN] 清洗后数据: {self.df.count()} 条")
        return self

    def enrich(self):
        """数据增强"""
        self.df = self.df \
            .withColumn("year", year(col("publish_date"))) \
            .withColumn("month", month(col("publish_date"))) \
            .withColumn("engagement", col("views") + col("likes") * 5) \
            .withColumn("popularity",
                when(col("views") >= 3000, "hot")
                .when(col("views") >= 1000, "normal")
                .otherwise("cold")
            ) \
            .withColumn("title_length", length(col("title")))

        print("[ENRICH] 数据增强完成")
        return self

    def analyze(self):
        """数据分析"""
        self.df.createOrReplaceTempView("demos")

        # 分析 1：作者排名
        self.author_ranking = self.spark.sql("""
            SELECT author,
                   COUNT(*) as articles,
                   SUM(views) as total_views,
                   ROUND(AVG(views), 0) as avg_views,
                   SUM(engagement) as total_engagement
            FROM demos
            GROUP BY author
            ORDER BY total_views DESC
        """)

        # 分析 2：月度趋势
        self.monthly_trend = self.spark.sql("""
            SELECT year, month,
                   COUNT(*) as articles,
                   SUM(views) as total_views
            FROM demos
            GROUP BY year, month
            ORDER BY year, month
        """)

        # 分析 3：热门标签
        self.tag_stats = self.df \
            .withColumn("tag", explode(split(lower(col("tags")), ","))) \
            .filter(col("tag") != "") \
            .groupBy("tag") \
            .agg(count("*").alias("count"), sum("views").alias("views")) \
            .orderBy(desc("count"))

        print("[ANALYZE] 分析完成")
        return self

    def output(self, output_dir="output"):
        """输出结果"""
        os.makedirs(output_dir, exist_ok=True)

        self.author_ranking.write.mode("overwrite") \
            .option("header", "true").csv(f"{output_dir}/author_ranking")

        self.monthly_trend.write.mode("overwrite") \
            .option("header", "true").json(f"{output_dir}/monthly_trend")

        self.df.write.mode("overwrite").parquet(f"{output_dir}/demos_enriched")

        print(f"[OUTPUT] 结果已写入 {output_dir}/")
        return self

    def show_results(self):
        """展示结果"""
        print("\n=== 作者排名 ===")
        self.author_ranking.show()

        print("\n=== 月度趋势 ===")
        self.monthly_trend.show()

        print("\n=== 热门标签 ===")
        self.tag_stats.show(10)

        return self

    def close(self):
        """释放资源"""
        self.spark.stop()
        print("[CLOSE] SparkSession 已关闭")


if __name__ == "__main__":
    # 使用示例
    data = [
        (1, "Spark入门", "张三", 1500, 50, "spark,大数据", "2024-01-15"),
        (2, "Spark SQL", "张三", 2800, 120, "spark,sql", "2024-02-28"),
        (3, "Spark调优", "张三", 4500, 200, "spark,调优", "2024-05-18"),
        (4, "Hadoop", "李四", 2300, 80, "hadoop,大数据", "2024-02-20"),
        (5, "Hive", "李四", 800, 30, "hive,数据仓库", "2024-03-10"),
        (6, "Flink", "王五", 3200, 150, "flink,流计算", "2024-04-05"),
        (7, "Kafka", "王五", 1800, 60, "kafka,中间件", "2024-06-22"),
        (8, "Python", "赵六", 950, 40, "python,数据分析", "2024-07-30"),
    ]

    pipeline = DemoDataPipeline()
    pipeline \
        .ingest(data=data) \
        .clean() \
        .enrich() \
        .analyze() \
        .show_results() \
        .output() \
        .close()
```

## 9. 总结

```
┌──────────────────────────────────────────────────────────┐
│               PySpark 实战总结                            │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. PySpark vs Scala：                                   │
│     - DataFrame/SQL API 性能接近                        │
│     - UDF 性能有差异，Pandas UDF 缩小差距               │
│     - Python 开发效率更高                                │
│                                                          │
│  2. 文件读写：CSV/JSON/Parquet                           │
│     - 内部存储推荐 Parquet                               │
│     - 外部交换用 CSV                                     │
│                                                          │
│  3. UDF 三种类型：                                       │
│     - 普通 UDF：简单但慢                                 │
│     - Pandas UDF：快，推荐使用                           │
│     - RDD map：最灵活但最慢                              │
│                                                          │
│  4. Pandas 互转：                                        │
│     - toPandas：注意 Driver 内存限制                     │
│     - createDataFrame：启用 Arrow 优化                   │
│                                                          │
│  5. 错误排查：                                           │
│     - 常见错误有固定模式                                 │
│     - 学会看 Spark UI 和日志                             │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[10-部署模式.md](10-部署模式.md) — 作业提交与部署，学习 spark-submit 和生产环境部署。
