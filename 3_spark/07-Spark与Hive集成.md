# Spark 与 Hive 集成

## 1. 为什么 Spark 需要集成 Hive

```
Spark + Hive 的互补关系：

┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  Hive 的优势：                                               │
│  ┌──────────────────────────────────────────────────┐       │
│  │  - 成熟的元数据管理（Metastore）                  │       │
│  │  - 丰富的表管理（分区、分桶）                     │       │
│  │  - 广泛的企业使用（Hadoop 生态标准组件）          │       │
│  │  - 兼容多种文件格式（Parquet/ORC/CSV）           │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
│  Hive 的劣势：                                               │
│  ┌──────────────────────────────────────────────────┐       │
│  │  - 执行引擎（MapReduce/Tez）性能有限              │       │
│  │  - 不支持流处理                                   │       │
│  │  - 机器学习能力弱                                 │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
│  Spark 的优势：                                              │
│  ┌──────────────────────────────────────────────────┐       │
│  │  - 内存计算，性能远超 Hive                       │       │
│  │  - 统一计算引擎（批/流/ML/图）                   │       │
│  │  - Catalyst 优化器自动优化 SQL                    │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
│  结论：Spark 作为执行引擎 + Hive 作为元数据存储              │
│       = 企业级大数据计算的最佳实践                             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 2. 三种 Catalog 实现

### 2.1 spark_catalog vs hive

```
Catalog 管理架构：

┌──────────────────────────────────────────────────────────────┐
│                    SparkSession                              │
│                                                              │
│  ┌──────────────────────────────────────────────────┐       │
│  │            SessionCatalog                         │       │
│  │                                                   │       │
│  │  spark_catalog（默认）                             │       │
│  │  ┌───────────────────────────────────────────┐   │       │
│  │  │  spark.sql.catalogImplementation=hive 时  │   │       │
│  │  │  -> 使用 Hive Metastore 管理元数据        │   │       │
│  │  │  -> 可以直接读写 Hive 表                  │   │       │
│  │  │                                           │   │       │
│  │  │  spark.sql.catalogImplementation=in-memory 时│   │       │
│  │  │  -> 使用内存管理元数据                     │   │       │
│  │  │  -> 不能访问 Hive 表                      │   │       │
│  │  └───────────────────────────────────────────┘   │       │
│  │                                                   │       │
│  │  hive（旧版兼容，不推荐）                         │       │
│  │  ┌───────────────────────────────────────────┐   │       │
│  │  │  通过 spark.table("hive.table_name")     │   │       │
│  │  │  访问 Hive 表（Spark 3.x 中标记为废弃）   │   │       │
│  │  └───────────────────────────────────────────┘   │       │
│  │                                                   │       │
│  └──────────────────────────────────────────────────┘       │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 配置区别

```properties
# 方式 1：使用 Hive Metastore（推荐，可以读写 Hive 表）
spark.sql.catalogImplementation=hive

# 方式 2：使用内存 Catalog（默认，不能访问 Hive 表）
spark.sql.catalogImplementation=in-memory

# Spark 3.4+ 新方式：显式定义 Catalog
spark.sql.catalog.spark_catalog=org.apache.spark.sql.hive.catalog.HiveSessionCatalog
```

```python
# Spark 3.4+ 可以注册多个 Catalog
spark.conf.set("spark.sql.catalog.my_hive",
    "org.apache.spark.sql.hive.catalog.HiveSessionCatalog")
spark.conf.set("spark.sql.catalog.my_hive.metastore.uris",
    "thrift://localhost:9083")

# 使用自定义 Catalog
spark.catalog.setCurrentCatalog("my_hive")
spark.sql("SHOW DATABASES")
```

## 3. 配置步骤详解

### 3.1 完整配置流程

```
配置步骤流程图：

┌──────────────────────────────────────────────────────────┐
│ Step 1: 确认 Hive Metastore 已安装                       │
│   └── MySQL/Derby + Hive Metastore Service               │
│                                                          │
│ Step 2: 复制 hive-site.xml 到 Spark conf                 │
│   └── cp $HIVE_HOME/conf/hive-site.xml                   │
│       $SPARK_HOME/conf/hive-site.xml                     │
│                                                          │
│ Step 3: 添加数据库驱动 JAR                               │
│   └── cp mysql-connector-java-*.jar $SPARK_HOME/jars/    │
│                                                          │
│ Step 4: 修改 spark-defaults.conf                         │
│   └── spark.sql.catalogImplementation=hive               │
│       spark.hadoop.hive.metastore.uris=thrift://...      │
│                                                          │
│ Step 5: 验证连接                                         │
│   └── spark-sql > SHOW DATABASES;                        │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 3.2 hive-site.xml 关键配置

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Metastore 连接方式 1：直接连接数据库 -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true&amp;useSSL=false</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.cj.jdbc.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>hive_password</value>
    </property>

    <!-- Metastore 连接方式 2：通过 Metastore 服务（推荐） -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:9083</value>
    </property>

    <!-- Hive 数据在 HDFS 上的位置 -->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>
</configuration>
```

### 3.3 spark-defaults.conf 配置

```properties
# 启用 Hive Catalog
spark.sql.catalogImplementation=hive

# 如果使用 Metastore 服务模式
spark.hadoop.hive.metastore.uris=thrift://localhost:9083

# Hive 数据仓库目录
spark.sql.warehouse.dir=hdfs:///user/hive/warehouse

# 是否支持动态分区
spark.sql.hive.convertMetastoreParquet=true

# Hive 分区推断
spark.sql.hive.convertMetastoreOrc=true

# 支持 Hive UDF
spark.sql.udf.allowPythonUDF=true
```

## 4. 读写 Hive 表

### 4.1 spark.read.table() — 读取 Hive 表

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .enableHiveSupport() \  # 必须启用
    .getOrCreate()

# 方式 1：读取整张表
df = spark.read.table("blog_db.articles")
df.show()

# 方式 2：使用 spark.table()
df = spark.table("blog_db.articles")
df.show()

# 方式 3：使用 SQL
df = spark.sql("SELECT * FROM blog_db.articles")
df.show()

# 读取分区表（指定分区）
df = spark.read.table("blog_db.articles") \
    .filter("year = 2024 AND month = 5")
df.show()

# 读取指定分区
df = spark.sql("""
    SELECT * FROM blog_db.articles
    WHERE year = 2024 AND month >= 6
""")

# 只读取表的元数据（不加载实际数据）
schema = spark.read.table("blog_db.articles").schema
print(schema)
```

### 4.2 df.write.saveAsTable() — 写入 Hive 表

```python
# 创建示例数据
demo_data = [
    ("Spark入门教程", "张三", 1500, "2024-01-15"),
    ("Hadoop核心原理", "李四", 2300, "2024-02-20"),
    ("Flink流处理", "王五", 3200, "2024-04-05"),
]

df = spark.createDataFrame(demo_data, ["title", "author", "views", "publish_date"])

# 添加分区列
from pyspark.sql.functions import col, substring
df = df.withColumn("year", substring(col("publish_date"), 1, 4).cast("int"))
df = df.withColumn("month", substring(col("publish_date"), 6, 2).cast("int"))

# ============ 写入方式 1：saveAsTable（创建新表） ============
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("blog_db.articles_spark")

# ============ 写入方式 2：insertInto（插入已有表） ============
# 注意：insertInto 不关心列名，按位置匹配
df.write \
    .mode("append") \
    .insertInto("blog_db.articles_hive")

# ============ 写入方式 3：SQL INSERT ============
df.createOrReplaceTempView("temp_articles")
spark.sql("""
    INSERT INTO TABLE blog_db.articles
    PARTITION (year, month)
    SELECT title, author, views, publish_date, year, month
    FROM temp_articles
""")

# ============ 写入方式 4：动态分区写入 ============
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("blog_db.articles_dynamic")
```

### 4.3 写入模式

```
写入模式对比：

┌──────────────────────────────────────────────────────────────┐
│ Mode         | 表不存在       | 表存在                         │
├──────────────────────────────────────────────────────────────┤
│ error        | 创建表        | 报错（默认）                   │
│ overwrite    | 创建表        | 删除旧表，创建新表             │
│ append       | 创建表        | 追加数据                      │
│ ignore       | 创建表        | 忽略（不写入）                 │
└──────────────────────────────────────────────────────────────┘
```

## 5. spark.sql() 直接执行 Hive SQL

### 5.1 DDL 操作

```python
# 创建数据库
spark.sql("""
    CREATE DATABASE IF NOT EXISTS blog_db
    COMMENT '业务数据库'
    WITH DBPROPERTIES ('creator' = 'spark', 'date' = '2024-01-01')
""")

# 使用数据库
spark.sql("USE blog_db")

# 创建分区表
spark.sql("""
    CREATE TABLE IF NOT EXISTS blog_db.articles (
        id          BIGINT          COMMENT '文章ID',
        title       STRING          COMMENT '标题',
        author      STRING          COMMENT '作者',
        views       BIGINT          COMMENT '浏览量',
        likes       INT             COMMENT '点赞数',
        publish_date DATE           COMMENT '发布日期'
    )
    COMMENT '博客文章表'
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression' = 'snappy')
""")

# 创建分桶表
spark.sql("""
    CREATE TABLE IF NOT EXISTS blog_db.articles_bucketed (
        id          BIGINT,
        title       STRING,
        author      STRING,
        views       BIGINT
    )
    CLUSTERED BY (author) INTO 4 BUCKETS
    STORED AS ORC
""")

# 删除表
spark.sql("DROP TABLE IF EXISTS blog_db.temp_articles")

# 修改表
spark.sql("ALTER TABLE blog_db.articles SET TBLPROPERTIES ('updated' = '2024-06-01')")

# 添加分区
spark.sql("ALTER TABLE blog_db.articles ADD IF NOT EXISTS PARTITION (year=2024, month=6)")

# 删除分区
spark.sql("ALTER TABLE blog_db.articles DROP IF EXISTS PARTITION (year=2024, month=1)")

# 修复分区（MSCK REPAIR）
spark.sql("MSCK REPAIR TABLE blog_db.articles")
```

### 5.2 DML 操作

```python
# 插入数据
spark.sql("""
    INSERT INTO TABLE blog_db.articles
    PARTITION (year=2024, month=1)
    VALUES (1, 'Spark入门教程', '张三', 1500, 50, DATE '2024-01-15')
""")

# 从查询结果插入
spark.sql("""
    INSERT INTO TABLE blog_db.articles
    PARTITION (year, month)
    SELECT id, title, author, views, likes, publish_date, year, month
    FROM blog_db.articles_temp
""")

# 动态分区插入
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

spark.sql("""
    INSERT OVERWRITE TABLE blog_db.articles
    PARTITION (year, month)
    SELECT id, title, author, views, likes, publish_date,
           YEAR(publish_date) as year, MONTH(publish_date) as month
    FROM blog_db.articles_staging
""")

# 更新数据（需要表支持 ACID 事务）
# spark.sql("UPDATE blog_db.articles SET views = views + 100 WHERE author = '张三'")

# 删除数据（需要表支持 ACID 事务）
# spark.sql("DELETE FROM blog_db.articles WHERE views < 100")
```

### 5.3 复杂查询

```python
# 多表 Join
result = spark.sql("""
    SELECT
        a.title,
        a.author,
        a.views,
        c.category_name,
        u.city
    FROM blog_db.articles a
    LEFT JOIN blog_db.categories c ON a.category_id = c.id
    LEFT JOIN blog_db.users u ON a.author = u.username
    WHERE a.year = 2024
    ORDER BY a.views DESC
    LIMIT 100
""")
result.show()

# 使用 CTE
result = spark.sql("""
    WITH monthly_stats AS (
        SELECT
            author,
            year,
            month,
            COUNT(*) as article_count,
            SUM(views) as total_views
        FROM blog_db.articles
        GROUP BY author, year, month
    )
    SELECT
        author,
        year,
        month,
        article_count,
        total_views,
        RANK() OVER (PARTITION BY author ORDER BY total_views DESC) as rank
    FROM monthly_stats
    ORDER BY author, year, month
""")
result.show()
```

## 6. 读写 Parquet/ORC 文件

### 6.1 Parquet 格式

```
Parquet 列式存储结构：

┌──────────────────────────────────────────────────────────┐
│                   Parquet 文件结构                        │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────────────────────────────────────┐      │
│  │              Row Group 1                     │      │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐        │      │
│  │  │ Col A   │ │ Col B   │ │ Col C   │        │      │
│  │  │ (压缩)  │ │ (压缩)  │ │ (压缩)  │        │      │
│  │  │         │ │         │ │         │        │      │
│  │  │ 1,2,3   │ │ 'a','b' │ │ 10,20   │        │      │
│  │  └─────────┘ └─────────┘ └─────────┘        │      │
│  └──────────────────────────────────────────────┘      │
│  ┌──────────────────────────────────────────────┐      │
│  │              Row Group 2                     │      │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐        │      │
│  │  │ Col A   │ │ Col B   │ │ Col C   │        │      │
│  │  │ (压缩)  │ │ (压缩)  │ │ (压缩)  │        │      │
│  │  └─────────┘ └─────────┘ └─────────┘        │      │
│  └──────────────────────────────────────────────┘      │
│  ┌──────────────────────────────────────────────┐      │
│  │  Footer (Schema + Statistics + Row Groups)   │      │
│  └──────────────────────────────────────────────┘      │
│                                                          │
│  优点：                                                   │
│  - 列式存储，只读取需要的列（列裁剪）                     │
│  - 每个列块有统计信息（min/max/null_count）               │
│  - 高压缩比（同类型数据连续存储）                         │
│  - 支持 Schema 演进                                      │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 6.2 读写 Parquet

```python
# 写入 Parquet
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("hdfs:///user/data/demos_parquet")

# 读取 Parquet
df_parquet = spark.read.parquet("hdfs:///user/data/demos_parquet")

# 读取指定分区
df_parquet = spark.read.parquet("hdfs:///user/data/demos_parquet/year=2024/month=5")

# 读取指定列（列裁剪，只读取需要的列，性能好）
df_parquet = spark.read.parquet("hdfs:///user/data/demos_parquet") \
    .select("title", "author", "views")

# 读取时指定 Schema（Schema 演进）
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("views", LongType()),
])
df = spark.read.schema(schema).parquet("hdfs:///user/data/demos_parquet")

# 查看文件元数据
df_parquet.printSchema()

# Parquet 文件压缩（默认 Snappy）
df.write \
    .option("compression", "snappy") \
    .parquet("output/demos_snappy")

df.write \
    .option("compression", "gzip") \
    .parquet("output/demos_gzip")

# 合并为单个文件
df.coalesce(1).write.parquet("output/demos_single")
```

### 6.3 读写 ORC

```python
# 写入 ORC
df.write \
    .mode("overwrite") \
    .orc("hdfs:///user/data/demos_orc")

# 读取 ORC
df_orc = spark.read.orc("hdfs:///user/data/demos_orc")

# ORC 压缩
df.write \
    .option("compression", "snappy") \
    .orc("output/demos_orc_snappy")

# Parquet vs ORC 对比
# Parquet：Spark 生态首选，社区支持好，压缩率高
# ORC：Hive 生态首选，压缩率更高，Hive 场景性能更好
# 建议：Spark 环境用 Parquet，Hive 环境用 ORC
```

## 7. Hive UDF 在 Spark 中的使用

### 7.1 Spark 内置函数替代

```python
# 大部分 Hive UDF 可以用 Spark 内置函数替代

# Hive: LOWER(str)    -> Spark: F.lower(col)
# Hive: UPPER(str)    -> Spark: F.upper(col)
# Hive: CONCAT(s1,s2) -> Spark: F.concat(col1, col2)
# Hive: SUBSTR(s,p,l) -> Spark: F.substring(col, p, l)
# Hive: CAST(x AS t)  -> Spark: col.cast(t)

from pyspark.sql.functions import lower, upper, concat, substring

df.select(
    lower("title").alias("lower_title"),
    upper("author").alias("upper_author"),
    concat("title", " - ", "author").alias("full_title")
).show()
```

### 7.2 使用 Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# 定义 UDF
@udf(StringType())
def popularity_level(views):
    if views is None:
        return "未知"
    elif views >= 3000:
        return "热门"
    elif views >= 1000:
        return "普通"
    else:
        return "冷门"

# 使用 UDF
df.withColumn("level", popularity_level("views")).show()

# 注册 UDF 以便在 SQL 中使用
spark.udf.register("popularity_level", popularity_level)

spark.sql("""
    SELECT title, views, popularity_level(views) as level
    FROM demos
""").show()
```

### 7.3 使用 Pandas UDF（性能更好）

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Pandas UDF：向量化操作，性能比普通 UDF 好很多
@pandas_udf(StringType())
def popularity_level_pandas(views: pd.Series) -> pd.Series:
    def _level(v):
        if pd.isna(v):
            return "未知"
        elif v >= 3000:
            return "热门"
        elif v >= 1000:
            return "普通"
        else:
            return "冷门"
    return views.apply(_level)

# 使用 Pandas UDF
df.withColumn("level", popularity_level_pandas("views")).show()

# 分组聚合 Pandas UDF
@pandas_udf("double")
def weighted_mean(views: pd.Series, likes: pd.Series) -> float:
    return (views * likes).sum() / likes.sum() if likes.sum() > 0 else 0.0

result = df.groupBy("author").agg(
    weighted_mean("views", "likes").alias("weighted_avg")
)
result.show()
```

### 7.4 使用 Hive Java UDF

```python
# 如果有 Hive 的 Java UDF JAR 包，可以在 Spark 中加载

# 方法 1：通过 --jars 参数
# spark-submit --jars /path/to/hive-udf.jar my_app.py

# 方法 2：通过配置
# spark.conf.set("spark.jars", "/path/to/hive-udf.jar")

# 方法 3：在代码中注册
from pyspark.sql.types import StringType

# 假设 Hive UDF 类为 com.example.hive.udf.MyUdf
spark.udf.registerJavaFunction(
    "my_hive_udf",
    "com.example.hive.udf.MyUdf",
    StringType()
)

# 使用
spark.sql("SELECT my_hive_udf(title) FROM demos").show()
```

## 8. Spark SQL 与 HiveQL 的语法差异

### 8.1 兼容性对比

```
┌──────────────────────────────────────────────────────────────┐
│                   语法兼容性对比                              │
├──────────────┬──────────────┬──────────────────────────────┤
│ 特性          │ HiveQL       │ Spark SQL                    │
├──────────────┼──────────────┼──────────────────────────────┤
│ SELECT       │ 支持         │ 支持                         │
│ JOIN         │ 支持         │ 支持                         │
│ 子查询       │ 支持         │ 支持                         │
│ CTE (WITH)   │ 支持         │ 支持                         │
│ 窗口函数     │ 支持         │ 支持                         │
│ UNION        │ 支持         │ 支持                         │
│ CASE WHEN    │ 支持         │ 支持                         │
│ INSERT       │ 支持         │ 支持                         │
│ UPDATE/DELETE│ 支持(ACID)   │ 部分支持                     │
│ LATERAL VIEW │ 支持         │ 支持                         │
│ SORT BY      │ 支持         │ 支持                         │
│ DISTRIBUTE BY│ 支持         │ 支持                         │
│ CLUSTER BY   │ 支持         │ 支持                         │
│ TRANSFORM    │ 支持         │ 不支持                       │
│ STORED AS    │ TEXT/ORC/... │ PARQUET/ORC/CSV/JSON/...     │
│ SerDe        │ 自定义       │ 部分支持                     │
│ ACID 事务    │ 支持         │ 有限支持                     │
└──────────────┴──────────────┴──────────────────────────────┘
```

### 8.2 常见差异

```sql
-- 1. 数据类型差异
-- Hive: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
-- Spark: ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType
-- 注意：Spark 不支持 Hive 的 BINARY 类型（使用 BinaryType 替代）

-- 2. 日期函数差异
-- Hive: FROM_UNIXTIME, UNIX_TIMESTAMP, DATE_FORMAT
-- Spark: 同样支持，但格式化语法可能有细微差异

-- Hive:
SELECT FROM_UNIXTIME(1705276800, 'yyyy-MM-dd HH:mm:ss');
-- Spark:
SELECT FROM_UNIXTIME(1705276800, 'yyyy-MM-dd HH:mm:ss');

-- 3. 字符串函数差异
-- Hive: REGEXP_EXTRACT(str, pattern)
-- Spark: regexp_extract(str, pattern, groupIdx)  -- 注意第三个参数

-- 4. 分桶语法
-- Hive: CLUSTERED BY (col) INTO N BUCKETS
-- Spark: 同样支持，但写入分桶表的方式不同

-- 5. 文件格式
-- Hive: STORED AS TEXTFILE / SEQUENCEFILE / ORC
-- Spark: USING PARQUET / ORC / CSV / JSON / TEXT / DELTA

-- 6. 事务操作
-- Hive: 支持 INSERT/UPDATE/DELETE（需要 ACID 表）
-- Spark: 支持 INSERT，UPDATE/DELETE 需要使用 Delta Lake
```

### 8.3 Hive 特有语法在 Spark 中的替代

```python
# LATERAL VIEW EXPLODE（Hive 特有，Spark 也支持）
# Hive 写法
spark.sql("""
    SELECT title, tag
    FROM demos
    LATERAL VIEW EXPLODE(split(tags, ',')) t AS tag
""")

# Spark 原生写法（推荐）
df.withColumn("tag", F.explode(F.split(F.col("tags"), ","))) \
    .select("title", "tag").show()

# SORT BY / DISTRIBUTE BY / CLUSTER BY
spark.sql("""
    SELECT * FROM demos
    DISTRIBUTE BY author
    SORT BY views DESC
""")

# TRANSFORM（Hive 特有，Spark 不支持）
# 替代方案：使用 UDF 或 mapPartitions
```

## 9. 实战：从 Hive 读业务数据 → 处理 → 写回 Hive

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, \
    count as _count, when, upper, trim, to_date, regexp_replace, \
    month as _month, year as _year, desc, rank as _rank
from pyspark.sql.window import Window

# ============ 1. 创建 SparkSession（启用 Hive 支持） ============
spark = SparkSession.builder \
    .appName("DemoHivePipeline") \
    .enableHiveSupport() \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

print("Spark Session 创建成功")
print(f"Catalog 实现: {spark.conf.get('spark.sql.catalogImplementation')}")

# ============ 2. 创建数据库和表 ============
spark.sql("CREATE DATABASE IF NOT EXISTS blog_db")
spark.sql("USE blog_db")

# 创建原始数据表
spark.sql("""
    CREATE TABLE IF NOT EXISTS blog_db.articles_raw (
        id          BIGINT,
        title       STRING,
        author      STRING,
        views       BIGINT,
        likes       INT,
        tags        STRING,
        publish_date STRING
    )
    STORED AS PARQUET
""")

# 创建处理后的分区表
spark.sql("""
    CREATE TABLE IF NOT EXISTS blog_db.articles (
        id          BIGINT,
        title       STRING,
        author      STRING,
        views       BIGINT,
        likes       INT,
        tags        STRING,
        publish_date DATE,
        popularity  STRING,
        tag_count   INT
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
""")

# 创建统计结果表
spark.sql("""
    CREATE TABLE IF NOT EXISTS blog_db.author_stats (
        author          STRING,
        article_count   BIGINT,
        total_views     BIGINT,
        avg_views       DOUBLE,
        total_likes     BIGINT,
        max_views       BIGINT
    )
    STORED AS PARQUET
""")

# ============ 3. 写入原始数据 ============
raw_data = [
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
]

raw_df = spark.createDataFrame(raw_data,
    ["id", "title", "author", "views", "likes", "tags", "publish_date"])

raw_df.write.mode("overwrite").saveAsTable("blog_db.articles_raw")
print("原始数据写入完成")

# ============ 4. 从 Hive 读取原始数据 ============
df = spark.read.table("blog_db.articles_raw")
print(f"读取到 {df.count()} 条原始数据")

# ============ 5. 数据清洗和处理 ============
df_clean = df \
    .withColumn("title", trim(col("title"))) \
    .withColumn("author", trim(col("author"))) \
    .withColumn("tags", trim(regexp_replace(col("tags"), r"\s+", ""))) \
    .withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd")) \
    .withColumn("popularity",
        when(col("views") >= 3000, "热门")
        .when(col("views") >= 1000, "普通")
        .otherwise("冷门")
    ) \
    .withColumn("tag_count",
        _count(col("tags"))  # 简化处理
    ) \
    .withColumn("year", _year(col("publish_date"))) \
    .withColumn("month", _month(col("publish_date")))

# ============ 6. 写回 Hive（分区表） ============
# 启用动态分区
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

df_clean.select("id", "title", "author", "views", "likes",
               "tags", "publish_date", "popularity", "tag_count",
               "year", "month") \
    .write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("blog_db.articles")

print("处理后数据写入完成")

# ============ 7. 统计分析并写入结果表 ============
author_stats = df_clean.groupBy("author").agg(
    _count("*").alias("article_count"),
    _sum("views").alias("total_views"),
    _avg("views").alias("avg_views"),
    _sum("likes").alias("total_likes"),
    _max("views").alias("max_views"),
)

author_stats.write.mode("overwrite").saveAsTable("blog_db.author_stats")
print("统计结果写入完成")

# ============ 8. 验证结果 ============
print("\n=== 文章表数据 ===")
spark.sql("SELECT * FROM blog_db.articles ORDER BY views DESC").show()

print("\n=== 作者统计 ===")
spark.sql("SELECT * FROM blog_db.author_stats ORDER BY total_views DESC").show()

print("\n=== 分区信息 ===")
spark.sql("SHOW PARTITIONS blog_db.articles").show()

spark.stop()
```

## 10. 总结

```
┌──────────────────────────────────────────────────────────┐
│            Spark 与 Hive 集成总结                         │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. 集成价值：Spark 计算 + Hive 元数据 = 最佳实践        │
│                                                          │
│  2. 配置核心：                                            │
│     - enableHiveSupport()                                │
│     - spark.sql.catalogImplementation=hive               │
│     - hive-site.xml + MySQL 驱动 JAR                    │
│                                                          │
│  3. 读写操作：                                            │
│     - spark.read.table() / spark.table()                │
│     - df.write.saveAsTable() / df.write.insertInto()    │
│     - spark.sql() 执行任意 Hive SQL                     │
│                                                          │
│  4. 文件格式：                                            │
│     - 推荐 Parquet（列式存储，压缩好，Spark 原生支持）   │
│     - ORC 适合 Hive 生态                                 │
│                                                          │
│  5. UDF 使用：                                           │
│     - 优先用 Spark 内置函数                              │
│     - Python UDF：简单但性能一般                         │
│     - Pandas UDF：向量化操作，性能好                     │
│     - Java UDF：加载 JAR 包                              │
│                                                          │
│  6. 语法差异：大部分 HiveQL 在 Spark SQL 中兼容          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[08-性能优化.md](08-性能优化.md) — Spark 性能调优，学习分区、缓存、AQE 等优化策略。
