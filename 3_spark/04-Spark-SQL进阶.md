# Spark SQL 进阶

## 1. Join 详解

### 1.1 Join 类型总览

```
Join 类型图示（A 表和 B 表 Join）：

INNER JOIN（内连接）：
  ┌──────────┐    ┌──────────┐
  │   A 表   │    │   B 表   │
  │ ┌──────┐ │    │ ┌──────┐ │
  │ │匹配行│◄├────┤►│匹配行│ │   只保留两边都匹配的行
  │ └──────┘ │    │ └──────┘ │
  │ 不匹配行 │    │ 不匹配行 │
  └──────────┘    └──────────┘

LEFT JOIN（左外连接）：
  ┌──────────┐    ┌──────────┐
  │   A 表   │    │   B 表   │
  │ ┌──────┐ │    │ ┌──────┐ │
  │ │全部行│◄├────┤►│匹配行│ │   A 表全部保留，B 表无匹配为 NULL
  │ └──────┘ │    │ └──────┘ │
  └──────────┘    └──────────┘

RIGHT JOIN（右外连接）：
  ┌──────────┐    ┌──────────┐
  │   A 表   │    │   B 表   │
  │ ┌──────┐ │    │ ┌──────┐ │
  │ │匹配行│◄├────┤►│全部行│ │   B 表全部保留，A 表无匹配为 NULL
  │ └──────┘ │    │ └──────┘ │
  └──────────┘    └──────────┘

FULL OUTER JOIN（全外连接）：
  ┌──────────┐    ┌──────────┐
  │   A 表   │    │   B 表   │
  │ ┌──────┐ │    │ ┌──────┐ │
  │ │全部行│◄├────┤►│全部行│ │   两边全部保留
  │ └──────┘ │    │ └──────┘ │
  └──────────┘    └──────────┘

CROSS JOIN（交叉连接）：
  A 的每一行 × B 的每一行 = 笛卡尔积
  如果 A 有 3 行，B 有 4 行，结果有 12 行
```

### 1.2 准备示例数据

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("JoinDemo") \
    .master("local[*]") \
    .getOrCreate()

# 文章表
articles = spark.createDataFrame([
    (1, "Spark入门教程", "张三", 1500, 1),
    (2, "Hadoop核心原理", "张三", 2300, 1),
    (3, "Hive数据仓库", "李四", 800, 2),
    (4, "Flink流处理", "王五", 3200, 3),
    (5, "Kafka消息队列", "王五", 1800, 3),
    (6, "Python数据分析", "赵六", 950, 4),
], ["article_id", "title", "author", "views", "category_id"])

# 分类表
categories = spark.createDataFrame([
    (1, "大数据"),
    (2, "数据仓库"),
    (3, "流计算"),
    (5, "前端开发"),   # 注意：没有文章使用这个分类
], ["category_id", "category_name"])

# 作者信息表
authors = spark.createDataFrame([
    ("张三", "北京", "大数据工程师"),
    ("李四", "上海", "数据仓库工程师"),
    ("王五", "深圳", "流计算工程师"),
    ("陈七", "杭州", "前端工程师"),  # 注意：没有文章的作者
], ["author_name", "city", "role"])

articles.createOrReplaceTempView("articles")
categories.createOrReplaceTempView("categories")
authors.createOrReplaceTempView("authors")
```

### 1.3 INNER JOIN

```python
# DataFrame API
inner_join = articles.join(
    categories,
    articles["category_id"] == categories["category_id"],
    "inner"
)

# SQL 方式
inner_join_sql = spark.sql("""
    SELECT a.article_id, a.title, c.category_name
    FROM articles a
    INNER JOIN categories c ON a.category_id = c.category_id
""")

inner_join_sql.show()
```

输出：
```
+----------+----------------+-------------+
|article_id|           title|category_name|
+----------+----------------+-------------+
|         1|  Spark入门教程  |       大数据|
|         2|Hadoop核心原理  |       大数据|
|         3|  Hive数据仓库  |     数据仓库|
|         4|   Flink流处理  |       流计算|
|         5| Kafka消息队列  |       流计算|
+----------+----------------+-------------+
```

### 1.4 LEFT JOIN

```python
# DataFrame API
left_join = articles.join(
    categories,
    articles["category_id"] == categories["category_id"],
    "left"
)

# SQL 方式
left_join_sql = spark.sql("""
    SELECT a.article_id, a.title, c.category_name
    FROM articles a
    LEFT JOIN categories c ON a.category_id = c.category_id
""")

left_join_sql.show()
```

输出：
```
+----------+----------------+-------------+
|article_id|           title|category_name|
+----------+----------------+-------------+
|         6|Python数据分析  |         null|  -- 无匹配，显示 NULL
|         1|  Spark入门教程  |       大数据|
|         2|Hadoop核心原理  |       大数据|
|         3|  Hive数据仓库  |     数据仓库|
|         4|   Flink流处理  |       流计算|
|         5| Kafka消息队列  |       流计算|
+----------+----------------+-------------+
```

### 1.5 RIGHT JOIN

```python
# SQL 方式
right_join_sql = spark.sql("""
    SELECT a.title, c.category_id, c.category_name
    FROM articles a
    RIGHT JOIN categories c ON a.category_id = c.category_id
""")

right_join_sql.show()
```

输出：
```
+----------------+-----------+-------------+
|           title|category_id|category_name|
+----------------+-----------+-------------+
|  Spark入门教程  |          1|       大数据|
|Hadoop核心原理  |          1|       大数据|
|  Hive数据仓库  |          2|     数据仓库|
|   Flink流处理  |          3|       流计算|
| Kafka消息队列  |          3|       流计算|
|            null|          5|     前端开发|  -- 无匹配的文章
+----------------+-----------+-------------+
```

### 1.6 FULL OUTER JOIN

```python
full_join_sql = spark.sql("""
    SELECT a.title, c.category_name
    FROM articles a
    FULL OUTER JOIN categories c ON a.category_id = c.category_id
""")
```

### 1.7 CROSS JOIN

```python
cross_join_sql = spark.sql("""
    SELECT a.title, c.category_name
    FROM articles a
    CROSS JOIN categories c
    WHERE a.category_id = c.category_id
""")
```

### 1.8 SELF JOIN（自连接）

自连接是将表与自身进行 Join，常用于层级关系数据。

```python
# 评论表：每条评论可以有父评论
comments = spark.createDataFrame([
    (1, "写得很好", "读者A", None),
    (2, "感谢支持", "张三", 1),
    (3, "学到了很多", "读者B", None),
    (4, "欢迎继续关注", "张三", 3),
    (5, "有个问题", "读者C", 3),
], ["comment_id", "content", "user", "parent_id"])

comments.createOrReplaceTempView("comments")

# 查询每条评论及其回复
replies = spark.sql("""
    SELECT
        c1.comment_id as comment_id,
        c1.content as comment,
        c1.user as commenter,
        c2.comment_id as reply_id,
        c2.content as reply,
        c2.user as replier
    FROM comments c1
    INNER JOIN comments c2 ON c2.parent_id = c1.comment_id
""")

replies.show()
```

输出：
```
+----------+--------+--------+--------+----------------+-------+
|comment_id|comment|commenter|reply_id|           reply|replier|
+----------+--------+--------+--------+----------------+-------+
|         1|写得很好|  读者A |       2|    感谢支持  |   张三|
|         3|学到了很多| 读者B |       4|欢迎继续关注  |   张三|
|         3|学到了很多| 读者B |       5|    有个问题  |  读者C|
+----------+--------+--------+--------+----------------+-------+
```

## 2. Join 优化策略

### 2.1 三种 Join 策略

```
Spark Join 执行策略：

┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  1. Broadcast Hash Join（广播哈希 Join）                      │
│     ┌────────────────────────────────────┐                  │
│     │ 小表广播到所有 Executor 的内存中    │                  │
│     │ 大表在每个 Executor 中与广播表 Join │                  │
│     │                                    │                  │
│     │ 适用：一张表 < 10MB（默认阈值）     │                  │
│     │ 优点：无 Shuffle，速度最快          │                  │
│     └────────────────────────────────────┘                  │
│                                                              │
│  2. Sort Merge Join（排序归并 Join）                          │
│     ┌────────────────────────────────────┐                  │
│     │ 两张表都按 Join Key 排序            │                  │
│     │ 然后归并匹配                       │                  │
│     │                                    │                  │
│     │ 适用：两张表都较大                  │                  │
│     │ 优点：处理大数据量稳定              │                  │
│     │ 缺点：需要排序，有 Shuffle          │                  │
│     └────────────────────────────────────┘                  │
│                                                              │
│  3. Shuffle Hash Join（Shuffle 哈希 Join）                   │
│     ┌────────────────────────────────────┐                  │
│     │ 按 Join Key 的 Hash 值 Shuffle     │                  │
│     │ 相同 Key 的数据发到同一个 Executor   │                  │
│     │ 在每个 Executor 中做 Hash Join      │                  │
│     │                                    │                  │
│     │ 适用：一张表可以放入内存            │                  │
│     │ 缺点：数据倾斜时性能差              │                  │
│     └────────────────────────────────────┘                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 手动选择 Join 策略

```python
# 方法 1：通过 hint 指定 Broadcast Join
from pyspark.sql.functions import broadcast

result = articles.join(
    broadcast(categories),  # 强制广播 categories 表
    articles["category_id"] == categories["category_id"],
    "inner"
)

# 方法 2：通过 SQL hint
result = spark.sql("""
    SELECT /*+ BROADCAST(c) */
        a.title, c.category_name
    FROM articles a
    JOIN categories c ON a.category_id = c.category_id
""")

# 方法 3：通过配置调整广播阈值
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB

# 方法 4：强制使用 Sort Merge Join（关闭广播）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

### 2.3 Join 优化建议

```python
# 1. 大表 Join 小表：使用 Broadcast Join
# 默认 autoBroadcastJoinThreshold = 10MB
# 小表会自动广播

# 2. 大表 Join 大表：使用 Sort Merge Join
# 确保 Join Key 两边数据类型一致
# 预先过滤不需要的数据

# 3. 多表 Join 时：先 Join 小表
# 错误写法：
result = big_table1.join(big_table2, ...).join(big_table3, ...)
# 正确写法：
result = small_table.join(big_table1, ...).join(big_table2, ...)

# 4. Join 前先过滤
# 错误：先 Join 再过滤
df1.join(df2, ...).filter(col("date") > "2024-01-01")
# 正确：先过滤再 Join
df1.filter(col("date") > "2024-01-01").join(df2, ...)
```

## 3. 子查询

### 3.1 WHERE 子查询

```python
# 查找浏览量高于平均浏览量的文章
result = spark.sql("""
    SELECT title, author, views
    FROM articles
    WHERE views > (SELECT AVG(views) FROM articles)
    ORDER BY views DESC
""")
result.show()
```

### 3.2 HAVING 子查询

```python
# 查找文章数超过平均文章数的作者
result = spark.sql("""
    SELECT author, COUNT(*) as article_count
    FROM articles
    GROUP BY author
    HAVING COUNT(*) > (
        SELECT AVG(article_count)
        FROM (
            SELECT author, COUNT(*) as article_count
            FROM articles
            GROUP BY author
        )
    )
""")
result.show()
```

### 3.3 FROM 子查询

```python
# 将子查询结果当作表使用
result = spark.sql("""
    SELECT author_level, COUNT(*) as author_count
    FROM (
        SELECT
            author,
            SUM(views) as total_views,
            CASE
                WHEN SUM(views) >= 5000 THEN '头部作者'
                WHEN SUM(views) >= 2000 THEN '腰部作者'
                ELSE '新晋作者'
            END as author_level
        FROM articles
        GROUP BY author
    ) author_summary
    GROUP BY author_level
""")
result.show()
```

## 4. UNION / UNION ALL / INTERSECT / EXCEPT

### 4.1 UNION vs UNION ALL

```
UNION ALL：直接合并，保留重复行
  ┌─────────┐   ┌─────────┐
  │ A, B, C │ + │ B, C, D │ = │ A, B, C, B, C, D │
  └─────────┘   └─────────┘   └────────────────┘

UNION：合并后去重
  ┌─────────┐   ┌─────────┐
  │ A, B, C │ + │ B, C, D │ = │ A, B, C, D │
  └─────────┘   └─────────┘   └──────────┘
```

```python
# 准备数据
articles_2024 = spark.createDataFrame([
    ("Spark入门", "张三"), ("Hadoop教程", "李四"), ("Flink入门", "王五")
], ["title", "author"])

articles_2023 = spark.createDataFrame([
    ("Hadoop教程", "李四"), ("Kafka入门", "王五"), ("Python基础", "赵六")
], ["title", "author"])

# UNION ALL（保留重复行，性能好）
union_all = articles_2024.unionAll(articles_2023)
union_all.show()

# UNION（去重，需要额外 Shuffle）
union_result = articles_2024.union(articles_2023)
union_result.show()
```

### 4.2 INTERSECT（交集）

```python
# 两个表中都存在的记录
intersect_result = articles_2024.intersect(articles_2023)
intersect_result.show()
# 输出：Hadoop教程, 李四
```

### 4.3 EXCEPT（差集）

```python
# 在 A 表中但不在 B 表中的记录
except_result = articles_2024.except(articles_2023)
except_result.show()
# 输出：Spark入门/张三, Flink入门/王五
```

## 5. CASE WHEN 表达式

### 5.1 基本 CASE WHEN

```python
from pyspark.sql.functions import when, col

# DataFrame API 方式
df_with_level = df.withColumn("popularity_level",
    when(col("views") >= 3000, "热门")
    .when(col("views") >= 1500, "普通")
    .when(col("views") >= 500, "冷门")
    .otherwise("待提升")
)

# SQL 方式
result = spark.sql("""
    SELECT
        title,
        views,
        CASE
            WHEN views >= 3000 THEN '热门'
            WHEN views >= 1500 THEN '普通'
            WHEN views >= 500  THEN '冷门'
            ELSE '待提升'
        END as popularity_level
    FROM articles
""")
result.show()
```

### 5.2 复杂 CASE WHEN

```python
# 多条件 CASE WHEN
result = spark.sql("""
    SELECT
        title,
        author,
        views,
        CASE
            WHEN views >= 3000 AND category_id = 1 THEN '大数据热门文章'
            WHEN views >= 3000 THEN '其他热门文章'
            WHEN views >= 1000 AND author = '张三' THEN '张三的普通文章'
            ELSE '其他文章'
        END as article_type
    FROM articles
""")
result.show()
```

## 6. 窗口函数

### 6.1 窗口函数概念

```
窗口函数原理：

普通聚合（GROUP BY）：
  ┌───────────────────────────────────────────┐
  │  每组只返回一行聚合结果                      │
  │  author  |  SUM(views)                    │
  │  --------|-----------                     │
  │  张三    |  8800                           │
  │  李四    |  3100                           │
  └───────────────────────────────────────────┘

窗口函数（OVER）：
  ┌───────────────────────────────────────────┐
  │  每行都保留，附加窗口计算结果                 │
  │  title    | author | views | SUM(views)    │
  │  ---------|--------|-------|------------  │
  │  Spark入门| 张三   | 1500  | 8800         │
  │  SQL详解  | 张三   | 2800  | 8800         │
  │  性能调优 | 张三   | 4500  | 8800         │
  │  Hadoop   | 李四   | 2300  | 3100         │
  │  Hive     | 李四   |  800  | 3100         │
  └───────────────────────────────────────────┘

  窗口定义 = PARTITION BY + ORDER BY + 窗口范围
```

### 6.2 窗口定义

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# 定义窗口
# 按 author 分区，按 views 降序排序
window_spec = Window \
    .partitionBy("author") \
    .orderBy(col("views").desc())

# 不分区，按 views 全局排序
window_global = Window \
    .orderBy(col("views").desc())

# 分区 + 排序 + 指定窗口范围（当前行及之前的所有行）
window_range = Window \
    .partitionBy("author") \
    .orderBy(col("views").desc()) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

### 6.3 ROW_NUMBER、RANK、DENSE_RANK

```python
from pyspark.sql.functions import row_number, rank, dense_rank

# 准备数据
scores = spark.createDataFrame([
    ("张三", "语文", 90),
    ("张三", "数学", 85),
    ("李四", "语文", 90),
    ("李四", "数学", 95),
    ("王五", "语文", 78),
    ("王五", "数学", 88),
], ["name", "subject", "score"])

scores.createOrReplaceTempView("scores")

# ROW_NUMBER：连续排名，相同值也给不同序号
scores.withColumn("row_num",
    row_number().over(Window.partitionBy("subject").orderBy(col("score").desc()))
).show()

# RANK：相同值排名相同，下一个排名跳号
scores.withColumn("rank",
    rank().over(Window.partitionBy("subject").orderBy(col("score").desc()))
).show()

# DENSE_RANK：相同值排名相同，下一个排名不跳号
scores.withColumn("dense_rank",
    dense_rank().over(Window.partitionBy("subject").orderBy(col("score").desc()))
).show()
```

三种排名函数的区别：

```
数据：95, 90, 90, 85, 78

ROW_NUMBER: 1, 2, 3, 4, 5    （每行不同）
RANK:       1, 2, 2, 4, 5    （相同值排名相同，跳号）
DENSE_RANK: 1, 2, 2, 3, 4    （相同值排名相同，不跳号）

适用场景：
- ROW_NUMBER：取每组前 N 名（Top N）
- RANK：允许并列排名（如成绩排名）
- DENSE_RANK：允许并列且不跳号（如奖牌排名）
```

### 6.4 LEAD / LAG

```python
from pyspark.sql.functions import lead, lag

# LAG：取前一行的值
# LEAD：取后一行的值
result = scores.withColumn("prev_score",
    lag("score", 1).over(Window.partitionBy("name").orderBy("subject"))
).withColumn("next_score",
    lead("score", 1).over(Window.partitionBy("name").orderBy("subject"))
)

result.show()
```

输出：
```
+----+-------+-----+----------+----------+
|name|subject|score|prev_score|next_score|
+----+-------+-----+----------+----------+
|  张三|  数学  |   85|      null|        90|
|  张三|  语文  |   90|        85|      null|
|  李四|  数学  |   95|      null|        90|
|  李四|  语文  |   90|        95|      null|
+----+-------+-----+----------+----------+
```

### 6.5 FIRST_VALUE / LAST_VALUE

```python
from pyspark.sql.functions import first_value, last_value

result = scores.withColumn("first_in_subject",
    first_value("name").over(
        Window.partitionBy("subject").orderBy(col("score").desc())
    )
).withColumn("last_in_subject",
    last_value("name").over(
        Window.partitionBy("subject")
        .orderBy(col("score").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
)

result.show()
```

### 6.6 NTILE

```python
from pyspark.sql.functions import ntile

# 将数据分为 N 个桶
result = scores.withColumn("quartile",
    ntile(4).over(Window.orderBy("score"))
)
result.show()
```

### 6.7 累计求和与移动平均

```python
from pyspark.sql.functions import sum as _sum, avg as _avg

# 累计求和
result = scores.withColumn("cumulative_sum",
    _sum("score").over(
        Window.partitionBy("name")
        .orderBy("subject")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
)

# 移动平均（当前行及前一行）
result = scores.withColumn("moving_avg",
    _avg("score").over(
        Window.partitionBy("name")
        .orderBy("subject")
        .rowsBetween(-1, Window.currentRow)
    )
)
```

### 6.8 实战：博客文章排名

```python
from pyspark.sql.functions import row_number, rank, dense_rank, \
    lead, lag, sum as _sum, avg as _avg, percent_rank

# 使用之前的 articles 表
window_author = Window.partitionBy("author").orderBy(col("views").desc())
window_all = Window.orderBy(col("views").desc())

result = articles.withColumn("author_rank",
    row_number().over(window_author)           # 作者内排名
).withColumn("global_rank",
    rank().over(window_all)                     # 全局排名
).withColumn("prev_views",
    lag("views", 1).over(window_author)         # 作者上一篇浏览量
).withColumn("author_total_views",
    _sum("views").over(Window.partitionBy("author"))  # 作者总浏览量
).withColumn("author_avg_views",
    _avg("views").over(Window.partitionBy("author"))  # 作者平均浏览量
).select(
    "title", "author", "views", "author_rank",
    "global_rank", "prev_views", "author_total_views", "author_avg_views"
)

result.orderBy("author", "author_rank").show()
```

## 7. CTE（WITH 子句）

### 7.1 基本 CTE

```python
# CTE 让复杂查询更清晰、更易维护
result = spark.sql("""
    -- CTE：作者统计
    WITH author_stats AS (
        SELECT
            author,
            COUNT(*) as article_count,
            SUM(views) as total_views,
            AVG(views) as avg_views
        FROM articles
        GROUP BY author
    ),
    -- CTE：分类统计
    category_stats AS (
        SELECT
            category_id,
            COUNT(*) as article_count
        FROM articles
        GROUP BY category_id
    )
    -- 主查询
    SELECT
        a.author,
        a.article_count,
        a.total_views,
        ROUND(a.avg_views, 2) as avg_views,
        CASE
            WHEN a.total_views >= 5000 THEN '头部作者'
            WHEN a.total_views >= 2000 THEN '腰部作者'
            ELSE '新晋作者'
        END as author_level
    FROM author_stats a
    ORDER BY a.total_views DESC
""")
result.show()
```

### 7.2 递归 CTE（Spark 3.0+）

Spark 3.0 开始支持递归 CTE，用于处理层级数据。

```python
# 数据平台分类层级（一级分类 -> 二级分类）
categories_tree = spark.createDataFrame([
    (1, None, "技术"),
    (2, 1, "大数据"),
    (3, 1, "前端"),
    (4, 2, "Spark"),
    (5, 2, "Hadoop"),
    (6, 3, "React"),
    (7, None, "生活"),
    (8, 7, "旅行"),
], ["id", "parent_id", "name"])

categories_tree.createOrReplaceTempView("categories_tree")

# 递归 CTE：展开分类层级
result = spark.sql("""
    WITH RECURSIVE category_path AS (
        -- 基础查询：顶级分类
        SELECT id, parent_id, name, name as path
        FROM categories_tree
        WHERE parent_id IS NULL

        UNION ALL

        -- 递归查询：子分类
        SELECT c.id, c.parent_id, c.name,
               CONCAT(p.path, ' > ', c.name) as path
        FROM categories_tree c
        INNER JOIN category_path p ON c.parent_id = p.id
    )
    SELECT * FROM category_path
""")
result.show(truncate=False)
```

## 8. 实战：博客文章统计分析

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, \
    count as _count, row_number, rank, dense_rank, lead, lag, \
    when, countDistinct, round as _round, desc
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DemoAdvancedAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# 准备数据
demo_data = [
    (1, "Spark入门教程", "张三", 1500, "2024-01-15", "大数据"),
    (2, "Spark SQL详解", "张三", 2800, "2024-02-28", "大数据"),
    (3, "Spark性能调优", "张三", 4500, "2024-05-18", "大数据"),
    (4, "Hadoop核心原理", "李四", 2300, "2024-02-20", "大数据"),
    (5, "Hive数据仓库", "李四", 800, "2024-03-10", "大数据"),
    (6, "Flink流处理入门", "王五", 3200, "2024-04-05", "流计算"),
    (7, "Kafka消息队列", "王五", 1800, "2024-06-22", "流计算"),
    (8, "Python数据分析", "赵六", 950, "2024-07-30", "数据分析"),
    (9, "Docker容器化部署", "赵六", 1200, "2024-08-12", "运维"),
    (10, "K8s实战", "王五", 2600, "2024-09-01", "运维"),
]

columns = ["id", "title", "author", "views", "publish_date", "category"]
df = spark.createDataFrame(demo_data, columns)
df.createOrReplaceTempView("demos")

# ============ 分析 1：每个分类的文章排名 ============
print("=== 分类内文章排名 ===")
result1 = spark.sql("""
    SELECT
        category,
        title,
        author,
        views,
        RANK() OVER (PARTITION BY category ORDER BY views DESC) as rank_in_category,
        DENSE_RANK() OVER (ORDER BY views DESC) as global_dense_rank
    FROM demos
    ORDER BY category, rank_in_category
""")
result1.show()

# ============ 分析 2：作者月度趋势 ============
print("=== 作者月度浏览量趋势 ===")
result2 = spark.sql("""
    SELECT
        author,
        SUBSTRING(publish_date, 1, 7) as month,
        SUM(views) as monthly_views,
        LAG(SUM(views), 1) OVER (
            PARTITION BY author ORDER BY SUBSTRING(publish_date, 1, 7)
        ) as prev_month_views
    FROM demos
    GROUP BY author, SUBSTRING(publish_date, 1, 7)
    ORDER BY author, month
""")
result2.show()

# ============ 分析 3：作者综合评分 ============
print("=== 作者综合评分 ===")
result3 = spark.sql("""
    WITH author_metrics AS (
        SELECT
            author,
            COUNT(*) as article_count,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            MAX(views) as max_views,
            COUNT(DISTINCT category) as category_count
        FROM demos
        GROUP BY author
    )
    SELECT
        author,
        article_count,
        total_views,
        ROUND(avg_views, 2) as avg_views,
        category_count,
        -- 综合评分公式
        ROUND(
            article_count * 10 +
            total_views * 0.01 +
            category_count * 20,
            2
        ) as score,
        CASE
            WHEN total_views >= 5000 THEN '头部作者'
            WHEN total_views >= 2000 THEN '腰部作者'
            ELSE '新晋作者'
        END as author_level
    FROM author_metrics
    ORDER BY score DESC
""")
result3.show()

# ============ 分析 4：连续增长检测 ============
print("=== 文章浏览量与前一篇对比 ===")
window_author = Window.partitionBy("author").orderBy("publish_date")
result4 = df.withColumn("prev_views",
    lag("views", 1).over(window_author)
).withColumn("growth_rate",
    when(col("prev_views").isNotNull(),
         _round((col("views") - col("prev_views")) / col("prev_views") * 100, 2))
    .otherwise(None)
).select("title", "author", "views", "prev_views", "growth_rate")

result4.orderBy("author", "publish_date").show()

spark.stop()
```

## 9. 总结

```
┌──────────────────────────────────────────────────────────┐
│               Spark SQL 进阶知识总结                       │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. Join 类型：inner/left/right/full/cross/self          │
│     优化策略：Broadcast/Sort Merge/Shuffle Hash           │
│                                                          │
│  2. 子查询：WHERE/HAVING/FROM 子查询                     │
│                                                          │
│  3. 集合操作：UNION/UNION ALL/INTERSECT/EXCEPT          │
│                                                          │
│  4. CASE WHEN：条件表达式，替代多个 IF                    │
│                                                          │
│  5. 窗口函数（重要）：                                    │
│     - 排名：ROW_NUMBER/RANK/DENSE_RANK                  │
│     - 偏移：LEAD/LAG                                    │
│     - 聚合：FIRST_VALUE/LAST_VALUE/NTILE                │
│     - 累计：SUM/AVG + rowsBetween                       │
│                                                          │
│  6. CTE：WITH 子句，提升查询可读性                       │
│     Spark 3.0+ 支持递归 CTE                             │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **下一节**：[05-DataFrame-API.md](05-DataFrame-API.md) — DataFrame API 详解，全面学习所有 Transformations 和 Actions。
