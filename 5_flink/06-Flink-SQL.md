# 06 - Flink SQL：用 SQL 写实时计算

## 一、为什么需要 Flink SQL？

在前面几篇文章中，我们用 DataStream API 写了不少代码。但如果你对 SQL 很熟悉，可能会想：**能不能用 SQL 来写实时计算？**

答案是：**当然可以！**

```
┌─────────────────────────────────────────────────────────────┐
│                  DataStream API vs Flink SQL                 │
│                                                             │
│  DataStream API：                                            │
│  - 灵活，可以实现任何逻辑                                     │
│  - 代码量大，学习成本高                                       │
│  - 需要写 Java/Python 代码                                   │
│                                                             │
│  Flink SQL：                                                 │
│  - 用 SQL 写实时计算，门槛低                                  │
│  - 代码简洁，几行 SQL 搞定                                    │
│  - 自动优化执行计划                                          │
│  - 内置丰富的窗口函数和连接器                                  │
│                                                             │
│  举例：统计每篇文章的浏览量                                    │
│                                                             │
│  DataStream API（Python，约 15 行）：                         │
│  result = stream                                            │
│      .map(lambda v: json.loads(v))                          │
│      .filter(lambda e: e["type"] == "view")                 │
│      .map(lambda e: (e["postId"], 1))                       │
│      .key_by(lambda x: x[0])                                │
│      .window(TumblingEventTimeWindows.of(Duration.of_minutes(1))) │
│      .sum(1)                                                │
│                                                             │
│  Flink SQL（3 行）：                                         │
│  SELECT postId, COUNT(*) AS viewCount                       │
│  FROM demo_events                                           │
│  WHERE type = 'view'                                        │
│  GROUP BY postId, TUMBLE(event_time, INTERVAL '1' MINUTE)   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 二、TableEnvironment

Flink SQL 的入口是 `TableEnvironment`。

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 创建流模式的 TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# 也可以从 DataStream 环境转换
from pyflink.datastream import StreamExecutionEnvironment
stream_env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(stream_env)
```

```
┌─────────────────────────────────────────────────────────────┐
│               TableEnvironment 的能力                        │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │              TableEnvironment                     │      │
│  │                                                  │      │
│  │  execute_sql(sql)        执行任意 SQL             │      │
│  │  sql_query(sql)          执行查询（返回 Table）    │      │
│  │  create_temporary_view() 创建临时视图             │      │
│  │  from_path()             从文件创建表             │      │
│  │  from_elements()         从集合创建表             │      │
│  │                                                  │      │
│  │  配置相关：                                       │      │
│  │  get_config()            获取配置                 │      │
│  │  use_catalog()           切换 Catalog             │      │
│  │  use_database()          切换 Database             │      │
│  │                                                  │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 三、DDL（创建表）

### 3.1 创建 Kafka 表

```python
table_env.execute_sql("""
    CREATE TABLE demo_events (
        type STRING,              -- 事件类型：view/comment/like/search
        postId INT,               -- 文章 ID
        userId STRING,            -- 用户 ID
        content STRING,           -- 事件附加内容（如搜索词）
        event_time TIMESTAMP(3),  -- 事件时间
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'blog-events',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-sql-demo',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
""")
```

```
┌─────────────────────────────────────────────────────────────┐
│               Kafka Connector 配置详解                        │
│                                                             │
│  'connector' = 'kafka'                                       │
│    → 使用 Kafka 连接器                                       │
│                                                             │
│  'topic' = 'blog-events'                                     │
│    → Kafka Topic 名称                                        │
│                                                             │
│  'properties.bootstrap.servers' = 'localhost:9092'           │
│    → Kafka 集群地址                                          │
│                                                             │
│  'properties.group.id' = 'flink-sql-demo'                   │
│    → Consumer Group ID                                      │
│                                                             │
│  'format' = 'json'                                          │
│    → 数据格式为 JSON                                         │
│    → 也支持 'csv', 'avro', 'debezium-json' 等               │
│                                                             │
│  'scan.startup.mode' = 'latest-offset'                      │
│    → 从最新偏移量开始消费                                     │
│    → 可选值：earliest-offset / latest-offset / specific-offset│
│    →        / timestamp / group-offsets                     │
│                                                             │
│  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND│
│    → 定义事件时间和 Watermark 策略                            │
│    → 允许 10 秒的乱序延迟                                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 创建 Elasticsearch 表

```python
table_env.execute_sql("""
    CREATE TABLE demo_stats (
        postId INT,
        viewCount BIGINT,
        commentCount BIGINT,
        likeCount BIGINT,
        windowStart TIMESTAMP(3),
        windowEnd TIMESTAMP(3),
        PRIMARY KEY (postId, windowStart) NOT ENFORCED
    ) WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = 'http://localhost:9200',
        'index' = 'demo-stats',
        'format' = 'json'
    )
""")
```

```
┌─────────────────────────────────────────────────────────────┐
│            Elasticsearch Connector 配置                      │
│                                                             │
│  'connector' = 'elasticsearch-7'                             │
│    → 使用 Elasticsearch 7.x 连接器                           │
│    → Flink 2.x 也支持 elasticsearch-8                       │
│                                                             │
│  'hosts' = 'http://localhost:9200'                           │
│    → ES 集群地址                                             │
│                                                             │
│  'index' = 'demo-stats'                                     │
│    → 索引名称                                                │
│                                                             │
│  PRIMARY KEY (postId, windowStart) NOT ENFORCED              │
│    → 主键定义（用于 upsert）                                  │
│    → NOT ENFORCED 表示不在 Flink 端校验                      │
│    → ES 会根据主键做文档的更新/插入                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 创建视图

```python
# 创建临时视图（只保留浏览事件）
table_env.execute_sql("""
    CREATE TEMPORARY VIEW demo_views AS
    SELECT * FROM demo_events WHERE type = 'view'
""")

# 从查询结果创建视图
table_env.create_temporary_view(
    "search_events",
    table_env.sql_query("""
        SELECT userId, content AS keyword, event_time
        FROM demo_events
        WHERE type = 'search'
    """)
)
```

## 四、常用查询

### 4.1 基础查询

```sql
-- 查看所有事件
SELECT * FROM demo_events;

-- 按事件类型统计
SELECT type, COUNT(*) AS event_count
FROM demo_events
GROUP BY type;

-- 统计每篇文章的浏览量
SELECT postId, COUNT(*) AS view_count
FROM demo_views
GROUP BY postId;
```

### 4.2 窗口聚合

```sql
-- 滚动窗口：每 1 分钟统计每篇文章的浏览量
SELECT
    postId,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS view_count
FROM demo_views
GROUP BY
    postId,
    TUMBLE(event_time, INTERVAL '1' MINUTE);

-- 滑动窗口：每 1 分钟统计最近 5 分钟的热门文章
SELECT
    postId,
    COUNT(*) AS view_count,
    HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
    HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_end
FROM demo_views
GROUP BY
    postId,
    HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);

-- 会话窗口：按用户分组，会话间隔 30 分钟
SELECT
    userId,
    COUNT(*) AS event_count,
    SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '30' MINUTE) AS session_end
FROM demo_events
GROUP BY
    userId,
    SESSION(event_time, INTERVAL '30' MINUTE);
```

```
┌─────────────────────────────────────────────────────────────┐
│               Flink SQL 窗口函数速查                           │
│                                                             │
│  ┌────────────┬────────────────────────────────────┐        │
│  │  窗口类型    │  语法                              │        │
│  ├────────────┼────────────────────────────────────┤        │
│  │  滚动窗口    │  TUMBLE(time, size)               │        │
│  │            │  TUMBLE_START / TUMBLE_END         │        │
│  ├────────────┼────────────────────────────────────┤        │
│  │  滑动窗口    │  HOP(time, slide, size)            │        │
│  │            │  HOP_START / HOP_END               │        │
│  ├────────────┼────────────────────────────────────┤        │
│  │  会话窗口    │  SESSION(time, gap)               │        │
│  │            │  SESSION_START / SESSION_END       │        │
│  ├────────────┼────────────────────────────────────┤        │
│  │  累积窗口    │  CUMULATE(time, step, max_size)   │        │
│  │            │  适合逐渐累积数据的场景              │        │
│  └────────────┴────────────────────────────────────┘        │
│                                                             │
│  注意：                                                       │
│  - 窗口函数必须在 GROUP BY 子句中使用                          │
│  - 窗口的时间字段必须定义了 WATERMARK                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 常用内置函数

```sql
-- 字符串函数
SELECT UPPER(userId), LOWER(type) FROM demo_events;
SELECT CONCAT(userId, '-', postId) FROM demo_events;

-- 时间函数
SELECT
    event_time,
    DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm:ss') AS formatted_time,
    YEAR(event_time) AS year,
    MONTH(event_time) AS month,
    HOUR(event_time) AS hour
FROM demo_events;

-- 聚合函数
SELECT
    postId,
    COUNT(*) AS total_views,          -- 计数
    COUNT(DISTINCT userId) AS uv,     -- 去重计数
    MAX(event_time) AS last_view_time -- 最大值
FROM demo_views
GROUP BY postId;

-- CASE WHEN 条件判断
SELECT
    postId,
    view_count,
    CASE
        WHEN view_count > 100 THEN 'hot'
        WHEN view_count > 50 THEN 'warm'
        ELSE 'normal'
    END AS popularity
FROM (
    SELECT postId, COUNT(*) AS view_count
    FROM demo_views
    GROUP BY postId
);
```

## 五、Catalog 管理

```
┌─────────────────────────────────────────────────────────────┐
│                    Catalog 体系                               │
│                                                             │
│  Catalog（目录）                                              │
│  └── Database（数据库）                                       │
│      └── Table（表）                                         │
│                                                             │
│  类比关系型数据库：                                            │
│  Catalog → Database → Table                                  │
│  相当于：  实例     → 库名     → 表名                          │
│                                                             │
│  Flink 默认有两个 Catalog：                                   │
│  - default_catalog：内存中的默认 Catalog                      │
│  - 自定义 Catalog：如 HiveCatalog、JdbcCatalog 等            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

```python
# 查看当前 Catalog 和 Database
result = table_env.execute_sql("SHOW CATALOGS")
result.print()

result = table_env.execute_sql("SHOW DATABASES")
result.print()

result = table_env.execute_sql("SHOW TABLES")
result.print()

# 创建 Database
table_env.execute_sql("CREATE DATABASE IF NOT EXISTS blog_db")

# 切换 Database
table_env.execute_sql("USE blog_db")

# 切换 Catalog
table_env.execute_sql("USE CATALOG default_catalog")
```

## 六、实战：Kafka 到 Elasticsearch 的实时管道

现在我们把所有知识串起来，构建一个完整的实时数据管道。

### 6.1 完整代码

```python
# demo_sql_pipeline.py
"""
数据平台实时数据管道：Kafka → Flink SQL → Elasticsearch
实时统计每篇文章的浏览量、评论数、点赞数
"""
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # 1. 创建 TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # 启用 Checkpoint
    table_env.execute_sql("""
        SET 'execution.checkpointing.interval' = '30s'
    """)

    # 2. 创建 Kafka 源表
    table_env.execute_sql("""
        CREATE TABLE demo_events (
            type STRING,
            postId INT,
            userId STRING,
            content STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'blog-events',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-sql-pipeline',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # 3. 创建 Elasticsearch 结果表
    table_env.execute_sql("""
        CREATE TABLE demo_stats (
            postId INT,
            viewCount BIGINT,
            commentCount BIGINT,
            likeCount BIGINT,
            windowStart TIMESTAMP(3),
            windowEnd TIMESTAMP(3),
            PRIMARY KEY (postId, windowStart) NOT ENFORCED
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://localhost:9200',
            'index' = 'demo-stats'
        )
    """)

    # 4. 执行实时聚合查询，将结果写入 Elasticsearch
    table_env.execute_sql("""
        INSERT INTO demo_stats
        SELECT
            postId,
            SUM(CASE WHEN type = 'view' THEN 1 ELSE 0 END) AS viewCount,
            SUM(CASE WHEN type = 'comment' THEN 1 ELSE 0 END) AS commentCount,
            SUM(CASE WHEN type = 'like' THEN 1 ELSE 0 END) AS likeCount,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS windowStart,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS windowEnd
        FROM demo_events
        GROUP BY
            postId,
            TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # 注意：INSERT INTO 是异步执行的
    # Flink SQL 作业会持续运行


if __name__ == "__main__":
    main()
```

### 6.2 管道架构图

```
┌─────────────────────────────────────────────────────────────────┐
│              Kafka → Flink SQL → Elasticsearch 管道              │
│                                                                 │
│  ┌──────────────┐                                               │
│  │    Kafka     │  blog-events topic                             │
│  │              │  JSON 格式：{type, postId, userId, event_time}  │
│  └──────┬───────┘                                               │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Flink SQL                            │  │
│  │                                                          │  │
│  │  ┌──────────────────┐    ┌──────────────────────────┐    │  │
│  │  │  demo_events 表   │    │  demo_stats 表           │    │  │
│  │  │  (Kafka Source)   │ →  │  (ES Sink)              │    │  │
│  │  └──────────────────┘    └──────────────────────────┘    │  │
│  │                                                          │  │
│  │  SQL 逻辑：                                              │  │
│  │  INSERT INTO demo_stats                                  │  │
│  │  SELECT                                                  │  │
│  │    postId,                                               │  │
│  │    SUM(CASE WHEN type='view' ...) AS viewCount,          │  │
│  │    SUM(CASE WHEN type='comment' ...) AS commentCount,    │  │
│  │    SUM(CASE WHEN type='like' ...) AS likeCount,          │  │
│  │    TUMBLE_START(...) AS windowStart,                     │  │
│  │    TUMBLE_END(...) AS windowEnd                           │  │
│  │  FROM demo_events                                        │  │
│  │  GROUP BY postId, TUMBLE(event_time, INTERVAL '1' MINUTE)│  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │  Elasticsearch   │  demo-stats 索引                          │
│  │                  │  实时更新，供 Dashboard 查询展示             │
│  └──────────────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.3 验证管道

```bash
# 1. 确保 Kafka 正在运行
kafka-topics --bootstrap-server localhost:9092 --list

# 2. 确保 Elasticsearch 正在运行（如果已安装）
curl http://localhost:9200/_cluster/health

# 3. 启动 Flink 集群
start-cluster.sh

# 4. 提交 SQL 作业
flink run -py demo_sql_pipeline.py

# 5. 发送测试数据
kafka-console-producer --bootstrap-server localhost:9092 --topic blog-events
# 输入测试数据：
# {"type":"view","postId":1,"userId":"alice","event_time":"2024-04-15 10:00:00"}
# {"type":"view","postId":1,"userId":"bob","event_time":"2024-04-15 10:00:01"}
# {"type":"comment","postId":1,"userId":"charlie","event_time":"2024-04-15 10:00:02"}

# 6. 在 Elasticsearch 中查看结果
curl http://localhost:9200/demo-stats/_search
```

## 七、面试题

### Q1：Flink SQL 和 DataStream API 如何互相转换？

**参考答案：**

Flink SQL 和 DataStream API 可以无缝互转：

**DataStream → Table：**
```python
# 通过 TableEnvironment 将 DataStream 转为 Table
table = table_env.from_data_stream(data_stream)

# 也可以注册为视图
table_env.create_temporary_view("my_table", data_stream)
```

**Table → DataStream：**
```python
# 将 Table 转为 DataStream（追加模式）
stream = table_env.to_data_stream(table)

# 将 Table 转为 DataStream（撤回模式，适合聚合结果）
stream = table_env.to_retract_stream(table)
```

这种互转能力使得我们可以用 SQL 做简单聚合，用 DataStream API 做复杂逻辑，灵活组合。

### Q2：Flink SQL 的动态表（Dynamic Table）是什么概念？

**参考答案：**

动态表是 Flink SQL 的核心概念。与关系型数据库中的静态表不同，动态表的数据会随着时间不断变化。

核心思想：
1. 流式数据被转换为动态表（随时间增长的表）
2. 对动态表执行 SQL 查询会产生新的动态表
3. 动态表最终被转换为流式输出

动态表有三种变更模式：
- **Append 模式**：只有插入，没有更新和删除（如简单的数据过滤）
- **Retract 模式**：有插入（+I）、更新（-U/+I）、删除（-D）
- **Upsert 模式**：有插入和更新（+I/+U），适合有主键的聚合场景

### Q3：Flink SQL 中如何处理迟到数据？

**参考答案：**

在 Flink SQL 中，可以通过以下方式处理迟到数据：

```sql
-- 1. 设置 Watermark 的延迟（在 DDL 中）
WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND

-- 2. 设置窗口允许的迟到时间（通过 Table API）
table_env.execute_sql("""
    SET 'table.exec.state.ttl' = '1h'
""")

-- 3. 对于超过 Watermark 延迟的迟到数据，可以配置不同的策略
-- 通过 SQL Hint 或配置参数：
SET 'table.exec.source.idle-timeout' = '5min'
```

## 八、总结

本篇我们学习了 Flink SQL 的使用，关键要点：

1. **TableEnvironment**：Flink SQL 的入口，创建和管理表
2. **DDL**：用 `CREATE TABLE` 定义 Kafka、Elasticsearch 等连接器
3. **窗口聚合**：TUMBLE（滚动）、HOP（滑动）、SESSION（会话）
4. **动态表**：Flink SQL 的核心概念，流数据被抽象为不断变化的表
5. **实战管道**：Kafka → Flink SQL → Elasticsearch，几行 SQL 搞定

> **下一篇：[07-数据平台实时管道实战.md](07-数据平台实时管道实战.md) — 数据分析项目实战**
