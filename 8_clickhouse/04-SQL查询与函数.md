# 04 - SQL 查询与函数：ClickHouse 的查询能力全解

## 一、基础查询

ClickHouse 的 SQL 语法和 MySQL 非常相似，如果你用过 MySQL，上手会很快。但 ClickHouse 有一些自己的特色和限制。

### 1.1 SELECT 基础

```sql
-- 基本查询
SELECT * FROM demo.access_logs LIMIT 10;

-- 指定列查询
SELECT user_id, post_id, action, city, timestamp
FROM demo.access_logs
LIMIT 10;

-- WHERE 条件过滤
SELECT *
FROM demo.access_logs
WHERE city = '北京'
  AND action = 'view'
  AND timestamp >= '2026-04-01 00:00:00'
ORDER BY timestamp DESC
LIMIT 20;

-- DISTINCT 去重
SELECT DISTINCT city FROM demo.access_logs;

-- IN 查询
SELECT * FROM demo.access_logs
WHERE city IN ('北京', '上海', '广州')
LIMIT 10;
```

### 1.2 GROUP BY 聚合

```sql
-- 基础聚合
SELECT
    city,
    count()              AS pv,
    count(DISTINCT user_id) AS uv
FROM demo.access_logs
GROUP BY city
ORDER BY pv DESC;

-- 多维度聚合
SELECT
    city,
    device,
    count()              AS pv,
    count(DISTINCT user_id) AS uv
FROM demo.access_logs
GROUP BY city, device
ORDER BY pv DESC;

-- HAVING 过滤（聚合后过滤）
SELECT
    city,
    count() AS pv
FROM demo.access_logs
GROUP BY city
HAVING pv > 100
ORDER BY pv DESC;
```

### 1.3 ORDER BY 排序

```sql
-- 升序排序
SELECT city, count() AS pv
FROM demo.access_logs
GROUP BY city
ORDER BY pv ASC;

-- 降序排序
SELECT city, count() AS pv
FROM demo.access_logs
GROUP BY city
ORDER BY pv DESC;

-- 多列排序
SELECT city, device, count() AS pv
FROM demo.access_logs
GROUP BY city, device
ORDER BY city ASC, pv DESC;

-- LIMIT 限制结果数量
SELECT * FROM demo.access_logs ORDER BY timestamp DESC LIMIT 10;

-- LIMIT WITH TIES（包含并列的行）
SELECT city, count() AS pv
FROM demo.access_logs
GROUP BY city
ORDER BY pv DESC
LIMIT 3 WITH TIES;
```

## 二、ClickHouse 特有函数

### 2.1 时间函数

```sql
-- toDateTime：字符串转 DateTime
SELECT toDateTime('2026-04-01 08:30:00');

-- toDate：提取日期部分
SELECT toDate('2026-04-01 08:30:00');      -- 2026-04-01
SELECT toDate(timestamp) FROM demo.access_logs;

-- toYYYYMM / toYYYYMMDD：日期转年月/年月日
SELECT toYYYYMM(timestamp) FROM demo.access_logs;
SELECT toYYYYMMDD(timestamp) FROM demo.access_logs;

-- formatDateTime：格式化日期
SELECT formatDateTime(now(), '%Y-%m-%d %H:%i:%s');

-- 时间加减
SELECT now() + INTERVAL 1 DAY;
SELECT now() - INTERVAL 7 DAY;
SELECT now() + INTERVAL 1 HOUR;

-- 提取时间部分
SELECT toHour(timestamp)    AS hour FROM demo.access_logs;
SELECT toMinute(timestamp)  AS minute FROM demo.access_logs;
SELECT toDayOfWeek(timestamp) AS weekday FROM demo.access_logs;
SELECT toStartOfDay(timestamp) AS day_start FROM demo.access_logs;
SELECT toStartOfMonth(timestamp) AS month_start FROM demo.access_logs;
```

### 2.2 条件聚合函数（xxxIf 系列）

这是 ClickHouse 非常实用的特色函数：

```sql
-- countIf：条件计数
SELECT
    toDate(timestamp) AS date,
    count()                              AS total,
    countIf(action = 'view')             AS views,     -- 只统计 view
    countIf(action = 'comment')          AS comments,  -- 只统计 comment
    countIf(action = 'like')             AS likes,     -- 只统计 like
    countIf(action = 'share')            AS shares     -- 只统计 share
FROM demo.access_logs
GROUP BY date
ORDER BY date;

-- sumIf：条件求和
SELECT
    city,
    sumIf(1, action = 'view')            AS pv,
    sumIf(1, action = 'like')            AS likes
FROM demo.access_logs
GROUP BY city;

-- avgIf：条件平均
SELECT
    post_id,
    avgIf(1, action = 'view')            AS avg_views
FROM demo.access_logs
GROUP BY post_id;
```

```
┌──────────────────────────────────────────────────────────────────┐
│                    xxxIf 函数 vs CASE WHEN                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  传统写法（CASE WHEN）：                                          │
│  SELECT                                                          │
│      count(CASE WHEN action = 'view' THEN 1 END) AS views,       │
│      count(CASE WHEN action = 'like' THEN 1 END) AS likes        │
│  FROM access_logs;                                               │
│                                                                  │
│  ClickHouse 写法（countIf）：                                     │
│  SELECT                                                          │
│      countIf(action = 'view') AS views,                          │
│      countIf(action = 'like') AS likes                           │
│  FROM access_logs;                                               │
│                                                                  │
│  countIf 更简洁、更直观、性能更好（向量化优化）                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.3 唯一值统计函数

```sql
-- uniqExact：精确去重计数（准确但慢）
SELECT count(DISTINCT user_id) FROM demo.access_logs;
-- 等价于
SELECT uniqExact(user_id) FROM demo.access_logs;

-- uniq：近似去重计数（HyperLogLog 算法，快但可能有小误差）
SELECT uniq(user_id) FROM demo.access_logs;

-- uniqCombined：更好的近似算法（推荐）
SELECT uniqCombined(user_id) FROM demo.access_logs;

-- 精度对比：
-- uniqExact(user_id)    → 精确结果，大数据量时慢
-- uniq(user_id)         → 误差 ~2%，速度快
-- uniqCombined(user_id) → 误差 ~0.5%，速度快
```

```
┌──────────────────────────────────────────────────────────────────┐
│                    uniqExact vs uniq 的选择                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  场景选择指南：                                                   │
│                                                                  │
│  ├── 需要精确结果（如财务统计）                                   │
│  │   └── 用 uniqExact                                            │
│  │                                                              │
│  ├── 数据量极大（亿级），允许小误差                               │
│  │   └── 用 uniq 或 uniqCombined                                 │
│  │                                                              │
│  └── 不确定？                                                    │
│      └── 先用 uniq（快），需要精确时再用 uniqExact               │
│                                                                  │
│  注意：ClickHouse 中 count(DISTINCT x) 等价于 uniqExact(x)       │
│  大数据量时建议显式使用 uniq() 以获得更好的性能                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.4 其他实用函数

```sql
-- 字符串函数
SELECT length('hello');                -- 5
SELECT lower('HELLO');                 -- hello
SELECT upper('hello');                 -- HELLO
SELECT concat('hello', ' ', 'world'); -- hello world
SELECT substring('hello world', 1, 5);-- hello
SELECT empty('');                      -- 1 (true)
SELECT notEmpty('hello');              -- 1 (true)

-- 条件函数
SELECT if(1 > 0, 'yes', 'no');        -- yes
SELECT multiIf(1 > 2, 'a', 1 > 0, 'b', 'c'); -- b

-- 类型转换
SELECT toString(123);                  -- '123'
SELECT toUInt32('123');               -- 123
SELECT toInt32('123');                -- 123
SELECT toFloat64('3.14');             -- 3.14

-- 数学函数
SELECT round(3.14159, 2);             -- 3.14
SELECT floor(3.9);                    -- 3
SELECT ceil(3.1);                     -- 4
SELECT abs(-5);                       -- 5

-- 格式化输出
SELECT formatReadableSize(1024 * 1024);   -- 1.00 MiB
SELECT formatReadableQuantity(1234567);   -- 1.23 million
SELECT formatReadableTimeDelta(3600);     -- 3600
```

## 三、窗口函数

ClickHouse 从 21.3 版本开始支持窗口函数，语法和标准 SQL 一致：

```sql
-- ROW_NUMBER：行号
SELECT
    post_id,
    city,
    countIf(action = 'view') AS views,
    row_number() OVER (PARTITION BY post_id ORDER BY views DESC) AS rank
FROM demo.access_logs
WHERE post_id > 0
GROUP BY post_id, city
ORDER BY post_id, rank
LIMIT 30;

-- RANK：排名（相同值并列，后续跳号）
SELECT
    user_id,
    count() AS actions,
    rank() OVER (ORDER BY actions DESC) AS rank
FROM demo.access_logs
WHERE user_id > 0
GROUP BY user_id
ORDER BY rank
LIMIT 10;

-- SUM OVER：累计求和
SELECT
    date,
    pv,
    sum(pv) OVER (ORDER BY date) AS cumulative_pv
FROM (
    SELECT
        toDate(timestamp) AS date,
        count() AS pv
    FROM demo.access_logs
    GROUP BY date
)
ORDER BY date;

-- LAG / LEAD：取前/后一行
SELECT
    date,
    pv,
    lagInFrame(pv) OVER (ORDER BY date) AS prev_day_pv
FROM (
    SELECT
        toDate(timestamp) AS date,
        count() AS pv
    FROM demo.access_logs
    GROUP BY date
)
ORDER BY date;
```

```
┌──────────────────────────────────────────────────────────────────┐
│                    ClickHouse 窗口函数注意事项                       │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. ClickHouse 的窗口函数使用 lagInFrame / leadInFrame             │
│     而不是标准 SQL 的 LAG / LEAD                                  │
│                                                                  │
│  2. 窗口函数的性能不如 GROUP BY 快                                │
│     如果能用 GROUP BY 解决，优先用 GROUP BY                       │
│                                                                  │
│  3. 窗口函数不支持嵌套（窗口内不能再有窗口）                        │
│                                                                  │
│  4. 常用窗口函数：                                                │
│     row_number() - 行号（不重复）                                 │
│     rank()        - 排名（重复，跳号）                             │
│     dense_rank()  - 排名（重复，不跳号）                           │
│     sum() OVER()  - 累计求和                                      │
│     avg() OVER()  - 移动平均                                      │
│     lagInFrame()  - 前一行值                                      │
│     leadInFrame() - 后一行值                                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 四、JOIN 操作

ClickHouse 支持 JOIN，但性能不如专门的 MPP 数据库（如 Doris）。建议优先通过数据冗余来避免 JOIN。

### 4.1 基础 JOIN

```sql
-- 假设有一张文章信息表
CREATE TABLE demo.posts_info (
    post_id    UInt32,
    title      String,
    author     String,
    category   String
)
ENGINE = MergeTree()
ORDER BY post_id;

-- INNER JOIN：只保留匹配的行
SELECT
    a.post_id,
    p.title,
    p.author,
    countIf(a.action = 'view') AS views
FROM demo.access_logs a
INNER JOIN demo.posts_info p ON a.post_id = p.post_id
GROUP BY a.post_id, p.title, p.author
ORDER BY views DESC
LIMIT 10;

-- LEFT JOIN：保留左表所有行
SELECT
    p.post_id,
    p.title,
    countIf(a.action = 'view') AS views
FROM demo.posts_info p
LEFT JOIN demo.access_logs a ON p.post_id = a.post_id
GROUP BY p.post_id, p.title
ORDER BY views DESC
LIMIT 10;
```

### 4.2 JOIN 类型

| JOIN 类型 | 说明 |
|-----------|------|
| INNER JOIN | 只保留两边都匹配的行 |
| LEFT JOIN | 保留左表所有行 |
| RIGHT JOIN | 保留右表所有行 |
| FULL JOIN | 保留两表所有行 |
| CROSS JOIN | 笛卡尔积 |
| LEFT SEMI JOIN | 只判断左表行是否在右表存在，不返回右表列 |
| LEFT ANTI JOIN | 只返回不在右表的左表行 |

### 4.3 JOIN 性能优化建议

```
┌──────────────────────────────────────────────────────────────────┐
│                    ClickHouse JOIN 优化建议                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 避免大表 JOIN 大表                                            │
│     ClickHouse 的 JOIN 不如 Doris/StarRocks 高效                  │
│                                                                  │
│  2. 小表放右边（驱动表原则）                                       │
│     SELECT ... FROM big_table JOIN small_table ON ...            │
│     小表会被加载到内存，加速 JOIN                                  │
│                                                                  │
│  3. 用数据冗余代替 JOIN                                           │
│     把需要 JOIN 的字段预先写入主表                                 │
│     例如：在日志表中冗余存储文章标题                               │
│                                                                  │
│  4. 使用字典（Dictionary）代替小表 JOIN                            │
│     字典会被缓存到内存，查询速度更快                               │
│                                                                  │
│  5. 使用 GLOBAL JOIN 避免分布式环境下的重复数据                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 五、物化视图（Materialized View）

物化视图是 ClickHouse 最强大的特性之一，可以实现**实时数据预聚合**。

### 5.1 什么是物化视图？

```
┌──────────────────────────────────────────────────────────────────┐
│                    物化视图原理                                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  普通视图（View）：不存储数据，只是一个查询别名                     │
│  每次查询都重新执行底层 SQL，不快                                  │
│                                                                  │
│  物化视图（Materialized View）：实际存储数据！                     │
│  当源表写入数据时，自动触发物化视图的聚合逻辑                       │
│  并将聚合结果写入目标表                                           │
│                                                                  │
│  ┌──────────────┐     写入触发      ┌──────────────────────┐     │
│  │  源表         │────────────────→ │  物化视图              │     │
│  │ access_logs  │                  │  自动执行 GROUP BY    │     │
│  │ (明细数据)    │                  │  并写入目标表          │     │
│  └──────────────┘                  └──────────┬───────────┘     │
│                                               │                  │
│                                               ▼                  │
│                                    ┌──────────────────────┐     │
│                                    │  目标表                │     │
│                                    │  daily_stats          │     │
│                                    │  (预聚合结果)          │     │
│                                    └──────────────────────┘     │
│                                                                  │
│  查询预聚合结果：秒级返回！                                        │
│  SELECT * FROM daily_stats                                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.2 创建物化视图

```sql
-- 步骤1：先创建目标表（存储聚合结果）
CREATE TABLE demo.daily_stats (
    date        Date,
    city        LowCardinality(String),
    device      LowCardinality(String),
    pv          UInt64,
    uv          UInt64,
    comment_cnt UInt64,
    like_cnt    UInt64,
    share_cnt   UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, city, device);

-- 步骤2：创建物化视图（从源表到目标表的自动聚合管道）
CREATE MATERIALIZED VIEW demo.mv_daily_stats
TO demo.daily_stats
AS
SELECT
    toDate(timestamp)                AS date,
    city,
    device,
    count()                          AS pv,
    count(DISTINCT user_id)          AS uv,
    countIf(action = 'comment')      AS comment_cnt,
    countIf(action = 'like')         AS like_cnt,
    countIf(action = 'share')        AS share_cnt
FROM demo.access_logs
GROUP BY date, city, device;
```

### 5.3 物化视图的工作流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    物化视图工作流程                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 用户插入数据到源表：                                          │
│     INSERT INTO access_logs VALUES (1, 1001, 1, 'view', ...);   │
│                                                                  │
│  2. ClickHouse 自动触发物化视图的 SELECT 查询：                    │
│     SELECT date, city, device, count(), ...                      │
│     FROM access_logs WHERE [新插入的数据]                         │
│     GROUP BY date, city, device;                                 │
│                                                                  │
│  3. 将聚合结果写入目标表：                                        │
│     INSERT INTO daily_stats VALUES ('2026-04-01', '北京', ...);  │
│                                                                  │
│  4. 后台 SummingMergeTree 自动合并相同排序键的数据：               │
│     10:00 的 pv=50 + 10:01 的 pv=20 → pv=70                     │
│                                                                  │
│  5. 查询时直接读目标表（已预聚合），不需要扫描源表：                 │
│     SELECT date, sum(pv), sum(uv) FROM daily_stats               │
│     GROUP BY date;                                               │
│     → 极快！                                                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.4 物化视图的注意事项

```
┌──────────────────────────────────────────────────────────────────┐
│                    物化视图注意事项                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 物化视图不能修改！                                            │
│     要修改需要 DROP 后重建，已聚合的数据不会重新计算               │
│                                                                  │
│  2. 源表已有的数据不会自动触发物化视图                            │
│     只有物化视图创建之后新写入的数据才会触发聚合                    │
│     如果需要处理历史数据，手动 INSERT INTO 目标表                   │
│                                                                  │
│  3. 目标表推荐使用 SummingMergeTree                               │
│     这样后台合并时会自动把相同键的数值求和                         │
│                                                                  │
│  4. 物化视图会增加写入延迟                                         │
│     每次写入源表都要额外执行一次聚合查询                           │
│     但查询加速的效果远大于写入开销                                  │
│                                                                  │
│  5. 一个源表可以创建多个物化视图                                   │
│     不同的物化视图可以有不同的聚合粒度                             │
│     例如：按天聚合 + 按小时聚合 + 按文章聚合                       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 六、数据分析项目：访问日志分析 SQL 示例

### 6.1 每日 PV/UV 趋势

```sql
-- 从明细表查询（精确）
SELECT
    toDate(timestamp)                    AS date,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes,
    countIf(action = 'share')            AS shares
FROM demo.access_logs
GROUP BY date
ORDER BY date;

-- 从物化视图查询（更快）
SELECT
    date,
    sum(pv)                              AS pv,
    sum(uv)                              AS uv,
    sum(comment_cnt)                     AS comments,
    sum(like_cnt)                        AS likes,
    sum(share_cnt)                       AS shares
FROM demo.daily_stats
GROUP BY date
ORDER BY date;
```

### 6.2 文章热度排行

```sql
SELECT
    post_id,
    countIf(action = 'view')             AS views,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes,
    countIf(action = 'share')            AS shares
FROM demo.access_logs
WHERE post_id > 0
GROUP BY post_id
ORDER BY views DESC
LIMIT 10;
```

### 6.3 用户地域分布

```sql
SELECT
    city,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pv_percent
FROM demo.access_logs
GROUP BY city
ORDER BY pv DESC;
```

### 6.4 设备类型分布

```sql
SELECT
    device,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pv_percent
FROM demo.access_logs
GROUP BY device
ORDER BY pv DESC;
```

### 6.5 每小时活跃趋势

```sql
SELECT
    toHour(timestamp)                    AS hour,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv
FROM demo.access_logs
GROUP BY hour
ORDER BY hour;
```

### 6.6 活跃用户排行

```sql
SELECT
    user_id,
    count()                              AS total_actions,
    countIf(action = 'view')             AS views,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes,
    min(toDate(timestamp))               AS first_visit,
    max(toDate(timestamp))               AS last_visit
FROM demo.access_logs
WHERE user_id > 0
GROUP BY user_id
ORDER BY total_actions DESC
LIMIT 10;
```

## 七、面试题

### 面试题 1：ClickHouse 为什么查询这么快？

**参考答案：**

ClickHouse 查询快的原因可以从多个层面分析：

**存储层面**：
1. **列式存储**：只读取查询涉及的列，大幅减少 IO。分析查询通常只涉及少数列，列存储的 IO 量可能只有行存储的 1/10。
2. **高压缩率**：同类型数据连续存储，配合 LZ4/ZSTD 压缩，压缩率通常 10-20 倍，进一步减少 IO。
3. **稀疏索引**：MergeTree 的主键索引每 8192 行记录一个条目，索引本身很小，可以完全缓存在内存中。
4. **分区裁剪**：按时间分区，查询时自动跳过不相关分区。

**计算层面**：
5. **向量化执行**：利用 CPU SIMD（SSE/AVX）指令，一次处理 64/256 个数据，而不是逐行处理。吞吐量提升 10-100 倍。
6. **多线程并行**：自动利用所有 CPU 核心，并行扫描和处理数据。
7. **代码生成（LLVM JIT）**：对热点查询进行 JIT 编译优化。

**系统层面**：
8. **C++ 编写**：贴近硬件，无 GC 暂停，内存管理精确可控。
9. **OS Page Cache**：利用操作系统的文件缓存，热数据自动缓存在内存。

---

### 面试题 2：ClickHouse 的物化视图和普通视图有什么区别？

**参考答案：**

**普通视图（VIEW）**：不存储任何数据，只是保存了一段 SELECT 查询的 SQL 文本。每次查询视图时，实际上是在执行视图定义的 SQL。性能没有任何提升，只是方便了查询编写。

**物化视图（MATERIALIZED VIEW）**：实际存储了聚合后的数据。当源表有新数据写入时，ClickHouse 会自动触发物化视图的查询逻辑，将聚合结果写入目标表。查询时直接读取目标表的预聚合数据，速度极快。

关键区别：
- 普通视图不存数据，物化视图存数据
- 普通视图不加速查询，物化视图大幅加速聚合查询
- 普通视图可以随意修改，物化视图创建后不能修改（需 DROP 重建）
- 物化视图需要指定目标表（TO table），普通视图不需要

---

### 面试题 3：countIf 和 CASE WHEN 哪个更好？

**参考答案：**

在 ClickHouse 中，推荐使用 `countIf`，原因如下：

**性能**：`countIf` 是 ClickHouse 的原生向量化函数，内部使用 SIMD 指令优化，性能比 `CASE WHEN` 更好。

**可读性**：`countIf(action = 'view')` 比 `count(CASE WHEN action = 'view' THEN 1 END)` 更简洁直观。

**ClickHouse 还提供了完整的 xxxIf 系列函数**：`sumIf`、`avgIf`、`minIf`、`maxIf` 等，都是针对条件聚合优化的。

如果使用 `CASE WHEN`，ClickHouse 虽然也能执行，但无法利用向量化优化的路径，性能会有明显差距。在大数据量下，这个差距可能达到数倍。

---

## 八、总结

```
┌──────────────────────────────────────────────────────────────────┐
│                      本章要点回顾                                  │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. ClickHouse SQL 语法类似 MySQL，但有一些特有函数               │
│  2. countIf/sumIf 等条件聚合函数是 ClickHouse 的特色              │
│  3. uniq/uniqCombined 用于近似去重计数，大数据量下性能更好        │
│  4. 窗口函数支持 row_number/rank/lag 等                          │
│  5. JOIN 能力可用但不是强项，建议通过数据冗余避免 JOIN            │
│  6. 物化视图是 ClickHouse 的核心特性，实现实时数据预聚合           │
│  7. 查询物化视图的目标表比扫描源表快几个数量级                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[03-表引擎与数据类型.md](03-表引擎与数据类型.md)**
>
> **下一篇：[05-数据分析项目实战.md](05-数据分析项目实战.md) -- 数据平台访问日志分析完整实战**
