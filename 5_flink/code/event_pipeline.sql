-- ==============================================================================
-- 博客事件实时统计 —— Flink SQL 脚本
-- ==============================================================================
-- 功能：
--   1. 创建 Kafka Source 表（读取博客事件）
--   2. 创建 Print Sink 表（输出统计结果）
--   3. 按事件类型统计（滚动窗口）
--   4. 每篇文章的访问量统计（5 分钟滚动窗口）
--   5. 搜索热词统计（5 分钟滚动窗口）
--
-- 使用方式：
--   方式一：通过 Flink SQL CLI 执行
--     sql-client.sh -f blog_pipeline.sql
--
--   方式二：通过 PyFlink 执行
--     table_env.execute_sql("source blog_pipeline.sql")  # 逐条执行
--
-- 前置条件：
--   - Flink 2.2.0 Standalone 集群已启动
--   - Kafka 4.2.0 已启动，且 blog-events Topic 已创建
-- ==============================================================================

-- ==============================================================================
-- 1. 配置 Flink 参数
-- ==============================================================================

-- 设置 Checkpoint 间隔为 30 秒
SET 'execution.checkpointing.interval' = '30s';

-- 设置并行度为 2
SET 'parallelism.default' = '2';

-- ==============================================================================
-- 2. 创建 Kafka Source 表（读取博客事件）
-- ==============================================================================

-- 使用 DATASTREAM 模式直接消费 Kafka 中的原始 JSON
-- 每条消息包含 eventType 和 data 两个字段
CREATE TABLE IF NOT EXISTS demo_events (
    -- 事件类型：view / comment / like / search
    `type` STRING,

    -- 文章 ID（搜索事件时为 null）
    postId INT,

    -- 用户 ID
    userId STRING,

    -- 事件附加内容（评论内容或搜索关键词）
    content STRING,

    -- 事件时间（ISO 8601 格式）
    event_time TIMESTAMP(3),

    -- Watermark 定义：允许 10 秒的乱序延迟
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'blog-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-sql-demo-stats',
    -- JSON 格式
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    -- 从最新偏移量开始消费
    'scan.startup.mode' = 'latest-offset'
);

-- ==============================================================================
-- 3. 创建 Print Sink 表（输出到控制台）
-- ==============================================================================

-- 事件类型统计结果表
CREATE TABLE IF NOT EXISTS event_type_stats (
    metric_name STRING,    -- 指标名称
    event_type STRING,     -- 事件类型
    event_count BIGINT,     -- 事件数量
    window_start TIMESTAMP(3),  -- 窗口开始时间
    window_end TIMESTAMP(3)    -- 窗口结束时间
) WITH (
    'connector' = 'print'
);

-- 文章浏览量统计结果表
CREATE TABLE IF NOT EXISTS post_view_stats (
    post_id INT,               -- 文章 ID
    view_count BIGINT,          -- 浏览次数
    window_start TIMESTAMP(3),  -- 窗口开始时间
    window_end TIMESTAMP(3)     -- 窗口结束时间
) WITH (
    'connector' = 'print'
);

-- 搜索关键词统计结果表
CREATE TABLE IF NOT EXISTS search_keyword_stats (
    keyword STRING,             -- 搜索关键词
    search_count BIGINT,        -- 搜索次数
    window_start TIMESTAMP(3),  -- 窗口开始时间
    window_end TIMESTAMP(3)     -- 窗口结束时间
) WITH (
    'connector' = 'print'
);

-- ==============================================================================
-- 4. 查询一：按事件类型统计（5 分钟滚动窗口）
-- ==============================================================================

INSERT INTO event_type_stats
SELECT
    '事件类型统计' AS metric_name,
    `type` AS event_type,
    COUNT(*) AS event_count,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end
FROM demo_events
GROUP BY
    `type`,
    TUMBLE(event_time, INTERVAL '5' MINUTE);

-- ==============================================================================
-- 5. 查询二：每篇文章的访问量统计（5 分钟滚动窗口）
-- ==============================================================================

INSERT INTO post_view_stats
SELECT
    postId AS post_id,
    COUNT(*) AS view_count,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end
FROM demo_events
WHERE `type` = 'view'
GROUP BY
    postId,
    TUMBLE(event_time, INTERVAL '5' MINUTE);

-- ==============================================================================
-- 6. 查询三：搜索关键词热词统计（5 分钟滚动窗口）
-- ==============================================================================

INSERT INTO search_keyword_stats
SELECT
    content AS keyword,
    COUNT(*) AS search_count,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end
FROM demo_events
WHERE `type` = 'search'
GROUP BY
    content,
    TUMBLE(event_time, INTERVAL '5' MINUTE);
