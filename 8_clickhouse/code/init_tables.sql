-- ============================================================
-- 数据平台 ClickHouse 建表脚本
-- 说明：创建博客数据库、访问日志表、统计表和物化视图
-- ============================================================

-- 创建博客数据库
CREATE DATABASE IF NOT EXISTS demo;

-- ============================================================
-- 1. 访问日志明细表（MergeTree 引擎）
-- 说明：存储所有用户访问行为明细数据，按天分区，按时间戳排序
-- ============================================================
CREATE TABLE IF NOT EXISTS demo.access_logs
(
    log_id     UInt64,           -- 日志唯一ID
    user_id    UInt32,           -- 用户ID（0表示匿名访客）
    post_id    UInt32,           -- 文章ID（0表示非文章页面）
    action     Enum8('view' = 1, 'click' = 2, 'comment' = 3, 'like' = 4, 'share' = 5, 'search' = 6),  -- 用户行为类型
    ip         String,           -- 访问者IP地址
    city       LowCardinality(String),  -- 城市（LowCardinality 适合低基数字符串）
    device     LowCardinality(String),  -- 设备类型：desktop / mobile / tablet
    timestamp  DateTime          -- 访问时间戳
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)   -- 按天分区，方便按日期范围查询和数据清理
ORDER BY (timestamp, user_id, post_id)  -- 排序键：时间 + 用户 + 文章，覆盖常见查询模式
SETTINGS index_granularity = 8192;  -- 索引粒度，每个数据块8192行

-- ============================================================
-- 2. 每日访问统计表（SummingMergeTree 引擎）
-- 说明：按天自动合并统计指标，适合实时聚合场景
-- ============================================================
CREATE TABLE IF NOT EXISTS demo.blog_stats
(
    date        Date,             -- 统计日期
    city        LowCardinality(String),  -- 城市
    device      LowCardinality(String),  -- 设备类型
    pv          UInt64,           -- 页面浏览量
    uv          UInt64,           -- 独立访客数
    comment_cnt UInt64,           -- 评论数
    like_cnt    UInt64,           -- 点赞数
    share_cnt   UInt64            -- 分享数
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, city, device)
SETTINGS index_granularity = 8192;

-- ============================================================
-- 3. 文章热度统计表（SummingMergeTree 引擎）
-- 说明：按文章聚合浏览、评论、点赞等数据
-- ============================================================
CREATE TABLE IF NOT EXISTS demo.post_stats
(
    post_id     UInt32,           -- 文章ID
    date        Date,             -- 统计日期
    view_cnt    UInt64,           -- 浏览量
    comment_cnt UInt64,           -- 评论数
    like_cnt    UInt64,           -- 点赞数
    share_cnt   UInt64            -- 分享数
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (post_id, date)
SETTINGS index_granularity = 8192;

-- ============================================================
-- 4. 数据平台每日汇总统计表（ReplacingMergeTree 引擎）
-- 说明：每天只有一条记录，更新时自动替换旧版本
-- ============================================================
CREATE TABLE IF NOT EXISTS demo.demo_daily_summary
(
    date            Date,         -- 日期
    total_pv        UInt64,       -- 总PV
    total_uv        UInt64,       -- 总UV
    new_users       UInt64,       -- 新增用户
    new_posts       UInt64,       -- 新增文章
    total_comments  UInt64,       -- 总评论
    total_likes     UInt64,       -- 总点赞
    bounce_rate     Float32,      -- 跳出率
    avg_duration    UInt32        -- 平均访问时长（秒）
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYY(date)
ORDER BY date
SETTINGS index_granularity = 8192;

-- ============================================================
-- 5. 物化视图：从访问日志自动聚合到每日统计表
-- 说明：当日志写入 access_logs 时，自动更新 blog_stats
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS demo.mv_blog_stats
TO demo.blog_stats
AS
SELECT
    toDate(timestamp)    AS date,
    city,
    device,
    count()              AS pv,
    count(DISTINCT user_id) AS uv,
    countIf(action = 'comment')  AS comment_cnt,
    countIf(action = 'like')     AS like_cnt,
    countIf(action = 'share')    AS share_cnt
FROM demo.access_logs
GROUP BY date, city, device;

-- ============================================================
-- 6. 物化视图：从访问日志自动聚合到文章热度统计表
-- 说明：当日志写入 access_logs 时，自动更新 post_stats
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS demo.mv_post_stats
TO demo.post_stats
AS
SELECT
    post_id,
    toDate(timestamp)    AS date,
    countIf(action = 'view')    AS view_cnt,
    countIf(action = 'comment') AS comment_cnt,
    countIf(action = 'like')    AS like_cnt,
    countIf(action = 'share')   AS share_cnt
FROM demo.access_logs
WHERE post_id > 0
GROUP BY post_id, date;
