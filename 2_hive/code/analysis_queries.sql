-- ============================================================
-- 博客数据数仓分析查询示例
-- 环境：Hive 4.0.1 + MySQL Metastore + HDFS
-- 前置条件：已执行 init_warehouse.sql 完成数据初始化
-- 使用方式：beeline -u "jdbc:hive2://localhost:10000/blog_db" -f analysis_queries.sql
-- ============================================================

USE blog_db;

-- ============================================================
-- 1. 每日 PV/UV 统计
-- PV（Page View）= 总访问次数
-- UV（Unique Visitor）= 独立访客数
-- ============================================================

-- 1.1 每日 PV 统计（按行为类型细分）
SELECT
    log_date                                        AS 统计日期,
    action                                          AS 行为类型,
    COUNT(*)                                        AS PV_访问次数
FROM access_logs
WHERE action IN ('view', 'comment', 'like')
GROUP BY log_date, action
ORDER BY log_date, PV_访问次数 DESC;

-- 1.2 每日 UV 统计（按登录用户去重）
SELECT
    log_date                                        AS 统计日期,
    COUNT(DISTINCT user_id)                         AS UV_独立访客数,
    COUNT(DISTINCT CASE WHEN user_id IS NULL THEN ip ELSE NULL END) AS 匿名访客数
FROM access_logs
WHERE action = 'view'
GROUP BY log_date
ORDER BY log_date;

-- 1.3 每日 PV/UV 综合报表
SELECT
    log_date                                        AS 统计日期,
    COUNT(*)                                        AS PV_总访问量,
    COUNT(DISTINCT user_id)                         AS UV_登录用户数,
    COUNT(DISTINCT ip)                              AS UV_独立IP数,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) AS 人均访问次数
FROM access_logs
WHERE action = 'view'
GROUP BY log_date
ORDER BY log_date;

-- ============================================================
-- 2. 热门文章 TOP 10
-- 基于访问日志中的浏览量排序
-- ============================================================

-- 2.1 总浏览量 TOP 10 文章
SELECT
    p.post_id                                       AS 文章ID,
    p.title                                         AS 文章标题,
    p.author                                        AS 作者,
    p.category                                      AS 分类,
    COUNT(*)                                        AS 日志浏览次数,
    p.views                                         AS 文章记录浏览量,
    p.likes                                         AS 点赞数
FROM access_logs a
JOIN posts p ON a.post_id = p.post_id
WHERE a.action = 'view'
GROUP BY p.post_id, p.title, p.author, p.category, p.views, p.likes
ORDER BY 日志浏览次数 DESC
LIMIT 10;

-- 2.2 每日热门文章（按日期分组）
SELECT
    log_date                                        AS 日期,
    p.title                                         AS 文章标题,
    p.author                                        AS 作者,
    COUNT(*)                                        AS 当日浏览次数
FROM access_logs a
JOIN posts p ON a.post_id = p.post_id
WHERE a.action = 'view'
GROUP BY log_date, p.title, p.author
ORDER BY 日期, 当日浏览次数 DESC;

-- 2.3 互动率最高的文章（点赞 + 评论 / 浏览）
SELECT
    p.post_id                                       AS 文章ID,
    p.title                                         AS 文章标题,
    p.category                                      AS 分类,
    COUNT(DISTINCT CASE WHEN a.action = 'like' THEN a.log_id END)  AS 点赞次数,
    COUNT(DISTINCT CASE WHEN a.action = 'comment' THEN a.log_id END) AS 评论次数,
    COUNT(DISTINCT CASE WHEN a.action = 'view' THEN a.log_id END)  AS 浏览次数,
    ROUND(
        COUNT(DISTINCT CASE WHEN a.action IN ('like', 'comment') THEN a.log_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN a.action = 'view' THEN a.log_id END), 0),
        2
    )                                               AS 互动率百分比
FROM access_logs a
JOIN posts p ON a.post_id = p.post_id
GROUP BY p.post_id, p.title, p.category
HAVING 浏览次数 > 0
ORDER BY 互动率百分比 DESC
LIMIT 10;

-- ============================================================
-- 3. 用户活跃度分析
-- ============================================================

-- 3.1 用户活跃度排名（按访问次数）
SELECT
    u.user_id                                       AS 用户ID,
    u.username                                      AS 用户名,
    u.city                                          AS 城市,
    COUNT(*)                                        AS 总操作次数,
    COUNT(DISTINCT a.log_date)                      AS 活跃天数,
    COUNT(DISTINCT CASE WHEN a.action = 'view' THEN a.post_id END)   AS 浏览文章数,
    COUNT(DISTINCT CASE WHEN a.action = 'like' THEN a.post_id END)   AS 收藏文章数,
    COUNT(DISTINCT CASE WHEN a.action = 'comment' THEN a.post_id END) AS 评价文章数
FROM access_logs a
JOIN users u ON a.user_id = u.user_id
GROUP BY u.user_id, u.username, u.city
ORDER BY 总操作次数 DESC;

-- 3.2 用户行为偏好分析
SELECT
    u.username                                      AS 用户名,
    a.action                                        AS 行为类型,
    COUNT(*)                                        AS 次数,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY u.username), 2) AS 行为占比
FROM access_logs a
JOIN users u ON a.user_id = u.user_id
GROUP BY u.username, a.action
ORDER BY u.username, 次数 DESC;

-- 3.3 用户阅读兴趣标签分析
-- 通过用户浏览的文章标签来分析兴趣偏好
SELECT
    u.username                                      AS 用户名,
    TRIM(tag)                                       AS 兴趣标签,
    COUNT(*)                                        AS 浏览文章数
FROM access_logs a
JOIN posts p ON a.post_id = p.post_id
JOIN users u ON a.user_id = u.user_id
LATERAL VIEW EXPLODE(SPLIT(p.tags, ',')) t AS tag
WHERE a.action = 'view'
GROUP BY u.username, TRIM(tag)
ORDER BY u.username, 浏览文章数 DESC;

-- 3.4 新注册用户活跃情况
-- 统计最近注册的用户是否在访问日志中有活跃记录
SELECT
    u.user_id                                       AS 用户ID,
    u.username                                      AS 用户名,
    u.register_date                                 AS 注册日期,
    DATEDIFF(MAX(a.log_date), u.register_date)      AS 注册后首次活跃天数,
    COUNT(*)                                        AS 累计操作次数,
    CASE
        WHEN COUNT(*) >= 10 THEN '高活跃用户'
        WHEN COUNT(*) >= 5  THEN '中等活跃用户'
        WHEN COUNT(*) >= 1  THEN '低活跃用户'
        ELSE '沉默用户'
    END                                             AS 用户等级
FROM users u
LEFT JOIN access_logs a ON u.user_id = a.user_id
GROUP BY u.user_id, u.username, u.register_date
ORDER BY 注册日期;

-- ============================================================
-- 4. 文章分类分布
-- ============================================================

-- 4.1 各分类文章数量与平均数据
SELECT
    category                                        AS 文章分类,
    COUNT(*)                                        AS 文章数量,
    ROUND(AVG(views))                               AS 平均浏览量,
    ROUND(AVG(likes))                               AS 平均点赞数,
    SUM(views)                                      AS 总浏览量,
    SUM(likes)                                      AS 总点赞数,
    MAX(views)                                      AS 最高浏览量
FROM posts
GROUP BY category
ORDER BY 文章数量 DESC;

-- 4.2 各分类下的文章互动数据（结合访问日志）
SELECT
    p.category                                      AS 文章分类,
    COUNT(DISTINCT p.post_id)                       AS 文章数,
    COUNT(DISTINCT CASE WHEN a.action = 'view' THEN a.log_id END)    AS 总浏览次数,
    COUNT(DISTINCT CASE WHEN a.action = 'like' THEN a.log_id END)    AS 总点赞次数,
    COUNT(DISTINCT CASE WHEN a.action = 'comment' THEN a.log_id END) AS 总评论次数
FROM posts p
LEFT JOIN access_logs a ON p.post_id = a.post_id
GROUP BY p.category
ORDER BY 总浏览次数 DESC;

-- 4.3 各作者发布情况
SELECT
    author                                          AS 作者,
    COUNT(*)                                        AS 发布总数,
    COUNT(DISTINCT category)                        AS 涉及分类数,
    ROUND(AVG(views))                               AS 平均浏览量,
    ROUND(AVG(likes))                               AS 平均点赞数,
    SUM(views)                                      AS 总浏览量
FROM posts
GROUP BY author
ORDER BY 发布总数 DESC, 总浏览量 DESC;

-- 4.4 标签热度分布
-- 使用 LATERAL VIEW EXPLODE 将逗号分隔的标签拆分为多行
SELECT
    TRIM(tag)                                       AS 标签,
    COUNT(*)                                        AS 文章数量,
    ROUND(AVG(views))                               AS 平均浏览量,
    SUM(views)                                      AS 总浏览量
FROM posts
LATERAL VIEW EXPLODE(SPLIT(tags, ',')) t AS tag
GROUP BY TRIM(tag)
ORDER BY 文章数量 DESC, 总浏览量 DESC;

-- ============================================================
-- 5. 月度增长趋势
-- ============================================================

-- 5.1 每月发布数量趋势
SELECT
    publish_month                                   AS 发布月份,
    COUNT(*)                                        AS 发布数量,
    SUM(views)                                      AS 月度总浏览量,
    ROUND(AVG(views))                               AS 月均浏览量,
    SUM(likes)                                      AS 月度总点赞数
FROM posts
GROUP BY publish_month
ORDER BY publish_month;

-- 5.2 每月环比增长率
SELECT
    t.发布月份,
    t.发布数量,
    t.月度总浏览量,
    -- 环比增长率 = (本期 - 上期) / 上期 * 100
    ROUND(
        (t.月度总浏览量 - LAG(t.月度总浏览量, 1) OVER (ORDER BY t.发布月份))
        * 100.0
        / NULLIF(LAG(t.月度总浏览量, 1) OVER (ORDER BY t.发布月份), 0),
        2
    )                                               AS 浏览量环比增长率,
    ROUND(
        (t.发布数量 - LAG(t.发布数量, 1) OVER (ORDER BY t.发布月份))
        * 100.0
        / NULLIF(LAG(t.发布数量, 1) OVER (ORDER BY t.发布月份), 0),
        2
    )                                               AS 发布量环比增长率
FROM (
    SELECT
        publish_month                               AS 发布月份,
        COUNT(*)                                    AS 发布数量,
        SUM(views)                                  AS 月度总浏览量
    FROM posts
    GROUP BY publish_month
) t
ORDER BY t.发布月份;

-- 5.3 每月新增用户趋势
SELECT
    MONTH(register_date)                            AS 注册月份,
    COUNT(*)                                        AS 新增用户数,
    COUNT(DISTINCT city)                            AS 覆盖城市数
FROM users
GROUP BY MONTH(register_date)
ORDER BY 注册月份;

-- 5.4 用户访问时间分布（按小时统计）
SELECT
    HOUR(a.log_time)                                AS 小时,
    COUNT(*)                                        AS 访问次数,
    COUNT(DISTINCT a.user_id)                       AS 活跃用户数
FROM access_logs a
WHERE a.action = 'view'
GROUP BY HOUR(a.log_time)
ORDER BY 小时;

-- 5.5 设备类型分布
SELECT
    device                                          AS 设备类型,
    COUNT(*)                                        AS 访问次数,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS 占比百分比
FROM access_logs
GROUP BY device
ORDER BY 访问次数 DESC;

-- ============================================================
-- 6. 综合分析视图
-- ============================================================

-- 6.1 创建每日数据概览视图（可选）
CREATE VIEW IF NOT EXISTS v_daily_overview AS
SELECT
    log_date                                        AS 统计日期,
    COUNT(DISTINCT CASE WHEN action = 'view' THEN log_id END)    AS 浏览PV,
    COUNT(DISTINCT CASE WHEN action = 'like' THEN log_id END)    AS 点赞数,
    COUNT(DISTINCT CASE WHEN action = 'comment' THEN log_id END) AS 评论数,
    COUNT(DISTINCT CASE WHEN action = 'search' THEN log_id END)  AS 搜索次数,
    COUNT(DISTINCT user_id)                         AS 活跃用户数,
    COUNT(DISTINCT post_id)                         AS 被访问文章数,
    COUNT(DISTINCT ip)                              AS 独立IP数
FROM access_logs
GROUP BY log_date;

-- 查询视图数据
SELECT * FROM v_daily_overview ORDER BY 统计日期;
