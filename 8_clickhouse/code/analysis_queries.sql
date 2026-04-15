-- ============================================================
-- 数据平台 ClickHouse 分析查询示例
-- 说明：包含 PV/UV 统计、热门文章排行、地域分布、设备分布等
-- ============================================================

-- ============================================================
-- 1. 基础 PV/UV 统计
-- ============================================================

-- 每日 PV/UV 趋势
SELECT
    toDate(timestamp)                    AS date,
    count()                              AS pv,           -- 总浏览量
    count(DISTINCT user_id)              AS uv,           -- 独立访客数
    countIf(action = 'comment')          AS comments,     -- 评论数
    countIf(action = 'like')             AS likes,        -- 点赞数
    countIf(action = 'share')            AS shares        -- 分享数
FROM demo.access_logs
GROUP BY date
ORDER BY date;

-- 最近 7 天 PV/UV 汇总
SELECT
    count()                              AS total_pv,
    count(DISTINCT user_id)              AS total_uv,
    count(DISTINCT toDate(timestamp))    AS active_days
FROM demo.access_logs
WHERE timestamp >= now() - INTERVAL 7 DAY;

-- ============================================================
-- 2. 热门文章排行
-- ============================================================

-- 文章浏览量 TOP 10
SELECT
    post_id,
    countIf(action = 'view')             AS views,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes,
    countIf(action = 'share')            AS shares,
    count()                              AS total_actions
FROM demo.access_logs
WHERE post_id > 0
GROUP BY post_id
ORDER BY views DESC
LIMIT 10;

-- 最近 7 天热门文章 TOP 5
SELECT
    post_id,
    countIf(action = 'view')             AS views,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes
FROM demo.access_logs
WHERE post_id > 0
  AND timestamp >= now() - INTERVAL 7 DAY
GROUP BY post_id
ORDER BY views DESC
LIMIT 5;

-- ============================================================
-- 3. 用户地域分布
-- ============================================================

-- 各城市 PV/UV 统计
SELECT
    city,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv,
    round(pv * 100.0 / sum(pv) OVER (), 2) AS pv_percent
FROM demo.access_logs
GROUP BY city
ORDER BY pv DESC;

-- 通过物化视图查询地域分布（更快，已预聚合）
SELECT
    city,
    sum(pv)                              AS pv,
    sum(uv)                              AS uv
FROM demo.blog_stats
GROUP BY city
ORDER BY pv DESC;

-- ============================================================
-- 4. 设备类型分布
-- ============================================================

-- 各设备 PV/UV 统计
SELECT
    device,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pv_percent
FROM demo.access_logs
GROUP BY device
ORDER BY pv DESC;

-- 各设备用户行为类型分布
SELECT
    device,
    countIf(action = 'view')             AS views,
    countIf(action = 'comment')          AS comments,
    countIf(action = 'like')             AS likes,
    countIf(action = 'share')            AS shares
FROM demo.access_logs
GROUP BY device
ORDER BY views DESC;

-- ============================================================
-- 5. 时间趋势分析
-- ============================================================

-- 按小时统计 PV（分析用户活跃时间段）
SELECT
    toHour(timestamp)                    AS hour,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv
FROM demo.access_logs
GROUP BY hour
ORDER BY hour;

-- 按星期统计 PV/UV（分析哪天最活跃）
SELECT
    toDayOfWeek(timestamp)               AS weekday,
    count()                              AS pv,
    count(DISTINCT user_id)              AS uv
FROM demo.access_logs
GROUP BY weekday
ORDER BY weekday;

-- 每小时 + 设备交叉分析
SELECT
    toHour(timestamp)                    AS hour,
    device,
    count()                              AS pv
FROM demo.access_logs
GROUP BY hour, device
ORDER BY hour, pv DESC;

-- ============================================================
-- 6. 用户行为分析
-- ============================================================

-- 各行为类型统计
SELECT
    action,
    count()                              AS cnt,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percent
FROM demo.access_logs
GROUP BY action
ORDER BY cnt DESC;

-- 用户活跃度排行（TOP 10 活跃用户）
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

-- ============================================================
-- 7. 综合分析：使用物化视图加速
-- ============================================================

-- 从 blog_stats 查询每日趋势（比扫描明细表快得多）
SELECT
    date,
    sum(pv)                              AS pv,
    sum(uv)                              AS uv,
    sum(comment_cnt)                     AS comments,
    sum(like_cnt)                        AS likes,
    sum(share_cnt)                       AS shares
FROM demo.blog_stats
GROUP BY date
ORDER BY date;

-- 从 post_stats 查询文章累计热度
SELECT
    post_id,
    sum(view_cnt)                        AS total_views,
    sum(comment_cnt)                     AS total_comments,
    sum(like_cnt)                        AS total_likes,
    sum(share_cnt)                       AS total_shares
FROM demo.post_stats
GROUP BY post_id
ORDER BY total_views DESC
LIMIT 10;

-- ============================================================
-- 8. 窗口函数示例
-- ============================================================

-- 每日 PV 环比增长（与前一天对比）
SELECT
    date,
    pv,
    lagInFrame(pv) OVER (ORDER BY date)          AS prev_pv,
    if(prev_pv > 0, round((pv - prev_pv) * 100.0 / prev_pv, 2), 0) AS growth_rate
FROM (
    SELECT
        toDate(timestamp) AS date,
        count() AS pv
    FROM demo.access_logs
    GROUP BY date
)
ORDER BY date;

-- 每篇文章在各城市的浏览量排名
SELECT
    post_id,
    city,
    views,
    row_number() OVER (PARTITION BY post_id ORDER BY views DESC) AS city_rank
FROM (
    SELECT
        post_id,
        city,
        countIf(action = 'view') AS views
    FROM demo.access_logs
    WHERE post_id > 0
    GROUP BY post_id, city
)
ORDER BY post_id, city_rank
LIMIT 30;
