-- ============================================================
-- 博客数据数仓初始化脚本
-- 环境：Hive 4.0.1 + MySQL Metastore + HDFS
-- 使用方式：beeline -u "jdbc:hive2://localhost:10000" -f init_warehouse.sql
-- ============================================================

-- ============================================================
-- 第一部分：创建数据库
-- ============================================================

-- 如果数据库已存在则先删除（生产环境慎用）
DROP DATABASE IF EXISTS blog_db CASCADE;

-- 创建数据仓库数据库
CREATE DATABASE IF NOT EXISTS blog_db
COMMENT '博客数据仓库 - 存储文章、用户和访问日志数据'
WITH DBPROPERTIES (
    'creator' = 'datalake',
    'create_date' = '2025-06-03',
    'description' = '博客数据分析仓库，包含文章、用户和访问行为数据'
);

-- 切换到数据库
USE blog_db;

-- ============================================================
-- 第二部分：创建内部表
-- ============================================================

-- ----------------------------------------------------------
-- 2.1 用户表（users）
-- 存储注册用户的基本信息
-- ----------------------------------------------------------
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id      INT          COMMENT '用户ID',
    username     STRING       COMMENT '用户名',
    email        STRING       COMMENT '邮箱地址',
    register_date DATE        COMMENT '注册日期',
    city         STRING       COMMENT '所在城市'
)
COMMENT '用户信息表'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'author' = 'datalake'
);

-- ----------------------------------------------------------
-- 2.2 文章表（posts）
-- 存储博客文章，按月份分区便于按时间范围查询
-- ----------------------------------------------------------
DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
    post_id      INT          COMMENT '文章ID',
    title        STRING       COMMENT '文章标题',
    content      STRING       COMMENT '文章内容',
    author       STRING       COMMENT '作者',
    category     STRING       COMMENT '文章分类',
    tags         STRING       COMMENT '标签（逗号分隔）',
    views        INT          COMMENT '浏览量',
    likes        INT          COMMENT '评论数',
    created_at   DATE         COMMENT '发布日期'
)
COMMENT '文章表 - 按发布月份分区'
PARTITIONED BY (
    publish_month STRING COMMENT '发布年月，格式：yyyy-MM'
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'author' = 'datalake'
);

-- ----------------------------------------------------------
-- 2.3 访问日志表（access_logs）
-- 存储用户访问行为数据，按日期分区
-- ----------------------------------------------------------
DROP TABLE IF EXISTS access_logs;

CREATE TABLE access_logs (
    log_id       INT          COMMENT '日志ID',
    user_id      INT          COMMENT '用户ID（未登录为NULL）',
    post_id      INT          COMMENT '文章ID（搜索行为为NULL）',
    action       STRING       COMMENT '行为类型：view-浏览, comment-评价, like-收藏, search-搜索',
    ip           STRING       COMMENT '访问IP地址',
    log_time     TIMESTAMP    COMMENT '访问时间',
    device       STRING       COMMENT '设备类型'
)
COMMENT '访问日志表 - 按日期分区'
PARTITIONED BY (
    log_date DATE COMMENT '日志日期，格式：yyyy-MM-dd'
)
CLUSTERED BY (user_id) INTO 3 BUCKETS
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'author' = 'datalake'
);

-- ============================================================
-- 第三部分：从 CSV 导入数据
-- ============================================================

-- ----------------------------------------------------------
-- 3.1 导入用户数据
-- 先将 CSV 上传到 HDFS 临时目录，再 LOAD 到表中
-- 执行前请确保已执行：
--   hdfs dfs -mkdir -p /tmp/demo_data
--   hdfs dfs -put data/users.csv /tmp/demo_data/
-- ----------------------------------------------------------
LOAD DATA INPATH '/tmp/demo_data/users.csv'
OVERWRITE INTO TABLE users;

-- ----------------------------------------------------------
-- 3.2 导入博客文章（需要按分区导入）
-- 由于文章表按 publish_month 分区，需要先创建分区再导入
-- 执行前请确保已执行：
--   hdfs dfs -mkdir -p /tmp/demo_data/posts
--   hdfs dfs -put data/posts.csv /tmp/demo_data/posts/
-- ----------------------------------------------------------

-- 创建各月份分区（数据涵盖 2025-01 到 2025-05）
ALTER TABLE posts ADD IF NOT EXISTS PARTITION (publish_month='2025-01');
ALTER TABLE posts ADD IF NOT EXISTS PARTITION (publish_month='2025-02');
ALTER TABLE posts ADD IF NOT EXISTS PARTITION (publish_month='2025-03');
ALTER TABLE posts ADD IF NOT EXISTS PARTITION (publish_month='2025-04');
ALTER TABLE posts ADD IF NOT EXISTS PARTITION (publish_month='2025-05');

-- 将全量数据先加载到临时表，再按分区插入
CREATE TABLE IF NOT EXISTS posts_tmp (
    post_id      INT,
    title        STRING,
    content      STRING,
    author       STRING,
    category     STRING,
    tags         STRING,
    views        INT,
    likes        INT,
    created_at   DATE
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 从 HDFS 加载到临时表
LOAD DATA INPATH '/tmp/demo_data/posts/posts.csv'
OVERWRITE INTO TABLE posts_tmp;

-- 按分区插入到目标表
INSERT OVERWRITE TABLE posts PARTITION (publish_month)
SELECT
    post_id,
    title,
    content,
    author,
    category,
    tags,
    views,
    likes,
    created_at,
    date_format(created_at, 'yyyy-MM') AS publish_month
FROM posts_tmp;

-- 删除临时表
DROP TABLE IF EXISTS posts_tmp;

-- ----------------------------------------------------------
-- 3.3 导入访问日志数据（按日期分区）
-- 执行前请确保已执行：
--   hdfs dfs -mkdir -p /tmp/demo_data/logs
--   hdfs dfs -put data/access_logs.csv /tmp/demo_data/logs/
-- ----------------------------------------------------------

-- 创建各日期分区（数据涵盖 2025-06-01 到 2025-06-03）
ALTER TABLE access_logs ADD IF NOT EXISTS PARTITION (log_date='2025-06-01');
ALTER TABLE access_logs ADD IF NOT EXISTS PARTITION (log_date='2025-06-02');
ALTER TABLE access_logs ADD IF NOT EXISTS PARTITION (log_date='2025-06-03');

-- 同样使用临时表方式导入
CREATE TABLE IF NOT EXISTS access_logs_tmp (
    log_id       INT,
    user_id      INT,
    post_id      INT,
    action       STRING,
    ip           STRING,
    log_time     TIMESTAMP,
    device       STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 从 HDFS 加载到临时表
LOAD DATA INPATH '/tmp/demo_data/logs/access_logs.csv'
OVERWRITE INTO TABLE access_logs_tmp;

-- 按分区插入到目标表
INSERT OVERWRITE TABLE access_logs PARTITION (log_date)
SELECT
    log_id,
    user_id,
    post_id,
    action,
    ip,
    log_time,
    device,
    cast(log_time AS DATE) AS log_date
FROM access_logs_tmp;

-- 删除临时表
DROP TABLE IF EXISTS access_logs_tmp;

-- ============================================================
-- 第四部分：验证数据
-- ============================================================

-- 查看所有表
SHOW TABLES;

-- 验证用户表
SELECT '--- 用户表数据验证 ---' AS info;
SELECT COUNT(*) AS user_count FROM users;
SELECT * FROM users LIMIT 5;

-- 验证文章表
SELECT '--- 文章表数据验证 ---' AS info;
SELECT COUNT(*) AS total_posts FROM posts;
SELECT publish_month, COUNT(*) AS post_count FROM posts GROUP BY publish_month ORDER BY publish_month;
SELECT * FROM posts LIMIT 5;

-- 验证访问日志表
SELECT '--- 访问日志表数据验证 ---' AS info;
SELECT COUNT(*) AS total_logs FROM access_logs;
SELECT log_date, COUNT(*) AS log_count FROM access_logs GROUP BY log_date ORDER BY log_date;
SELECT action, COUNT(*) AS action_count FROM access_logs GROUP BY action ORDER BY action_count DESC;

-- 验证分区信息
SELECT '--- 分区信息 ---' AS info;
SHOW PARTITIONS posts;
SHOW PARTITIONS access_logs;
