-- ============================================================
-- Apache Iceberg 实战演示脚本
-- 使用 Spark SQL 操作 Iceberg 表
-- 包含：创建表、插入数据、查询、时间旅行、Schema 演进
-- 依赖：Spark 3.5.8 + Iceberg
-- ============================================================

-- ============================================================
-- 第一部分：环境配置
-- ============================================================

-- 设置 Spark SQL 参数，启用 Iceberg 扩展
SET spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;

-- 配置 Iceberg Catalog（使用 HadoopCatalog，基于 HDFS/本地目录）
SET spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog;
SET spark.sql.catalog.local.type=hadoop;
SET spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse;

-- 设置默认 Catalog
SET spark.sql.defaultCatalog=local;

-- 打印当前配置信息
SELECT 'Iceberg 演示环境已就绪' AS status;


-- ============================================================
-- 第二部分：创建 Iceberg 表
-- ============================================================

-- 创建博客文章表
-- 使用 Partition 按发布月份分区，提升查询性能
CREATE TABLE IF NOT EXISTS local.demo.posts (
    post_id        BIGINT       COMMENT '文章ID',
    title          STRING       COMMENT '文章标题',
    author         STRING       COMMENT '作者',
    category       STRING       COMMENT '分类',
    tags           ARRAY<STRING> COMMENT '标签列表',
    content        STRING       COMMENT '文章内容',
    publish_date   DATE         COMMENT '发布日期',
    view_count     BIGINT       COMMENT '浏览量',
    like_count     BIGINT       COMMENT '点赞数',
    comment_count  BIGINT       COMMENT '评论数',
    status         STRING       COMMENT '状态: published/draft/archived'
)
USING iceberg
PARTITIONED BY (months(publish_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.target-file-size-bytes' = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'history.expire.max-snapshot-age-ms' = '259200000'
);

-- 验证表已创建
DESCRIBE EXTENDED local.demo.posts;


-- ============================================================
-- 第三部分：插入数据
-- ============================================================

-- 插入博客文章数据
INSERT INTO local.demo.posts VALUES
    (1,  '从零搭建大数据平台：Hadoop 生态入门指南',   '数据小白',   '大数据',     ARRAY('hadoop', 'hdfs', '入门'),      '本文从零开始搭建 Hadoop 大数据平台',                DATE '2025-01-15', 12580, 342, 56,  'published'),
    (2,  'Spark SQL 实战：数据分析性能飞跃',           '数据小白',   'Spark',      ARRAY('spark', 'sql', '性能'),        'Spark SQL 替代 Hive 实现性能提升',                  DATE '2025-02-08', 8930,  267, 43,  'published'),
    (3,  'Kafka 深度解析：消息队列核心原理',           '数据小白',   '消息队列',   ARRAY('kafka', 'mq', '架构'),          '深入剖析 Kafka 存储架构和副本机制',                 DATE '2025-03-01', 15200, 489, 78,  'published'),
    (4,  'Flink vs Spark Streaming：选型指南',        '流计算达人', '流计算',     ARRAY('flink', 'spark', '选型'),        '两大实时计算框架全面对比',                          DATE '2025-03-20', 11800, 356, 91,  'published'),
    (5,  'Elasticsearch 全文搜索实战',                '数据小白',   '搜索引擎',   ARRAY('es', '搜索', '中文分词'),       '为数据平台搭建高性能全文搜索',                      DATE '2025-04-10', 9870,  312, 47,  'published'),
    (6,  '数据湖架构演进：HDFS 到 Iceberg',           '架构师老王', '数据湖',     ARRAY('iceberg', '数据湖', '架构'),     '现代表格式的重大转变',                              DATE '2025-05-05', 7650,  234, 38,  'published'),
    (7,  'Hive SQL 性能优化 20 条经验',              'SQL优化大师','数据仓库',   ARRAY('hive', 'sql', '优化'),           '生产环境验证的优化技巧',                            DATE '2025-05-22', 13400, 421, 65,  'published'),
    (8,  'MinIO 对象存储入门指南',                    '数据小白',   '存储',       ARRAY('minio', 's3', '对象存储'),       '搭建私有云 S3 兼容存储',                            DATE '2025-06-10', 6320,  198, 29,  'published'),
    (9,  'DolphinScheduler 工作流调度实践',           '数据小白',   '任务调度',   ARRAY('调度', '工作流', '自动化'),      '让数据处理流程全自动化',                            DATE '2025-06-28', 5480,  167, 23,  'published'),
    (10, '向量数据库 Milvus 入门',                   'AI探索者',   'AI',         ARRAY('milvus', '向量', '语义搜索'),     '语义搜索的新时代',                                  DATE '2025-07-15', 8900,  278, 42,  'published');

-- 插入第二批数据（不同月份，用于分区测试）
INSERT INTO local.demo.posts VALUES
    (11, 'Spark Structured Streaming 实时 ETL',     '流计算达人', '流计算',     ARRAY('spark', 'streaming', 'etl'),     '构建完整的实时 ETL 管道',                          DATE '2025-08-01', 7230,  215, 36,  'published'),
    (12, '大数据集群监控：Prometheus + Grafana',     '运维老张',   '运维',       ARRAY('prometheus', 'grafana', '监控'), '可视化监控大屏搭建',                               DATE '2025-08-20', 6100,  185, 27,  'published'),
    (13, '数据质量治理：从源头保障数据可信度',       '数据治理专家','数据治理',   ARRAY('质量', '治理', '最佳实践'),       '数据质量六大维度与落地',                            DATE '2025-09-05', 5680,  172, 31,  'published'),
    (14, 'ClickHouse vs Elasticsearch 性能对比',    '性能测试员', 'OLAP',       ARRAY('clickhouse', 'es', '性能'),      'OLAP 引擎基准测试',                                DATE '2025-09-22', 10200, 310, 53,  'published'),
    (15, 'Kafka Connect 实战：零代码数据同步',       '数据小白',   '消息队列',   ARRAY('kafka', 'connect', 'cdc'),       'JDBC Source + HDFS Sink',                         DATE '2025-10-10', 4900,  148, 22,  'published'),
    (16, 'Iceberg 时间旅行：再也不怕误删数据',       '架构师老王', '数据湖',     ARRAY('iceberg', '快照', '回滚'),        '快照机制实现版本管理',                              DATE '2025-10-28', 6750,  203, 35,  'published'),
    (17, 'Flink Checkpoint 详解',                   '流计算达人', '流计算',     ARRAY('flink', 'checkpoint', '容错'),    'Exactly-Once 语义的保障',                          DATE '2025-11-15', 8300,  256, 44,  'published'),
    (18, '数据平台大数据架构：完整链路设计',             '数据小白',   '架构设计',   ARRAY('架构', '数据平台', '全链路'),         '从采集到搜索的全链路覆盖',                          DATE '2025-12-01', 11600, 367, 58,  'published'),
    (19, 'PySpark 最佳实践指南',                    'Python爱好者','Spark',      ARRAY('pyspark', 'python', '大数据'),    'Python 开发者的 Spark 入门',                       DATE '2025-12-18', 9400,  289, 41,  'published'),
    (20, '2025 大数据技术趋势回顾',                 '数据小白',   '行业观察',   ARRAY('趋势', '大数据', '2026'),         '技术演进与未来展望',                                DATE '2026-01-05', 14500, 478, 87,  'published');

-- 验证数据
SELECT COUNT(*) AS total_posts FROM local.demo.posts;


-- ============================================================
-- 第四部分：数据查询
-- ============================================================

-- 4.1 查询所有已发布的文章
SELECT post_id, title, author, publish_date, view_count
FROM local.demo.posts
WHERE status = 'published'
ORDER BY publish_date DESC;

-- 4.2 分区裁剪：只查询 2025 年下半年的文章（性能优化关键）
SELECT title, author, publish_date, view_count
FROM local.demo.posts
WHERE publish_date >= DATE '2025-07-01'
ORDER BY view_count DESC;

-- 4.3 聚合统计：各分类的文章数量和平均浏览量
SELECT
    category,
    COUNT(*) AS post_count,
    CAST(AVG(view_count) AS BIGINT) AS avg_views,
    SUM(view_count) AS total_views
FROM local.demo.posts
GROUP BY category
ORDER BY total_views DESC;

-- 4.4 聚合统计：各作者的文章数和总点赞数
SELECT
    author,
    COUNT(*) AS post_count,
    SUM(like_count) AS total_likes,
    SUM(comment_count) AS total_comments
FROM local.demo.posts
GROUP BY author
ORDER BY total_likes DESC;

-- 4.5 Top N 查询：浏览量最高的 5 篇文章
SELECT title, author, view_count, like_count
FROM local.demo.posts
ORDER BY view_count DESC
LIMIT 5;


-- ============================================================
-- 第五部分：Schema 演进（加列）
-- ============================================================

-- 记录当前快照 ID（用于后面时间旅行对比）
CREATE OR REPLACE TEMP VIEW current_snapshot AS
SELECT snapshot_id FROM local.demo.posts.snapshots
ORDER BY committed_at DESC LIMIT 1;

SELECT concat('当前快照 ID: ', snapshot_id) AS info FROM current_snapshot;

-- 5.1 添加新列：word_count（字数统计）
ALTER TABLE local.demo.posts ADD COLUMN word_count BIGINT AFTER content;

-- 5.2 添加新列：reading_time（预计阅读时长，分钟）
ALTER TABLE local.demo.posts ADD COLUMN reading_time INT AFTER word_count;

-- 5.3 更新新列的数据
UPDATE local.demo.posts
SET word_count = CAST(LENGTH(content) * 10 AS BIGINT),
    reading_time = CAST(LENGTH(content) * 10 / 500 AS INT)
WHERE status = 'published';

-- 验证 Schema 演进结果
SELECT post_id, title, word_count, reading_time
FROM local.demo.posts
ORDER BY post_id
LIMIT 5;

-- 查看表的历史 Schema
SELECT * FROM local.demo.posts.history;


-- ============================================================
-- 第六部分：时间旅行（Time Travel）
-- ============================================================

-- 6.1 查看所有快照
SELECT
    snapshot_id,
    parent_id,
    committed_at,
    operation,
    summary['added-records'] AS added_records,
    summary['deleted-records'] AS deleted_records
FROM local.demo.posts.snapshots
ORDER BY committed_at DESC;

-- 6.2 回到添加新列之前的快照（使用快照 ID）
-- 注意：替换 <snapshot_id> 为上面第一个快照的实际 ID
-- SELECT post_id, title, author, publish_date
-- FROM local.demo.posts VERSION AS OF <snapshot_id>
-- ORDER BY publish_date;

-- 6.3 使用时间戳进行时间旅行（回到 5 分钟前）
-- SELECT post_id, title, author
-- FROM local.demo.posts FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01 00:00:00'
-- ORDER BY post_id;

-- 6.4 演示：回滚到上一个版本
-- 先获取第一个快照 ID（创建表后第一次写入的快照）
CREATE OR REPLACE TEMP VIEW first_snapshot AS
SELECT snapshot_id FROM local.demo.posts.snapshots
ORDER BY committed_at ASC LIMIT 1;

-- SELECT concat('第一个快照 ID: ', snapshot_id) AS info FROM first_snapshot;

-- 回滚到第一个快照（注意：这只是查询，不影响当前表状态）
-- SELECT post_id, title, author FROM local.demo.posts VERSION AS OF (
--     SELECT snapshot_id FROM first_snapshot
-- );


-- ============================================================
-- 第七部分：快照管理
-- ============================================================

-- 7.1 查看过期快照（保留最近 2 个快照）
-- CALL local.system.expire_snapshots('local.demo.posts', TIMESTAMP '2026-01-01 00:00:00', 2);

-- 7.2 清理孤儿文件（删除不在任何快照中的博客图片）
-- CALL local.system.remove_orphan_files('local.demo.posts', TIMESTAMP '2026-01-01 00:00:00');

-- 7.3 查看表当前的文件列表
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes
FROM local.demo.posts.files;


-- ============================================================
-- 第八部分：Upsert 操作（Merge Into）
-- ============================================================

-- 使用 MERGE INTO 实现Upsert：如果文章存在则更新浏览量，不存在则插入
-- 创建临时数据模拟更新
CREATE OR REPLACE TEMP VIEW post_updates AS
SELECT 1 AS post_id, 13000 AS view_count, 360 AS like_count UNION ALL
SELECT 5 AS post_id, 10200 AS view_count, 330 AS like_count UNION ALL
SELECT 10 AS post_id, 9100 AS view_count, 290 AS like_count;

-- 执行 MERGE INTO
MERGE INTO local.demo.posts AS target
USING post_updates AS source
ON target.post_id = source.post_id
WHEN MATCHED THEN
    UPDATE SET
        view_count = source.view_count,
        like_count = source.like_count
WHEN NOT MATCHED THEN
    INSERT (post_id, title, author, category, content, publish_date, view_count, like_count, comment_count, status)
    VALUES (source.post_id, '新文章', '未知', '未分类', '新内容', CURRENT_DATE(), source.view_count, source.like_count, 0, 'draft');

-- 验证更新结果
SELECT post_id, title, view_count, like_count
FROM local.demo.posts
WHERE post_id IN (1, 5, 10)
ORDER BY post_id;


-- ============================================================
-- 完成
-- ============================================================
SELECT 'Iceberg 演示脚本执行完毕！' AS status;
