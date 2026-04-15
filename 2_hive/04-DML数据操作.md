# Hive DML — 数据操作详解

> Hive 4.0.1 学习系列 - 第 4 篇
> 适用环境：macOS Apple Silicon / JDK 21 / Hive 4.0.1

## 一、INSERT VALUES（插入数据）

### 1.1 单行插入

```sql
-- 使用 dwd 数据库
USE dwd;

-- 单行插入
INSERT INTO TABLE dwd_article_publish
VALUES (
    1001,                                    -- article_id
    'Hive 入门到精通指南',                   -- title
    1001,                                    -- author_id
    '张三',                                  -- author_name
    1,                                       -- category_id
    '大数据',                                -- category_name
    3500,                                    -- word_count
    DATE '2026-04-10',                       -- publish_date
    'Hive,数仓,大数据'                       -- tags
);
```

### 1.2 多行插入

```sql
-- 多行批量插入（比多次单行插入效率更高）
INSERT INTO TABLE dwd.dwd_article_publish
VALUES
    (1002, 'Spark 核心原理详解', 1001, '张三', 1, '大数据', 5200, DATE '2026-04-11', 'Spark,分布式'),
    (1003, 'Flink 实时流处理入门', 1002, '李四', 2, '实时计算', 4800, DATE '2026-04-12', 'Flink,流处理'),
    (1004, 'HDFS 存储架构深度解析', 1001, '张三', 1, '大数据', 6100, DATE '2026-04-13', 'HDFS,存储'),
    (1005, 'Kafka 消息队列实战', 1003, '王五', 3, '消息队列', 3900, DATE '2026-04-14', 'Kafka,MQ'),
    (1006, 'Elasticsearch 搜索优化', 1002, '李四', 4, '搜索引擎', 4200, DATE '2026-04-15', 'ES,搜索');
```

### 1.3 插入到分区表

```sql
-- 插入到指定分区
INSERT INTO TABLE dwd.dwd_access_detail PARTITION (dt = '2026-04-15')
VALUES
    ('uuid-001', 2001, 1001, '192.168.1.100', 'Mozilla/5.0 ...', 120),
    ('uuid-002', 2002, 1002, '192.168.1.101', 'Mozilla/5.0 ...', 85),
    ('uuid-003', NULL, 1003, '10.0.0.50', 'Mozilla/5.0 ...', 45),
    ('uuid-004', 2001, 1004, '192.168.1.100', 'Mozilla/5.0 ...', 200),
    ('uuid-005', 2003, 1001, '172.16.0.10', 'Mozilla/5.0 ...', 60);

-- 验证分区数据
SELECT * FROM dwd.dwd_access_detail WHERE dt = '2026-04-15';
```

## 二、INSERT SELECT（从查询结果插入）

### 2.1 基本语法

`INSERT SELECT` 是 Hive 中最常用的数据加载方式，从查询结果插入到目标表：

```sql
-- 基本语法
INSERT INTO TABLE target_table
SELECT col1, col2, ...
FROM source_table
WHERE condition;

-- 覆盖插入（先清空目标数据，再插入）
INSERT OVERWRITE TABLE target_table
SELECT col1, col2, ...
FROM source_table
WHERE condition;
```

### 2.2 数据平台项目 ETL 示例：ODS → DWD

这是数仓中最典型的数据加工流程，从 ODS 层读取原始数据，清洗后写入 DWD 层：

```sql
-- ODS 层原始数据
SELECT * FROM ods.ods_product_article WHERE dt = '2026-04-15' LIMIT 3;
-- id  | title      | content | author_id | category_id | status | ...
-- 1   | Hive教程   | ...     | 1001      | 1           | 1      | ...

-- DWD 层清洗规则：
-- 1. 去除 status != 1（非已发布状态）的数据
-- 2. 过滤 content 为空的数据
-- 3. 关联作者名称和分类名称
-- 4. 计算字数（LENGTH 函数）

-- 第一步：确保维度表有数据
INSERT INTO TABLE dim.dim_user
VALUES
    (1001, 'zhangsan', '张三', 'zhangsan@data.com', 'seller', 1, DATE '2025-01-15'),
    (1002, 'lisi',     '李四', 'lisi@data.com',     'seller', 1, DATE '2025-03-20'),
    (1003, 'wangwu',   '王五', 'wangwu@data.com',   'seller', 1, DATE '2025-06-10');

INSERT INTO TABLE dim.dim_category
VALUES
    (1, '大数据',   NULL, 1, 1),
    (2, '实时计算', NULL, 1, 2),
    (3, '消息队列', NULL, 1, 3),
    (4, '搜索引擎', NULL, 1, 4);

-- 第二步：ETL 清洗 ODS → DWD
INSERT OVERWRITE TABLE dwd.dwd_article_publish PARTITION (dt = '2026-04-15')
SELECT
    t.id                                    AS article_id,
    t.title                                 AS title,
    t.author_id                             AS author_id,
    COALESCE(u.nickname, '未知作者')        AS author_name,
    t.category_id                           AS category_id,
    COALESCE(c.name, '未分类')              AS category_name,
    LENGTH(t.content)                       AS word_count,
    CAST(t.created_at AS DATE)              AS publish_date,
    t.title                                 AS tags    -- ODS 暂无 tags，用 title 占位
FROM ods.ods_product_article t
LEFT JOIN dim.dim_user u ON t.author_id = u.user_id
LEFT JOIN dim.dim_category c ON t.category_id = c.category_id
WHERE t.dt = '2026-04-15'
  AND t.status = 1                         -- 仅在售
  AND t.content IS NOT NULL                -- 内容不为空
  AND LENGTH(TRIM(t.content)) > 0;         -- 内容不为空字符串
```

### 2.3 多表插入（Multi-Table Insert）

Hive 支持一次扫描源表，将数据插入多个目标表，避免重复扫描：

```sql
-- 从一张源表同时写入多张目标表
FROM ods.ods_product_article
WHERE dt = '2026-04-15'
INSERT OVERWRITE TABLE dwd.dwd_product_publish PARTITION (dt = '2026-04-15')
SELECT
    id, title, author_id, category_id,
    LENGTH(content) AS desc_length,
    CAST(created_at AS DATE) AS publish_date
WHERE status = 1 AND content IS NOT NULL

INSERT OVERWRITE TABLE dwd.dwd_product_draft PARTITION (dt = '2026-04-15')
SELECT
    id, title, author_id, category_id,
    LENGTH(content) AS desc_length,
    CAST(updated_at AS DATE) AS last_modified
WHERE status = 0;
```

多表插入的执行流程：

```
┌──────────────────────────────────────────────────────────┐
│              Multi-Table Insert 执行流程                  │
│                                                          │
│  ┌──────────┐                                            │
│  │ ODS 源表 │ ← 只扫描一次（MapReduce 只启动一次 Job）   │
│  └────┬─────┘                                            │
│       │                                                  │
│       ├──→ [过滤 status=1] ──→ DWD 在售文章表           │
│       │                                                  │
│       └──→ [过滤 status=0] ──→ DWD 草稿文章表             │
│                                                          │
│  优势：一次扫描，多个输出，减少 I/O 和 MapReduce 开销     │
└──────────────────────────────────────────────────────────┘
```

## 三、LOAD DATA（从文件导入）

### 3.1 基本语法

`LOAD DATA` 将 HDFS 或本地文件移动到 Hive 表对应的目录中：

```sql
LOAD DATA [LOCAL] INPATH 'filepath'
  [OVERWRITE] INTO TABLE table_name
  [PARTITION (part_col = value, ...)];
```

### 3.2 从 HDFS 导入

```sql
-- 将 HDFS 上的文件移动到 Hive 表目录
-- 注意：LOAD DATA 是移动操作，原文件会被移走
LOAD DATA INPATH '/tmp/product_articles_2026-04-15.tsv'
OVERWRITE INTO TABLE ods.ods_product_article
PARTITION (dt = '2026-04-15');

-- 不使用 OVERWRITE，则追加数据
LOAD DATA INPATH '/tmp/product_articles_2026-04-16.tsv'
INTO TABLE ods.ods_product_article
PARTITION (dt = '2026-04-16');
```

### 3.3 从本地导入

```sql
-- 从本地文件系统上传到 HDFS（使用 LOCAL 关键字）
-- 文件会被上传到 Hive 表对应的 HDFS 目录
LOAD DATA LOCAL INPATH '/tmp/data/articles.tsv'
INTO TABLE ods.ods_product_article
PARTITION (dt = '2026-04-15');
```

### 3.4 LOAD DATA vs INSERT 的区别

```
┌──────────────────────────────────────────────────────────────────┐
│              LOAD DATA vs INSERT SELECT 对比                     │
│                                                                  │
│  LOAD DATA：                                                     │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ • 纯文件移动操作，不经过 MapReduce                     │       │
│  │ • 速度极快（只是 HDFS 文件 rename）                   │       │
│  │ • 不做任何数据验证和转换                              │       │
│  │ • 文件格式必须与表定义一致                            │       │
│  │ • 适合：原始数据快速导入 ODS 层                        │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
│  INSERT SELECT：                                                 │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ • 经过 MapReduce / Spark 引擎执行                    │       │
│  │ • 速度较慢（需要启动计算作业）                        │       │
│  │ • 支持数据验证、转换、JOIN、聚合                      │       │
│  │ • 输出格式由目标表的 STORED AS 决定                   │       │
│  │ • 适合：ETL 加工、跨层的数据转换                      │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
│  典型流程：                                                      │
│  外部文件 → [LOAD DATA] → ODS 层 (TextFile)                    │
│  ODS 层   → [INSERT SELECT] → DWD 层 (ORC，已清洗)             │
│  DWD 层   → [INSERT SELECT] → DWS 层 (ORC，已聚合)             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 四、导出数据

### 4.1 INSERT OVERWRITE 导出到目录

```sql
-- 将查询结果导出到 HDFS 目录
INSERT OVERWRITE DIRECTORY '/tmp/export/article_ranking'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    article_id,
    title,
    author_name,
    word_count,
    publish_date
FROM dwd.dwd_article_publish
WHERE dt = '2026-04-15'
ORDER BY word_count DESC;

-- 导出为 ORC 格式（保留压缩优势）
INSERT OVERWRITE DIRECTORY '/tmp/export/article_ranking_orc'
STORED AS ORC
SELECT * FROM dwd.dwd_article_publish WHERE dt = '2026-04-15';
```

### 4.2 使用 hive -e 命令行导出

```bash
# 方式1：hive -e 执行 SQL 并重定向到文件
hive -e "SELECT * FROM dwd.dwd_article_publish WHERE dt = '2026-04-15'" \
    > /tmp/article_publish.tsv

# 方式2：hive -f 执行 SQL 文件
cat > /tmp/query.hql << 'EOF'
SELECT article_id, title, author_name, word_count
FROM dwd.dwd_article_publish
WHERE dt = '2026-04-15'
ORDER BY word_count DESC
LIMIT 10;
EOF

hive -f /tmp/query.hql > /tmp/top10_articles.tsv

# 方式3：使用 Beeline 导出（推荐，支持格式化输出）
beeline -u "jdbc:hive2://localhost:10000" -n browncutie \
    --outputformat=csv2 \
    -e "SELECT * FROM dwd.dwd_article_publish WHERE dt = '2026-04-15'" \
    > /tmp/articles.csv

# 方式4：Beeline 静默模式（去掉表头等额外信息）
beeline -u "jdbc:hive2://localhost:10000" -n browncutie \
    --silent=true \
    --outputformat=csv2 \
    -e "SELECT * FROM dwd.dwd_article_publish WHERE dt = '2026-04-15'" \
    > /tmp/articles_clean.csv
```

### 4.3 使用 HDFS 命令导出

```bash
# 直接从 HDFS 复制 Hive 表文件到本地
hdfs dfs -get /user/hive/warehouse/dwd.db/dwd_article_publish/dt=2026-04-15/* \
    /tmp/export/dwd_article_publish/

# 注意：ORC/Parquet 等二进制格式文件需要用对应工具读取
# TextFile 格式可以直接查看
cat /tmp/export/dwd_article_publish/000000_0
```

## 五、UPDATE 和 DELETE（ACID 表）

### 5.1 为什么普通表不支持 UPDATE/DELETE

Hive 的普通表（非 ACID 表）不支持 UPDATE 和 DELETE 操作，这与 Hive 的设计理念有关：

```
┌──────────────────────────────────────────────────────────────────┐
│            普通表不支持 UPDATE/DELETE 的原因                     │
│                                                                  │
│  1. HDFS 文件系统特性：                                          │
│     HDFS 是"一次写入，多次读取"的文件系统                        │
│     不支持随机修改文件中间的某一行                                │
│     修改数据 = 重写整个文件（成本极高）                          │
│                                                                  │
│  2. MapReduce 批处理模型：                                       │
│     Hive 设计为批量处理引擎，不是事务型数据库                    │
│     逐行 UPDATE/DELETE 在分布式环境下效率极低                   │
│                                                                  │
│  3. 数据仓库设计理念：                                           │
│     数仓采用"追加写入"模式（Append-Only）                       │
│     纠正数据的方式是重新跑 ETL，而不是 UPDATE                    │
│                                                                  │
│  ┌────────────────────┐    ┌────────────────────┐              │
│  │ 传统数据库 (OLTP)  │    │ 数据仓库 (OLAP)    │              │
│  │                    │    │                    │              │
│  │ UPDATE users       │    │ INSERT OVERWRITE   │              │
│  │ SET age = 26       │    │ TABLE users        │              │
│  │ WHERE id = 1;      │    │ SELECT ...         │              │
│  │                    │    │ WHERE ...;         │              │
│  └────────────────────┘    └────────────────────┘              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.2 ACID 表（事务表）

Hive 从 0.13 版本开始引入 ACID 事务支持。Hive 4.x 中 ACID 表已经是默认行为：

```
┌──────────────────────────────────────────────────────────────────┐
│                    Hive ACID 事务原理                             │
│                                                                  │
│  ACID = Atomicity + Consistency + Isolation + Durability        │
│                                                                  │
│  底层实现机制：                                                  │
│                                                                  │
│  普通表：                                                        │
│  ┌──────────┐                                                    │
│  │ 文件 A   │ ← 所有数据在一个文件中                             │
│  └──────────┘                                                    │
│                                                                  │
│  ACID 表：                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                       │
│  │ Base 文件│  │ Delta 文件│  │ Delete   │                       │
│  │ (原始)   │  │ (新增/修改)│  │ 文件      │                      │
│  └──────────┘  └──────────┘  └──────────┘                       │
│       │             │             │                              │
│       └─────────────┼─────────────┘                              │
│                     ▼                                            │
│            ┌──────────────┐                                     │
│            │  Compaction  │ ← 后台合并进程                      │
│            │  (压缩合并)  │                                     │
│            └──────────────┘                                     │
│                                                                  │
│  INSERT → 写入 delta 文件                                       │
│  UPDATE → 写入 delta 文件（新值）+ delete 文件（旧值标记）      │
│  DELETE → 写入 delete 文件（标记删除）                          │
│  Compaction → 合并 base + delta + delete 为新的 base 文件      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.3 创建和使用 ACID 表

```sql
-- 创建 ACID 事务表（Hive 4.x 默认支持）
-- 需要设置为分桶表 + 存储在 ORC 格式
CREATE TABLE dwd.dwd_user_profile (
    user_id         BIGINT,
    username        STRING,
    nickname        STRING,
    total_articles  INT,
    total_views     BIGINT
)
CLUSTERED BY (user_id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

-- 插入数据
INSERT INTO TABLE dwd.dwd_user_profile
VALUES (2001, 'zhangsan', '张三', 5, 1200);

-- UPDATE 操作（仅 ACID 表支持）
UPDATE dwd.dwd_user_profile
SET total_views = total_views + 500
WHERE user_id = 2001;

-- DELETE 操作（仅 ACID 表支持）
DELETE FROM dwd.dwd_user_profile
WHERE user_id = 9999;

-- 验证
SELECT * FROM dwd.dwd_user_profile WHERE user_id = 2001;
```

### 5.4 ACID 表的限制

```
┌──────────────────────────────────────────────────────────────────┐
│                    ACID 表使用限制                                │
│                                                                  │
│  1. 必须是 ORC 格式                                             │
│  2. 必须是分桶表（CLUSTERED BY）                                │
│  3. 不能是外部表（EXTERNAL）                                    │
│  4. 不支持 LOAD DATA                                            │
│  5. 性能低于普通表（delta 文件 + compaction 开销）               │
│                                                                  │
│  建议：                                                          │
│  - 数仓分层中使用普通表 + INSERT OVERWRITE                      │
│  - 仅在确实需要行级更新/删除的场景使用 ACID 表                   │
│  - 例如：用户画像表、实时维度更新等                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 六、实战：导入文章 CSV 数据到 Hive 表

### 6.1 准备测试数据

在本地创建 CSV 测试文件：

```bash
# 创建测试数据文件
cat > /tmp/product_articles.csv << 'EOF'
id,title,author_id,category_id,status,view_count,like_count,created_at,updated_at
101,MacBook Pro 16寸,1001,1,1,3500,42,2026-04-10 10:30:00,2026-04-10 10:30:00
102,机械键盘 Cherry轴,1001,1,1,5200,78,2026-04-11 14:20:00,2026-04-11 14:20:00
103,4K显示器 27寸,1002,2,1,4800,65,2026-04-12 09:15:00,2026-04-12 09:15:00
104,无线蓝牙耳机,1001,1,1,6100,91,2026-04-13 16:45:00,2026-04-13 16:45:00
105,人体工学椅,1003,3,1,3900,33,2026-04-14 11:00:00,2026-04-14 11:00:00
106,USB-C扩展坞,1002,4,1,4200,56,2026-04-15 08:30:00,2026-04-15 08:30:00
107,移动固态硬盘 1TB,1001,1,1,5500,88,2026-04-15 13:20:00,2026-04-15 13:20:00
108,智能台灯,1003,5,0,0,0,2026-04-15 15:00:00,2026-04-15 15:00:00
109,无线充电板,1002,6,1,3100,29,2026-04-15 17:10:00,2026-04-15 17:10:00
110,降噪麦克风,1003,6,0,0,0,2026-04-15 19:00:00,2026-04-15 19:00:00
EOF

# 上传到 HDFS
hdfs dfs -mkdir -p /tmp/product_data
hdfs dfs -put /tmp/product_articles.csv /tmp/product_data/
```

### 6.2 创建 ODS 表并加载数据

```sql
-- 创建 ODS 层外部表（CSV 格式，带表头）
USE ods;

CREATE EXTERNAL TABLE ods_product_article_csv (
    id              STRING,
    title           STRING,
    author_id       STRING,
    category_id     STRING,
    status          STRING,
    view_count      STRING,
    like_count  STRING,
    created_at      STRING,
    updated_at      STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1')
LOCATION '/tmp/product_data';

-- 验证数据加载
SELECT * FROM ods.ods_product_article_csv LIMIT 5;
```

### 6.3 ETL 清洗：ODS → DWD

```sql
-- 第一步：创建 DWD 目标表
USE dwd;

CREATE TABLE dwd_product_clean (
    post_id      BIGINT          COMMENT '文章ID',
    title           STRING          COMMENT '名称',
    author_id       BIGINT          COMMENT '作者ID',
    category_id     INT             COMMENT '分类ID',
    status          INT             COMMENT '状态',
    view_count      BIGINT          COMMENT '访问量',
    like_count  INT             COMMENT '点赞数',
    publish_date    DATE            COMMENT '发布日期'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 第二步：ETL 清洗写入
INSERT OVERWRITE TABLE dwd.dwd_product_clean
SELECT
    CAST(id          AS BIGINT)      AS post_id,
    TRIM(title)                       AS title,
    CAST(author_id  AS BIGINT)      AS author_id,
    CAST(category_id AS INT)         AS category_id,
    CAST(status      AS INT)         AS status,
    CAST(view_count  AS BIGINT)      AS view_count,
    CAST(like_count AS INT)      AS like_count,
    CAST(created_at  AS DATE)        AS publish_date
FROM ods.ods_product_article_csv
WHERE
    id IS NOT NULL
    AND TRIM(id) != ''
    AND CAST(status AS INT) = 1      -- 仅在售文章
    AND CAST(view_count AS BIGINT) > 0;

-- 第三步：验证结果
SELECT * FROM dwd.dwd_product_clean ORDER BY view_count DESC;
```

### 6.4 DWD → DWS 汇总

```sql
-- 创建 DWS 汇总表
USE dws;

CREATE TABLE dws_seller_stats (
    author_id       BIGINT          COMMENT '作者ID',
    product_count   INT             COMMENT '文章数量',
    total_views     BIGINT          COMMENT '总访问量',
    total_likes BIGINT          COMMENT '总点赞数',
    avg_views       DOUBLE          COMMENT '平均访问量'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 汇总计算
INSERT OVERWRITE TABLE dws.dws_seller_stats
SELECT
    author_id,
    COUNT(*)                            AS product_count,
    SUM(view_count)                     AS total_views,
    SUM(like_count)                 AS total_likes,
    ROUND(AVG(view_count), 2)           AS avg_views
FROM dwd.dwd_product_clean
GROUP BY author_id
ORDER BY total_views DESC;

-- 验证
SELECT * FROM dws.dws_seller_stats;
```

### 6.5 完整数据流总结

```
┌──────────────────────────────────────────────────────────────────┐
│              博客文章导入完整流程                                  │
│                                                                  │
│  [1] 本地 CSV 文件                                               │
│  /tmp/product_articles.csv                                       │
│       │                                                          │
│       │ hdfs dfs -put                                            │
│       ▼                                                          │
│  [2] HDFS 原始文件                                               │
│  /tmp/product_data/product_articles.csv                         │
│       │                                                          │
│       │ CREATE EXTERNAL TABLE + LOCATION                         │
│       ▼                                                          │
│  [3] ODS 层（外部表，TextFile/CSV）                              │
│  ods.ods_product_article_csv                                    │
│  - 字段全部为 STRING（原始格式）                                 │
│  - 跳过 CSV 表头（skip.header.line.count=1）                    │
│       │                                                          │
│       │ INSERT OVERWRITE TABLE ... SELECT（ETL 清洗）            │
│       │ - 类型转换：STRING → BIGINT/INT/DATE                     │
│       │ - 数据过滤：status = 1 且 view_count > 0                │
│       │ - 去空值：TRIM、IS NOT NULL                              │
│       ▼                                                          │
│  [4] DWD 层（内部表，ORC/SNAPPY）                                │
│  dwd.dwd_product_clean                                          │
│  - 字段类型正确                                                  │
│  - 数据已清洗                                                    │
│       │                                                          │
│       │ INSERT OVERWRITE TABLE ... SELECT（聚合）                │
│       │ - GROUP BY author_id                                     │
│       │ - COUNT / SUM / AVG                                      │
│       ▼                                                          │
│  [5] DWS 层（内部表，ORC/SNAPPY）                                │
│  dws.dws_seller_stats                                           │
│  - 按作者聚合统计                                                │
│  - 供 ADS 层使用                                                 │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 七、总结

本篇学习了 Hive DML 的核心操作：

1. **INSERT VALUES**：单行和多行插入，分区表的分区指定
2. **INSERT SELECT**：从查询结果插入，ODS → DWD 的 ETL 典型流程
3. **Multi-Table Insert**：一次扫描多表输出，减少重复计算
4. **LOAD DATA**：纯文件移动，快速导入原始数据到 ODS 层
5. **数据导出**：INSERT OVERWRITE DIRECTORY、hive -e、Beeline 导出
6. **UPDATE/DELETE**：普通表不支持，需要使用 ACID 事务表
7. **实战**：CSV → ODS → DWD → DWS 完整数据导入流程

下一篇文章将学习 Hive 内置函数大全，包括数学、字符串、日期、条件、聚合、窗口函数等。
