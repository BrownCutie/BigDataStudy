# Hive DDL — 数据定义语言详解

> Hive 4.0.1 学习系列 - 第 3 篇
> 适用环境：macOS Apple Silicon / JDK 21 / Hive 4.0.1

## 一、数据库操作

### 1.1 CREATE DATABASE

```sql
-- 基础语法
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];

-- 创建数据平台数仓各层级的数据库
CREATE DATABASE IF NOT EXISTS ods   COMMENT '贴源层 - 原始数据存储';
CREATE DATABASE IF NOT EXISTS dwd   COMMENT '明细层 - 清洗后明细数据';
CREATE DATABASE IF NOT EXISTS dws   COMMENT '汇总层 - 轻度聚合数据';
CREATE DATABASE IF NOT EXISTS ads   COMMENT '应用层 - 面向应用的汇总';
CREATE DATABASE IF NOT EXISTS dim   COMMENT '维度层 - 维度表';

-- 指定 HDFS 存储位置
CREATE DATABASE IF NOT EXISTS data_warehouse
    COMMENT '数据平台数仓专用数据库'
    LOCATION '/user/hive/data_warehouse.db'
    WITH DBPROPERTIES ('creator'='browncutie', 'create_time'='2026-04-15');
```

数据库在 HDFS 上的存储结构：

```
/user/hive/warehouse/
├── ods.db/                    ← CREATE DATABASE ods
│   ├── ods_product_article/
│   └── ods_access_log/
├── dwd.db/                    ← CREATE DATABASE dwd
│   ├── dwd_access_detail/
│   └── dwd_article_publish/
├── dws.db/                    ← CREATE DATABASE dws
├── ads.db/                    ← CREATE DATABASE ads
└── dim.db/                    ← CREATE DATABASE dim
```

### 1.2 USE / SHOW / DROP

```sql
-- 切换数据库
USE ods;

-- 查看当前使用的数据库
SELECT CURRENT_DATABASE();

-- 查看所有数据库
SHOW DATABASES;

-- 模糊匹配数据库名
SHOW DATABASES LIKE 'd*';

-- 查看数据库详细信息
DESCRIBE DATABASE ods;
-- 输出：
-- ods          hdfs://localhost:9000/user/hive/warehouse/ods.db  browncutie

-- 查看数据库扩展信息（包含 DBPROPERTIES）
DESCRIBE DATABASE EXTENDED ods;

-- 删除空数据库
DROP DATABASE IF EXISTS test_db;

-- 删除非空数据库（CASCADE 会删除库下所有表和数据）
DROP DATABASE IF EXISTS test_db CASCADE;

-- ⚠️ 警告：CASCADE 删除不可恢复，生产环境请谨慎使用
```

## 二、CREATE TABLE 详解

### 2.1 完整语法结构

```sql
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
  (col_name data_type [COMMENT col_comment], ...)
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [CLUSTERED BY (col_name, col_name, ...)
    [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  [SKEWED BY (col_name, ...) ON ((col_value, ...), (...)) [STORED AS DIRECTORIES]
  [
    [ROW FORMAT row_format]
    [STORED AS file_format]
    | STORED BY 'storage.handler.class' [WITH SERDEPROPERTIES (...)]
  ]
  [LOCATION hdfs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]
```

### 2.2 内部表 vs 外部表 vs 临时表

这是 Hive 中最重要的概念之一，三种表的区别如下：

```
┌──────────────────────────────────────────────────────────────────┐
│                    三种表类型对比                                 │
│                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐   │
│  │   内部表         │  │   外部表         │  │   临时表       │   │
│  │ (Managed Table) │  │(External Table) │  │(Temporary Table)│   │
│  ├─────────────────┤  ├─────────────────┤  ├────────────────┤   │
│  │ 关键字: 无       │  │ 关键字: EXTERNAL│  │ 关键字:TEMPORARY│  │
│  │                 │  │                 │  │                │   │
│  │ Hive 管理数据    │  │ Hive 仅管元数据  │  │ 仅当前会话可见  │   │
│  │ 和元数据        │  │ 数据由用户管理   │  │ 会话结束自动删除│   │
│  │                 │  │                 │  │                │   │
│  │ DROP 时删除     │  │ DROP 时只删     │  │ 无需手动删除    │   │
│  │ 元数据 + 数据   │  │ 元数据，数据保留 │  │                │   │
│  │                 │  │                 │  │                │   │
│  │ 适用: 数仓内部  │  │ 适用: ODS 层、   │  │ 适用: 临时中间  │   │
│  │ 中间表         │  │ 共享数据源      │  │ 结果           │   │
│  └─────────────────┘  └─────────────────┘  └────────────────┘   │
│                                                                  │
│  删除行为对比：                                                   │
│                                                                  │
│  DROP TABLE managed_table:                                       │
│    → 元数据删除 (MySQL Metastore 中的记录)                       │
│    → 数据删除 (HDFS 上的文件)      ← 数据丢失!                   │
│                                                                  │
│  DROP TABLE external_table:                                      │
│    → 元数据删除 (MySQL Metastore 中的记录)                       │
│    → 数据保留 (HDFS 上的文件不动)  ← 数据安全                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**数据平台项目中的使用建议：**

```
+------------+------------------+--------------------------------+
|   表类型   |    使用层级      |          理由                  |
+------------+------------------+--------------------------------+
| 外部表     | ODS 层           | 原始数据来自外部系统，不归     |
| EXTERNAL   |                  | Hive 管理，删除表不应丢失数据  |
+------------+------------------+--------------------------------+
| 内部表     | DWD / DWS / ADS  | 数仓加工后的数据，随表生命周期 |
| (默认)     |                  | 一起管理                       |
+------------+------------------+--------------------------------+
| 临时表     | 复杂查询中间结果  | 仅当前会话使用，用完即丢       |
| TEMPORARY  |                  |                                |
+------------+------------------+--------------------------------+
```

### 2.3 建表示例

```sql
-- ========== 外部表（ODS 层）==========
CREATE EXTERNAL TABLE ods.ods_product_article (
    id              BIGINT          COMMENT '文章ID',
    title           STRING          COMMENT '文章标题',
    content         STRING          COMMENT '文章内容',
    author_id       BIGINT          COMMENT '作者ID',
    category_id     INT             COMMENT '分类ID',
    status          INT             COMMENT '状态: 0下架 1在售 2售罄',
    view_count      BIGINT          COMMENT '访问量',
    like_count  INT             COMMENT '点赞数',
    created_at      STRING          COMMENT '创建时间',
    updated_at      STRING          COMMENT '更新时间'
)
COMMENT 'ODS层-文章原始数据'
PARTITIONED BY (dt STRING COMMENT '数据日期 yyyy-MM-dd')
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/ods/product_article';

-- ========== 内部表（DWD 层）==========
CREATE TABLE dwd.dwd_product_publish (
    post_id      BIGINT          COMMENT '文章ID',
    title           STRING          COMMENT '文章标题',
    author_id       BIGINT          COMMENT '作者ID',
    seller_name     STRING          COMMENT '作者名称',
    category_id     INT             COMMENT '分类ID',
    category_name   STRING          COMMENT '分类名称',
    price           DOUBLE          COMMENT '价格',
    publish_date    DATE            COMMENT '发布日期',
    tags            STRING          COMMENT '标签，逗号分隔'
)
COMMENT 'DWD层-文章发布明细事实表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'create.user'  = 'browncutie',
    'create.time'  = '2026-04-15'
);

-- ========== 临时表（复杂查询中间结果）==========
CREATE TEMPORARY TABLE tmp_article_stats (
    article_id      BIGINT,
    total_pv        BIGINT,
    total_uv        BIGINT
);
-- 注意：临时表不需要指定数据库，仅在当前会话可见
-- 会话结束或关闭 Beeline 后自动删除
```

## 三、数据类型大全

### 3.1 基本数据类型

```
┌──────────────────────────────────────────────────────────────────┐
│                     Hive 基本数据类型                             │
│                                                                  │
│  整数类型                          浮点类型                       │
│  ┌──────────┬──────────┐          ┌──────────┬──────────┐       │
│  │  类型    │   范围   │          │  类型    │   说明   │       │
│  ├──────────┼──────────┤          ├──────────┼──────────┤       │
│  │ TINYINT  │ -128~127 │          │ FLOAT    │ 单精度   │       │
│  │ SMALLINT │ ±3.2万   │          │ DOUBLE   │ 双精度   │       │
│  │ INT      │ ±21亿    │          │ DECIMAL  │ 精确小数 │       │
│  │ BIGINT   │ ±922亿亿 │          │ (p,s)    │ p=精度   │       │
│  └──────────┴──────────┘          │          │ s=小数位 │       │
│                                   └──────────┴──────────┘       │
│  日期时间类型                      字符串类型                     │
│  ┌──────────┬──────────┐          ┌──────────┬──────────┐       │
│  │  类型    │   说明   │          │  类型    │   说明   │       │
│  ├──────────┼──────────┤          ├──────────┼──────────┤       │
│  │ TIMESTAMP│ 日期时间 │          │ STRING   │ 变长字符 │       │
│  │ DATE     │ 日期     │          │ CHAR(n)  │ 定长字符 │       │
│  │ INTERVAL │ 时间间隔 │          │ VARCHAR  │ 变长字符 │       │
│  └──────────┴──────────┘          └──────────┴──────────┘       │
│                                                                  │
│  布尔类型                                                       │
│  ┌──────────┬──────────┐                                       │
│  │  类型    │   说明   │                                       │
│  ├──────────┼──────────┤                                       │
│  │ BOOLEAN  │ true/false│                                       │
│  └──────────┴──────────┘                                       │
│                                                                  │
│  二进制类型                                                     │
│  ┌──────────┬──────────┐                                       │
│  │  类型    │   说明   │                                       │
│  ├──────────┼──────────┤                                       │
│  │ BINARY   │ 字节数组 │                                       │
│  └──────────┴──────────┘                                       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 复杂数据类型

Hive 支持三种复杂数据类型，这在处理嵌套数据时非常有用：

```
ARRAY（数组）：
  存储同类型元素的有序集合
  ┌─────────────────────────────┐
  │ tags: ARRAY<STRING>         │
  │                             │
  │ ["Hive", "大数据", "数仓"]  │
  │                             │
  │ 访问: tags[0] → "Hive"     │
  │ 访问: tags[1] → "大数据"   │
  │ 长度: SIZE(tags) → 3        │
  └─────────────────────────────┘

MAP（映射）：
  存储键值对集合
  ┌─────────────────────────────────┐
  │ props: MAP<STRING, STRING>      │
  │                                 │
  │ {                              │
  │   "difficulty" → "中级",        │
  │   "duration"   → "2小时",       │
  │   "author"     → "张三"         │
  │ }                              │
  │                                 │
  │ 访问: props["difficulty"]       │
  │       → "中级"                  │
  └─────────────────────────────────┘

STRUCT（结构体）：
  存储不同类型字段的命名集合
  ┌─────────────────────────────────┐
  │ author: STRUCT<                 │
  │   name: STRING,                 │
  │   email: STRING,                │
  │   age: INT                      │
  │ >                               │
  │                                 │
  │ 访问: author.name  → "张三"     │
  │ 访问: author.email → "xx@xx.com"│
  │ 访问: author.age   → 25         │
  └─────────────────────────────────┘
```

**数据平台项目中的实际应用：**

```sql
-- 使用复杂类型的文章表
CREATE TABLE dim.dim_product_ext (
    post_id      BIGINT          COMMENT '文章ID',
    title           STRING          COMMENT '名称',
    tags            ARRAY<STRING>   COMMENT '标签数组',
    metadata        MAP<STRING, STRING> COMMENT '元数据键值对',
    seller_info     STRUCT<
                        name: STRING,
                        email: STRING,
                        role: STRING
                    >                COMMENT '作者信息结构体'
)
COMMENT '文章扩展维度表（使用复杂类型）'
STORED AS ORC;

-- 插入复杂数据类型的数据
INSERT INTO dim.dim_product_ext VALUES (
    1001,
    'MacBook Pro 16寸',
    ARRAY('笔记本', '苹果', '办公'),
    MAP('difficulty', '高端', 'duration', '3天发货', 'views', '1250'),
    STRUCT('张三', 'zhangsan@data.com', 'admin')
);

-- 查询复杂类型字段
SELECT
    post_id,
    title,
    tags[0]                        AS first_tag,        -- 数组索引访问
    tags[1]                        AS second_tag,
    SIZE(tags)                     AS tag_count,        -- 数组长度
    metadata['difficulty']         AS difficulty,       -- MAP 键访问
    metadata['duration']           AS duration,
    seller_info.name               AS seller_name,      -- STRUCT 字段访问
    seller_info.email              AS seller_email
FROM dim.dim_product_ext;
```

## 四、分区表

### 4.1 分区原理

分区是 Hive 中最重要的性能优化手段之一。分区实际上是在 HDFS 上为不同的分区值创建不同的目录，查询时通过"分区裁剪"只扫描相关目录，大幅减少数据扫描量：

```
不使用分区：
  /user/hive/warehouse/dwd_access_detail/
  ├── 000000_0          ← 包含所有日期的数据
  ├── 000001_0
  └── 000002_0

  SELECT * FROM dwd_access_detail WHERE dt = '2026-04-15';
  → 需要扫描全部文件！效率极低

使用分区：
  /user/hive/warehouse/dwd_access_detail/
  ├── dt=2026-04-13/
  │   ├── 000000_0     ← 只有 4月13日 的数据
  │   └── 000001_0
  ├── dt=2026-04-14/
  │   └── 000000_0     ← 只有 4月14日 的数据
  └── dt=2026-04-15/
      ├── 000000_0     ← 只有 4月15日 的数据
      └── 000001_0

  SELECT * FROM dwd_access_detail WHERE dt = '2026-04-15';
  → 只扫描 dt=2026-04-15/ 目录！分区裁剪生效
```

### 4.2 单分区表

```sql
-- 创建单分区表（按日期分区）
CREATE TABLE dwd.dwd_access_detail (
    access_id       STRING          COMMENT '访问唯一ID',
    user_id         BIGINT          COMMENT '用户ID',
    article_id      BIGINT          COMMENT '文章ID',
    ip              STRING          COMMENT '访客IP',
    duration_sec    INT             COMMENT '停留秒数'
)
PARTITIONED BY (dt STRING COMMENT '日期分区 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 插入数据到指定分区
INSERT INTO TABLE dwd.dwd_access_detail PARTITION (dt = '2026-04-15')
VALUES
    ('a001', 1001, 201, '192.168.1.1', 120),
    ('a002', 1002, 201, '192.168.1.2', 85),
    ('a003', NULL, 202, '10.0.0.1', 45);

-- 查询指定分区（分区裁剪）
SELECT * FROM dwd.dwd_access_detail WHERE dt = '2026-04-15';

-- 查看分区列表
SHOW PARTITIONS dwd.dwd_access_detail;

-- 手动添加分区（通常用于外部表）
ALTER TABLE dwd.dwd_access_detail ADD PARTITION (dt = '2026-04-16');

-- 删除分区
ALTER TABLE dwd.dwd_access_detail DROP PARTITION (dt = '2026-04-16');
```

### 4.3 多级分区表

```sql
-- 创建多级分区表（按年/月分区）
CREATE TABLE dwd.dwd_access_detail_multi (
    access_id       STRING          COMMENT '访问唯一ID',
    user_id         BIGINT          COMMENT '用户ID',
    article_id      BIGINT          COMMENT '文章ID',
    ip              STRING          COMMENT '访客IP',
    duration_sec    INT             COMMENT '停留秒数'
)
PARTITIONED BY (
    year    STRING  COMMENT '年 yyyy',
    month   STRING  COMMENT '月 MM'
)
STORED AS ORC;

-- 插入数据到多级分区
INSERT INTO TABLE dwd.dwd_access_detail_multi PARTITION (year = '2026', month = '04')
VALUES ('b001', 2001, 301, '172.16.0.1', 200);

-- HDFS 目录结构：
-- /user/hive/warehouse/dwd.db/dwd_access_detail_multi/
-- ├── year=2026/
-- │   ├── month=01/
-- │   ├── month=02/
-- │   ├── month=03/
-- │   └── month=04/
-- │       └── 000000_0
-- └── year=2025/
--     └── month=12/

-- 查询时分区裁剪（两级都生效）
SELECT * FROM dwd.dwd_access_detail_multi
WHERE year = '2026' AND month = '04';

-- 查看多级分区
SHOW PARTITIONS dwd.dwd_access_detail_multi;
-- 输出：
-- year=2026/month=04
```

**分区设计原则：**

```
+------------------+----------------------------------------------+
|     原则         |                 说明                          |
+------------------+----------------------------------------------+
| 不要过度分区     | 单个分区文件不宜过小（建议 > 128MB）          |
| 常用查询字段分区 | 选择 WHERE 条件中最常用的字段作为分区字段     |
| 分区层级 2-3 级  | 层级过深导致目录过多，NameNode 压力大         |
| 避免数据倾斜     | 某个分区值数据量过大时，考虑细分              |
+------------------+----------------------------------------------+
```

## 五、分桶表

### 5.1 分桶原理

分桶（Bucketing）是对文件进一步细分的方法。与分区的"目录级"划分不同，分桶是在文件内部按指定列的哈希值将数据分散到固定数量的文件中：

```
分区 vs 分桶：

分区（Partition）：按值划分目录
  table/
  ├── dt=2026-04-15/     ← 目录
  │   └── data files
  └── dt=2026-04-16/     ← 目录
      └── data files

分桶（Bucket）：按哈希值划分文件
  table/
  ├── 000000_0           ← bucket 0 (hash(col) % num_buckets = 0)
  ├── 000001_0           ← bucket 1 (hash(col) % num_buckets = 1)
  ├── 000002_0           ← bucket 2 (hash(col) % num_buckets = 2)
  └── 000003_0           ← bucket 3 (hash(col) % num_buckets = 3)

分区 + 分桶结合：
  table/
  └── dt=2026-04-15/
      ├── 000000_0       ← 分区内的 bucket 0
      ├── 000001_0       ← 分区内的 bucket 1
      ├── 000002_0       ← 分区内的 bucket 2
      └── 000003_0       ← 分区内的 bucket 3
```

### 5.2 创建分桶表

```sql
-- 创建分桶表（按 user_id 分 4 个桶）
CREATE TABLE dwd.dwd_user_access_bucketed (
    access_id       STRING          COMMENT '访问ID',
    user_id         BIGINT          COMMENT '用户ID',
    article_id      BIGINT          COMMENT '文章ID',
    duration_sec    INT             COMMENT '停留秒数'
)
CLUSTERED BY (user_id) INTO 4 BUCKETS
STORED AS ORC;

-- 分桶表数据写入需要强制开启分桶
SET hive.enforce.bucketing = true;

-- 插入数据（必须使用 INSERT，不能 LOAD）
INSERT INTO TABLE dwd.dwd_user_access_bucketed
SELECT access_id, user_id, article_id, duration_sec
FROM dwd.dwd_access_detail
WHERE dt = '2026-04-15';

-- 分桶的主要用途：优化 JOIN 操作（SMB Join）
-- 当两个表按相同列分桶且桶数相同或成倍数关系时，
-- Hive 可以只做桶级别的 JOIN，避免全表扫描
```

## 六、ALTER TABLE

### 6.1 修改表结构

```sql
-- ========== 添加列 ==========
ALTER TABLE dwd.dwd_article_publish
ADD COLUMNS (
    comment_count   INT     COMMENT '评论数',
    collect_count   INT     COMMENT '收藏数'
);

-- ========== 修改列名 ==========
ALTER TABLE dwd.dwd_article_publish
CHANGE COLUMN word_count article_word_count INT COMMENT '文章字数';

-- ========== 修改列类型 ==========
-- 注意：Hive 对类型变更有限制，只能从小范围改为大范围
-- INT → BIGINT ✓    STRING → INT ✗
ALTER TABLE dwd.dwd_article_publish
CHANGE COLUMN article_word_count article_word_count BIGINT COMMENT '文章字数';

-- ========== 修改表属性 ==========
-- 修改表注释
ALTER TABLE dwd.dwd_article_publish SET TBLPROPERTIES (
    'comment' = 'DWD层-文章发布明细（已添加评论和收藏字段）'
);

-- 修改表名
ALTER TABLE dwd.dwd_article_publish RENAME TO dwd.dwd_article_publish_v2;

-- ========== 修改分区 ==========
-- 添加分区
ALTER TABLE dwd.dwd_access_detail ADD IF NOT EXISTS
    PARTITION (dt = '2026-04-17')
    PARTITION (dt = '2026-04-18');

-- 修复分区（将 HDFS 上已有数据目录注册到 Metastore）
-- MSCK REPAIR TABLE 会扫描 HDFS 目录，自动补全缺失的分区
MSCK REPAIR TABLE dwd.dwd_access_detail;

-- 删除分区
ALTER TABLE dwd.dwd_access_detail DROP IF EXISTS PARTITION (dt = '2026-04-17');
```

### 6.2 ALTER TABLE 操作汇总

```
┌──────────────────────────────────────────────────────────────────┐
│                   ALTER TABLE 操作一览                            │
│                                                                  │
│  表结构修改：                                                    │
│  ├── ADD COLUMNS (...)              添加新列                    │
│  ├── CHANGE COLUMN old new type     修改列名/类型               │
│  ├── REPLACE COLUMNS (...)          替换所有列                  │
│  └── DROP [COLUMN] column_name      删除列（4.x 需要配置）      │
│                                                                  │
│  表属性修改：                                                    │
│  ├── SET TBLPROPERTIES (...)          设置表属性                │
│  ├── UNSET TBLPROPERTIES (...)        删除表属性                │
│  └── RENAME TO new_table_name         重命名表                  │
│                                                                  │
│  分区操作：                                                      │
│  ├── ADD PARTITION (...)               添加分区                  │
│  ├── DROP PARTITION (...)              删除分区                  │
│  ├── PARTITION (...) SET LOCATION      修改分区存储位置          │
│  └── MSCK REPAIR TABLE                 修复分区                  │
│                                                                  │
│  存储格式修改：                                                  │
│  └── SET FILEFORMAT (ORC|PARQUET|...)  修改存储格式              │
│                                                                  │
│  SerDe 修改：                                                    │
│  └── SET SERDE / SET SERDEPROPERTIES   修改序列化/反序列化配置  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 七、SHOW / DESCRIBE 命令

```sql
-- ========== SHOW 系列 ==========

-- 查看所有数据库
SHOW DATABASES;
SHOW DATABASES LIKE 'd*';

-- 查看当前数据库的表
SHOW TABLES;
SHOW TABLES LIKE 'dwd_*';

-- 查看表的分区
SHOW PARTITIONS dwd.dwd_access_detail;
SHOW PARTITIONS dwd.dwd_access_detail PARTITION(dt = '2026-04-15');

-- 查看表的列信息
SHOW COLUMNS FROM dwd.dwd_article_publish;

-- 查看表的属性
SHOW TBLPROPERTIES dwd.dwd_article_publish;

-- 查看表的创建语句（非常重要！）
SHOW CREATE TABLE dwd.dwd_article_publish;

-- 查看函数
SHOW FUNCTIONS;
SHOW FUNCTIONS LIKE '*date*';

-- ========== DESCRIBE 系列 ==========

-- 查看表结构（列名、类型、注释）
DESCRIBE dwd.dwd_article_publish;
-- 等价于
DESC dwd.dwd_article_publish;
DESC FORMATTED dwd.dwd_article_publish;

-- 查看指定列的详细信息
DESCRIBE dwd.dwd_article_publish article_id;

-- 查看分区字段
DESCRIBE dwd.dwd_access_detail dt;
```

## 八、COMMENT 和 TBLPROPERTIES

### 8.1 COMMENT 注释

注释是数仓管理中最重要的元数据之一，好的注释能让表结构自解释：

```sql
-- 数据库注释
CREATE DATABASE IF NOT EXISTS dwd
    COMMENT '明细数据层 - 存放经过清洗和标准化的明细事实数据';

-- 表注释
CREATE TABLE dwd.dwd_access_detail (
    -- 列注释
    access_id       STRING      COMMENT '访问唯一标识，格式为UUID',
    user_id         BIGINT      COMMENT '用户ID，未登录访客为NULL',
    article_id      BIGINT      COMMENT '被访问的文章ID',
    ip              STRING      COMMENT '访客IP地址，已脱敏处理',
    user_agent      STRING      COMMENT '浏览器User-Agent字符串',
    duration_sec    INT         COMMENT '页面停留时长（秒），最小单位秒'
)
COMMENT 'DWD层-访问明细事实表，从ODS层解析清洗而来，粒度为单次访问'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式yyyy-MM-dd');

-- 修改列注释
ALTER TABLE dwd.dwd_access_detail
CHANGE COLUMN duration_sec duration_sec INT COMMENT '页面停留时长（秒），超时30分钟记为0';
```

### 8.2 TBLPROPERTIES 表属性

TBLPROPERTIES 用于存储表的元属性，常见用途包括：

```sql
-- 创建时设置属性
CREATE TABLE dwd.dwd_article_publish (
    article_id      BIGINT,
    title           STRING
)
STORED AS ORC
TBLPROPERTIES (
    -- 压缩配置
    'orc.compress'          = 'SNAPPY',
    -- 业务属性
    'owner'                 = 'data-team',
    'team'                  = 'analytics',
    'create_date'           = '2026-04-15',
    'update_frequency'      = 'daily',
    'data_quality_level'    = 'cleaned',
    -- 事务表属性
    'transactional'         = 'true'
);

-- 修改属性
ALTER TABLE dwd.dwd_article_publish
SET TBLPROPERTIES (
    'update_frequency' = 'hourly',
    'owner' = 'data-team'
);

-- 删除属性
ALTER TABLE dwd.dwd_article_publish
UNSET TBLPROPERTIES ('data_quality_level');

-- 查看属性
SHOW TBLPROPERTIES dwd.dwd_article_publish;
-- 或
DESC FORMATTED dwd.dwd_article_publish;
-- 在输出中找到 "Table Parameters:" 部分
```

**常用 TBLPROPERTIES 键值：**

```
+---------------------------+--------------------------------------+
|          属性键            |              说明                    |
+---------------------------+--------------------------------------+
| orc.compress              | ORC 压缩编码 (SNAPPY/ZSTD/GZIP)     |
| parquet.compression       | Parquet 压缩编码                     |
| transactional             | 是否为 ACID 事务表                   |
| external.table.purge      | 删除外部表时是否同时删除数据          |
| skip.header.line.count    | 跳过文件前 N 行（处理 CSV 表头）     |
| auto.purge                | 删除时是否跳过回收站直接删除          |
| last_modified_by          | 最后修改人                           |
| last_modified_time        | 最后修改时间                         |
+---------------------------+--------------------------------------+
```

## 九、总结

本篇学习了 Hive DDL 的核心内容：

1. **数据库操作**：CREATE / USE / DROP DATABASE，理解 HDFS 存储结构
2. **三种表类型**：内部表（Hive 管理数据）、外部表（用户管理数据）、临时表（会话级）
3. **数据类型**：基本类型（INT、STRING、DATE 等）和复杂类型（ARRAY、MAP、STRUCT）
4. **分区表**：通过目录级划分实现分区裁剪，单分区和多级分区
5. **分桶表**：通过哈希值划分文件，优化 JOIN 性能
6. **ALTER TABLE**：加列、改列、改属性、分区操作
7. **注释和属性**：COMMENT 自解释、TBLPROPERTIES 管理元数据

下一篇文章将学习 Hive DML — 数据操作语言，包括数据的插入、加载、导出和更新。
