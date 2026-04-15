# BigDataStudy - 大数据从入门到精通

> 大数据技术从入门到精通的学习仓库，覆盖 11 个组件、80+ 篇课程文档、200+ 道面试题

## 项目简介

这是一个从零学习大数据技术的完整课程体系。所有组件以**个人博客系统**为统一案例贯穿，从基础概念到生产实战，再到面试题，循序渐进。

每个组件包含：
- 渐进式课程文档（概念讲解 + ASCII 原理图 + 代码示例）
- 可运行的实操代码（Python/Java/SQL/Shell）
- 博客系统实战案例
- 高频面试题及详细答案

## 学习路线

```
第一阶段：存储与数仓基础
  1. Hadoop ── 分布式存储（HDFS + YARN + MapReduce）
  2. Hive ─── 数据仓库（SQL 分析、分区表、优化）

第二阶段：批量计算
  3. Spark ─── 批量计算（SQL + DataFrame + RDD + PySpark）
     └─ Pandas ── Python 数据分析（Spark 前置知识）

第三阶段：消息队列与实时计算
  4. Kafka ─── 消息队列（生产者、消费者、分区）
  5. Flink ─── 实时计算（DataStream、窗口、Checkpoint）

第四阶段：缓存与搜索
  6. Redis ─── 内存数据库（缓存、排行榜、分布式锁）
  7. Elasticsearch ── 全文搜索（倒排索引、Query DSL）

第五阶段：OLAP 分析
  8. ClickHouse ── 列式分析（MergeTree、OLAP 查询）

第六阶段：存储与数据湖
  9. MinIO ─── 对象存储（S3 兼容、图片存储）
  10. Iceberg ── 数据湖（时间旅行、Schema 演进）

第七阶段：调度编排
  11. DolphinScheduler ── 任务调度（DAG 工作流编排）
```

## 文件树

```
BigDataStudy/
├── README.md                         <- 项目说明
│
├── 1_hadoop/                          Hadoop 3.4.2 分布式存储
│   ├── 01-HDFS概述与架构.md            HDFS 主从架构与设计哲学
│   ├── 02-环境搭建.md                  安装配置与伪分布式启动
│   ├── 03-HDFS命令行操作.md            Shell 命令管理文件
│   ├── 04-读写流程详解.md              数据写入与读取的完整流程
│   ├── 05-YARN资源调度.md              ResourceManager 与调度器
│   ├── 06-MapReduce编程模型.md         编程模型与 Shuffle 原理
│   ├── 07-配置与调优.md                YARN 与 MapReduce 配置优化
│   ├── 08-运维管理.md                  安全模式、数据平衡、故障排查
│   └── code/                          实操代码
│       ├── WordCount.java             MapReduce Java 版词频统计
│       ├── mapper.py / reducer.py      Hadoop Streaming Python 版
│       └── run_wordcount.sh           一键运行脚本
│
├── 2_hive/                            Hive 4.0.1 数据仓库
│   ├── 01-数据仓库概念.md              数仓分层与 ETL
│   ├── 02-环境搭建.md                  Metastore 与 HiveServer2
│   ├── 03-DDL数据定义.md               建库建表、分区与分桶
│   ├── 04-DML数据操作.md               数据导入导出
│   ├── 05-函数详解.md                  内置函数与 UDF 编写
│   ├── 06-文件格式.md                  TextFile vs Parquet vs ORC
│   ├── 07-查询优化.md                  执行计划与 MapJoin
│   ├── 08-高级特性.md                  视图、物化视图、事务
│   └── code/                          实操代码
│       ├── data/                      示例数据（文章/用户/访问日志）
│       ├── init_blog_warehouse.sql    数仓初始化脚本
│       └── analysis_queries.sql        分析查询示例
│
├── 3_spark/                           Spark 3.5.8 批量计算
│   ├── 01-核心概念.md                  RDD/DataFrame/DataSet 架构
│   ├── 02-环境搭建.md                  Local/YARN/Standalone 模式
│   ├── 03-Spark-SQL基础.md             SQL 查询与 CTE
│   ├── 04-Spark-SQL进阶.md             窗口函数与复杂聚合
│   ├── 05-DataFrame-API.md             DataFrame DSL 编程
│   ├── Pandas数据分析.md               Python Pandas 全函数教程
│   ├── 06-RDD编程.md                   RDD 转换与行动算子
│   ├── 07-Spark与Hive集成.md           读写 Hive 表
│   ├── 08-性能优化.md                  内存管理与广播变量
│   ├── 09-PySpark.md                  Python 接口与 UDF
│   ├── 10-部署模式.md                  提交作业与资源配置
│   └── code/                          实操代码
│       ├── pandas_tutorial.py          Pandas 可运行教程脚本
│       ├── etl_pipeline.py             PySpark ETL 脚本
│       ├── data_analysis.py            PySpark 数据分析脚本
│       └── run_spark_jobs.sh           一键运行脚本
│
├── 4_kafka/                           Kafka 4.2.0 消息队列
│   ├── 01-消息队列概述.md              解耦、异步、削峰
│   ├── 02-安装与配置.md                KRaft 模式与 server.properties
│   ├── 03-Topic与分区.md               分区策略与副本机制
│   ├── 04-生产者详解.md                发送模式与可靠性保证
│   ├── 05-消费者详解.md                消费者组与 Rebalance
│   ├── 06-高级特性.md                  保留策略与日志压缩
│   ├── 07-事件管道实战.md              完整事件管道设计
│   ├── 08-生产与面试.md                集群规划与 15 道面试题
│   └── code/                          实操代码
│       ├── producer.py                 Python 生产者
│       ├── consumer.py                 Python 消费者
│       └── run_kafka_demo.sh           演示脚本
│
├── 5_flink/                           Flink 2.2.0 实时计算
│   ├── 01-实时计算概述.md              批处理 vs 实时、Lambda/Kappa
│   ├── 02-安装与配置.md                Standalone 模式与 Web UI
│   ├── 03-DataStream-API.md            Source/Transformation/Sink
│   ├── 04-状态管理与Checkpoint.md      Exactly-Once 语义
│   ├── 05-窗口与时间.md                Watermark 与窗口类型
│   ├── 06-Flink-SQL.md                 Kafka/ES Connector
│   ├── 07-实时管道实战.md              UV/PV、实时统计
│   ├── 08-生产与面试.md                反压排查与 15 道面试题
│   └── code/                          实操代码
│       ├── BlogEventStatistics.java    Flink DataStream 作业
│       ├── blog_pipeline.sql          Flink SQL 脚本
│       └── run_flink_job.sh            提交脚本
│
├── 6_redis/                           Redis 内存数据库
│   ├── 01-内存数据库概述.md            Redis vs Memcached
│   ├── 02-安装与配置.md                macOS 安装与 redis.conf
│   ├── 03-五大基础数据类型.md          String/Hash/List/Set/ZSet
│   ├── 04-高级特性.md                  持久化、事务、发布订阅
│   ├── 05-博客项目实战.md              缓存、排行榜、分布式锁
│   ├── 06-生产与面试.md                15 道面试题
│   └── code/                          实操代码
│
├── 7_elasticsearch/                   Elasticsearch 9.3.3 全文搜索
│   ├── 01-搜索引擎概述.md              倒排索引与 BM25
│   ├── 02-安装与配置.md                JVM 调优
│   ├── 03-索引与映射.md                text vs keyword
│   ├── 04-文档CRUD操作.md              批量操作与并发控制
│   ├── 05-搜索与Query-DSL.md           bool 查询与高亮
│   ├── 06-聚合分析.md                  桶聚合与指标聚合
│   ├── 07-中文分词与优化.md            IK 分词器与性能优化
│   ├── 08-生产与面试.md                集群规划与 15 道面试题
│   └── code/                          实操代码
│       ├── data/blog_posts.json       博客文章 JSON 数据
│       ├── bulk_import.sh              批量导入脚本
│       └── search_examples.sh          搜索查询示例
│
├── 8_clickhouse/                      ClickHouse OLAP 数据库
│   ├── 01-OLAP与列式存储概述.md         行存储 vs 列存储
│   ├── 02-安装与配置.md                macOS 安装与端口配置
│   ├── 03-表引擎与数据类型.md           MergeTree 家族
│   ├── 04-SQL查询与函数.md             窗口函数与物化视图
│   ├── 05-博客分析实战.md              数据分析与实时报表
│   ├── 06-生产与面试.md                15 道面试题
│   └── code/                          实操代码
│       ├── data/                      日志示例数据
│       ├── init_tables.sql             建表脚本
│       ├── analysis_queries.sql        分析查询
│       └── blog_analytics.py           Python 分析脚本
│
├── 9_minio/                           MinIO 对象存储
│   ├── 01-对象存储概述.md              三种存储对比
│   ├── 02-安装与配置.md                Go 二进制部署
│   ├── 03-mc命令行工具.md              Bucket 与文件管理
│   ├── 04-Python-SDK编程.md            minio-py 操作
│   ├── 05-进阶特性.md                  版本控制与生命周期
│   ├── 06-生产与面试.md                10 道面试题
│   └── code/                          实操代码
│
├── 10_iceberg/                         Iceberg 1.10.1 数据湖
│   ├── 01-数据湖概述.md                湖仓一体与三大表格式对比
│   ├── 02-安装与配置.md                Spark 集成与 JdbcCatalog
│   ├── 03-表操作.md                    CRUD 与 MERGE INTO
│   ├── 04-时间旅行与快照.md            历史数据查询
│   ├── 05-分区与模式演进.md            隐藏分区与 Schema 变更
│   ├── 06-生产与面试.md                10 道面试题
│   └── code/                          实操代码
│       └── iceberg_demo.sql            Spark SQL 演示脚本
│
├── 11_dolphinscheduler/                DolphinScheduler 3.4.1 任务调度
│   ├── 01-任务调度概述.md              DS vs Airflow
│   ├── 02-安装与配置.md                Standalone 部署
│   ├── 03-核心概念与Web-UI.md          任务类型与依赖管理
│   ├── 04-DAG工作流开发.md             参数传递与条件分支
│   ├── 05-数据管道编排.md              4 个工作流设计
│   ├── 06-生产与面试.md                10 道面试题
│   └── code/task_examples/            实操代码
│       ├── check_data_quality.sh       数据质量检查
│       ├── export_to_es.sh             导出数据到 ES
│       └── send_notification.sh        通知脚本
│
└── milvus/                            Milvus 向量数据库（待安装）
```

## 环境说明

| 项目 | 版本 |
|------|------|
| 操作系统 | macOS (Apple Silicon) |
| JDK | 21 |
| Python | 3.9 |
| MySQL | 8.0 |
| Hadoop | 3.4.2 |
| Hive | 4.0.1 |
| Spark | 3.5.8 |
| Kafka | 4.2.0 (KRaft) |
| Flink | 2.2.0 |
| Redis | 最新版 |
| Elasticsearch | 9.3.3 |
| ClickHouse | 最新版 |
| MinIO | 最新版 |
| Iceberg | 1.10.1 |
| DolphinScheduler | 3.4.1 |

## 大数据技术栈架构

```
博客事件 ──→ Kafka ──→ Flink ──→ Elasticsearch（全文搜索）
                          │
                          → Hive / Iceberg（离线分析）
静态文件   ──→ MinIO
               Spark（批量 ETL）→ ClickHouse（OLAP 分析）
               Redis（缓存、排行榜）
DolphinScheduler（调度编排）
HDFS（底层存储）
```

## 面试题统计

共 **200+ 道**面试题，覆盖所有组件的高频考点。
