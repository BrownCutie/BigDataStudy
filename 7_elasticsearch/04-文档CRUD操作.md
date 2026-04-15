# 04 - 文档 CRUD 操作：数据的增删改查

## 一、文档的基本概念

在 Elasticsearch 中，**文档（Document）** 是最小的数据单元，以 JSON 格式存储。你可以把它类比为关系型数据库中的一行记录。

```
┌──────────────────────────────────────────────────────────────┐
│                   文档的结构                                   │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  {                                                           │
│    "_index": "posts",       ← 文档所属的索引             │
│    "_id": "1",                  ← 文档的唯一标识              │
│    "_version": 1,               ← 版本号（乐观并发控制）       │
│    "_seq_no": 0,                ← 序列号（ES 7.x+）           │
│    "_primary_term": 1,          ← 主项（ES 7.x+）             │
│    "_source": {                 ← 文档的实际内容（JSON）       │
│      "title": "Kafka 入门教程",                              │
│      "content": "Apache Kafka 是一个分布式流处理平台",         │
│      "tags": ["kafka", "大数据"],                            │
│      "author": "browncutie",                                 │
│      "created_at": "2026-04-15",                             │
│      "views": 100                                           │
│    }                                                        │
│  }                                                          │
│                                                              │
│  _index + _id → 唯一确定一个文档                              │
│  _source   → 文档的原始 JSON 数据                             │
│  _version  → 每次修改自增，用于乐观并发控制                    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 二、创建文档

### 2.1 指定文档 ID 创建（PUT）

```bash
curl -X PUT "http://localhost:9200/posts/_doc/1" -H 'Content-Type: application/json' -d '{
  "title": "Kafka 入门教程",
  "content": "Apache Kafka 是一个分布式流处理平台，广泛用于消息队列和流式数据处理",
  "summary": "Kafka 核心概念详解，包括 Topic、Partition、Consumer Group",
  "tags": ["kafka", "大数据", "消息队列"],
  "category": "大数据",
  "author": "browncutie",
  "created_at": "2026-04-15 10:30:00",
  "views": 100,
  "likes": 20,
  "is_published": true,
  "is_top": false,
  "comments": 5,
  "url": "/posts/kafka-intro"
}'
```

返回结果：

```json
{
  "_index": "posts",
  "_id": "1",
  "_version": 1,
  "result": "created",
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  }
}
```

### 2.2 自动生成 ID 创建（POST）

```bash
curl -X POST "http://localhost:9200/posts/_doc" -H 'Content-Type: application/json' -d '{
  "title": "Spark 入门教程",
  "content": "Apache Spark 是一个快速的大数据处理引擎",
  "tags": ["spark", "大数据"],
  "category": "大数据",
  "author": "browncutie",
  "created_at": "2026-04-15 14:00:00",
  "views": 200,
  "likes": 50,
  "is_published": true,
  "is_top": false,
  "comments": 10,
  "url": "/posts/spark-intro"
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│              PUT vs POST 创建文档                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  PUT /_doc/1  → 指定 ID                                       │
│    - ID 由你指定（通常是数据库的主键）                          │
│    - 如果 ID 已存在，会全量覆盖（_version +1）                 │
│    - 幂等操作：多次执行结果相同                                │
│                                                              │
│  POST /_doc    → 自动生成 ID                                   │
│    - ES 自动生成一个 22 字符的 Base64 URL-safe ID              │
│    - 永远是创建新文档                                          │
│    - 非幂等操作：每次执行都会创建一个新文档                     │
│                                                              │
│  什么时候用什么？                                              │
│  - 从数据库同步数据 → 用 PUT（用数据库主键作为 ES 的 _id）      │
│  - 日志数据、事件数据 → 用 POST（自动生成 ID 即可）             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 2.3 create 操作（ID 已存在则报错）

```bash
# 如果 ID=1 已存在，create 操作会报错（不会覆盖）
curl -X PUT "http://localhost:9200/posts/_create/1" -H 'Content-Type: application/json' -d '{
  "title": "新文章"
}'

# 返回 409 Conflict：
# "reason": "[1]: version conflict, document already exists"
```

## 三、读取文档

### 3.1 根据 ID 获取文档

```bash
curl http://localhost:9200/posts/_doc/1?pretty
```

返回完整的文档，包括 `_source` 和元数据。

### 3.2 只获取 `_source` 内容

```bash
curl http://localhost:9200/posts/_source/1?pretty
```

### 3.3 判断文档是否存在

```bash
curl -I http://localhost:9200/posts/_doc/1

# 200 OK → 存在
# 404 Not Found → 不存在
```

### 3.4 获取多个文档（_mget）

```bash
curl -X GET "http://localhost:9200/posts/_mget" -H 'Content-Type: application/json' -d '{
  "ids": ["1", "2"]
}'
```

### 3.5 使用 _source 过滤返回字段

```bash
# 只返回 title 和 views 字段
curl -X GET "http://localhost:9200/posts/_doc/1?_source=title,views" | python3 -m json.tool
```

## 四、更新文档

### 4.1 全量更新（PUT）

```bash
# 全量更新：整个 _source 被替换
curl -X PUT "http://localhost:9200/posts/_doc/1" -H 'Content-Type: application/json' -d '{
  "title": "Kafka 入门教程（更新版）",
  "content": "更新后的内容",
  "tags": ["kafka", "大数据"],
  "category": "大数据",
  "author": "browncutie",
  "created_at": "2026-04-15 10:30:00",
  "views": 150,
  "likes": 30,
  "is_published": true,
  "is_top": false,
  "comments": 8,
  "url": "/posts/kafka-intro"
}'
```

> **注意：** 全量更新需要提供完整的文档内容，未提供的字段会被删除！

### 4.2 部分更新（_update）

```bash
# 部分更新：只修改指定字段，其他字段保持不变
curl -X POST "http://localhost:9200/posts/_update/1" -H 'Content-Type: application/json' -d '{
  "doc": {
    "views": 151,
    "likes": 31,
    "comments": 9
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│              全量更新 vs 部分更新                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  PUT _doc/1（全量更新）                                       │
│    - 整个文档被替换                                            │
│    - 必须提供所有字段                                          │
│    - 遗漏字段会丢失                                            │
│                                                              │
│  POST _update/1（部分更新）                                    │
│    - 只修改指定字段                                            │
│    - 其他字段保持不变                                          │
│    - 底层实现：先读取旧文档 → 合并 → 全量写入新文档             │
│    - 多一次读操作，但使用更安全                                │
│                                                              │
│  建议：修改少量字段用 _update，替换整个文档用 PUT              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 4.3 脚本更新

使用 Painless 脚本进行更灵活的更新：

```bash
# 浏览量 +1
curl -X POST "http://localhost:9200/posts/_update/1" -H 'Content-Type: application/json' -d '{
  "script": {
    "source": "ctx._source.views += params.increment",
    "params": {
      "increment": 1
    }
  }
}'
```

```bash
# 添加标签（如果不存在的话）
curl -X POST "http://localhost:9200/posts/_update/1" -H 'Content-Type: application/json' -d '{
  "script": {
    "source": "if (!ctx._source.tags.contains(params.tag)) { ctx._source.tags.add(params.tag) }",
    "params": {
      "tag": "流处理"
    }
  }
}'
```

### 4.4 Upsert（更新或插入）

```bash
# 文档存在则更新，不存在则创建
curl -X POST "http://localhost:9200/posts/_update/999" -H 'Content-Type: application/json' -d '{
  "doc": {
    "views": 100
  },
  "upsert": {
    "title": "新文章标题",
    "content": "新文章内容",
    "views": 100,
    "author": "browncutie",
    "created_at": "2026-04-15"
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│                    Upsert 流程                                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  _update/999                                                 │
│      │                                                      │
│      ▼                                                      │
│  文档 999 存在吗？                                           │
│      │                                                      │
│      ├── 是 → 执行 doc 部分更新                               │
│      │                                                      │
│      └── 否 → 执行 upsert，创建新文档                         │
│                                                              │
│  适用场景：从数据库同步数据时，不确定文档是否已存在              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 五、删除文档

### 5.1 根据 ID 删除

```bash
curl -X DELETE "http://localhost:9200/posts/_doc/1"
```

### 5.2 根据查询条件删除

```bash
# 删除所有 author 为 "test" 的文档
curl -X POST "http://localhost:9200/posts/_delete_by_query" -H 'Content-Type: application/json' -d '{
  "query": {
    "term": {
      "author": "test"
    }
  }
}'
```

### 5.3 删除索引（慎用！）

```bash
# 删除整个索引及其所有文档
curl -X DELETE "http://localhost:9200/posts"
```

```
┌──────────────────────────────────────────────────────────────┐
│                ES 的删除是"软删除"                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  删除文档后，文档不会立即从磁盘上消失！                         │
│                                                              │
│  Lucene 的段（Segment）是不可变的：                           │
│  - 新文档写入新段                                             │
│  - 删除操作只是标记文档为"已删除"                              │
│  - 在段合并（Segment Merge）时，才真正物理删除                 │
│                                                              │
│  删除流程：                                                    │
│  1. 用户发送 DELETE 请求                                      │
│  2. ES 在内存中标记文档为 deleted                             │
│  3. 下一次 refresh 时，标记写入新段                           │
│  4. 段合并时，真正删除已标记的文档                             │
│                                                              │
│  这意味着：刚删除的文档，在段合并前仍占用磁盘空间               │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 六、批量操作（_bulk API）

_bulk API 是 Elasticsearch 中**最高效的批量操作方式**，可以在一次请求中执行多个创建、更新、删除操作。

### 6.1 _bulk 格式

```
┌──────────────────────────────────────────────────────────────┐
│                _bulk API 格式                                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  格式：每两行为一组                                           │
│  第 1 行：操作指令（action + metadata）                       │
│  第 2 行：文档内容（某些操作不需要）                           │
│                                                              │
│  支持的操作：                                                 │
│  index   → 创建文档（ID 存在则覆盖）                           │
│  create  → 创建文档（ID 存在则报错）                           │
│  update  → 更新文档                                           │
│  delete  → 删除文档（不需要文档内容行）                         │
│                                                              │
│  注意：每行末尾必须有换行符 \n                                │
│  最后一个操作后面也要有换行符                                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 6.2 批量创建文档

```bash
curl -X POST "http://localhost:9200/posts/_bulk?pretty" -H 'Content-Type: application/x-ndjson' -d '
{"index": {"_id": "1"}}
{"title": "Kafka 入门教程", "content": "Apache Kafka 是一个分布式流处理平台，广泛用于消息队列和流式数据处理场景", "summary": "Kafka 核心概念详解", "tags": ["kafka", "大数据", "消息队列"], "category": "大数据", "author": "browncutie", "created_at": "2026-04-15 10:30:00", "views": 520, "likes": 45, "is_published": true, "is_top": true, "comments": 12, "url": "/posts/kafka-intro"}
{"index": {"_id": "2"}}
{"title": "Spark 快速入门", "content": "Apache Spark 是一个快速的大数据处理引擎，支持批处理和流处理", "summary": "Spark 核心概念和快速上手指南", "tags": ["spark", "大数据", "分布式计算"], "category": "大数据", "author": "browncutie", "created_at": "2026-04-10 09:00:00", "views": 380, "likes": 32, "is_published": true, "is_top": false, "comments": 8, "url": "/posts/spark-intro"}
{"index": {"_id": "3"}}
{"title": "HDFS 分布式文件系统详解", "content": "HDFS 是 Hadoop 的核心组件之一，提供高可靠的分布式文件存储", "summary": "深入理解 HDFS 的架构和原理", "tags": ["hadoop", "hdfs", "大数据"], "category": "大数据", "author": "browncutie", "created_at": "2026-04-05 15:00:00", "views": 290, "likes": 25, "is_published": true, "is_top": false, "comments": 6, "url": "/posts/hdfs-guide"}
{"index": {"_id": "4"}}
{"title": "Hive 数仓实践指南", "content": "Hive 是基于 Hadoop 的数据仓库工具，可以将 SQL 转化为 MapReduce 任务执行", "summary": "Hive 数仓建设的最佳实践", "tags": ["hive", "数仓", "大数据"], "category": "大数据", "author": "browncutie", "created_at": "2026-03-28 11:00:00", "views": 210, "likes": 18, "is_published": true, "is_top": false, "comments": 4, "url": "/posts/hive-guide"}
{"index": {"_id": "5"}}
{"title": "Flink 实时计算入门", "content": "Apache Flink 是一个分布式流处理框架，支持有状态的计算和精确一次语义", "summary": "Flink 流处理核心概念", "tags": ["flink", "实时计算", "大数据"], "category": "大数据", "author": "browncutie", "created_at": "2026-03-20 14:30:00", "views": 175, "likes": 15, "is_published": true, "is_top": false, "comments": 3, "url": "/posts/flink-intro"}
{"index": {"_id": "6"}}
{"title": "Elasticsearch 全文搜索实战", "content": "Elasticsearch 是一个分布式全文搜索和分析引擎，基于 Lucene 构建", "summary": "ES 搜索引擎的核心概念和实战操作", "tags": ["elasticsearch", "搜索", "大数据"], "category": "大数据", "author": "browncutie", "created_at": "2026-04-01 16:00:00", "views": 320, "likes": 28, "is_published": true, "is_top": false, "comments": 7, "url": "/posts/es-guide"}
{"index": {"_id": "7"}}
{"title": "DolphinScheduler 任务调度实践", "content": "DolphinScheduler 是一个分布式的工作流调度平台，用于编排大数据任务", "summary": "DolphinScheduler 工作流编排指南", "tags": ["调度", "大数据", "工作流"], "category": "运维", "author": "browncutie", "created_at": "2026-03-15 10:00:00", "views": 150, "likes": 12, "is_published": true, "is_top": false, "comments": 2, "url": "/posts/ds-guide"}
{"index": {"_id": "8"}}
{"title": "MinIO 对象存储入门", "content": "MinIO 是一个高性能的对象存储服务，兼容 Amazon S3 API", "summary": "MinIO 的安装和使用指南", "tags": ["minio", "对象存储", "S3"], "category": "存储", "author": "browncutie", "created_at": "2026-03-10 09:30:00", "views": 130, "likes": 10, "is_published": true, "is_top": false, "comments": 2, "url": "/posts/minio-intro"}
'
```

### 6.3 批量操作的不同类型混合

```bash
curl -X POST "http://localhost:9200/posts/_bulk?pretty" -H 'Content-Type: application/x-ndjson' -d '
{"index": {"_id": "9"}}
{"title": "Iceberg 数据湖入门", "content": "Apache Iceberg 是一个开源的表格格式，用于大型数据分析数据集", "tags": ["iceberg", "数据湖"], "category": "大数据", "author": "browncutie", "created_at": "2026-03-05 08:00:00", "views": 100, "likes": 8, "is_published": true, "is_top": false, "comments": 1, "url": "/posts/iceberg-intro"}
{"update": {"_id": "1"}}
{"doc": {"views": 521}}
{"delete": {"_id": "9"}}
'
```

### 6.4 _bulk 的性能建议

```
┌──────────────────────────────────────────────────────────────┐
│                _bulk 性能优化建议                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 批次大小                                                   │
│     - 单次 _bulk 请求建议 5-15MB                              │
│     - 大约 1000-5000 条文档/批次                              │
│     - 太小：网络开销大                                        │
│     - 太大：内存压力大，错误恢复成本高                          │
│                                                              │
│  2. 并发控制                                                   │
│     - 多线程并发发送 _bulk 请求                                │
│     - 建议并发数不超过集群数据节点数的 2 倍                     │
│     - 使用官方 elasticsearch-py 的 helpers.bulk()              │
│                                                              │
│  3. 写入优化（大批量导入时）                                    │
│     - 临时增大 refresh_interval（如 30s）                     │
│     - 临时设置 number_of_replicas: 0                          │
│     - 导入完成后恢复设置                                       │
│                                                              │
│  4. 错误处理                                                   │
│     - _bulk 不会因为单条失败而回滚整个批次                     │
│     - 检查返回的 errors 字段，逐条重试失败的文档                │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 七、版本控制（乐观并发）

### 7.1 _version 机制

每次文档被修改，`_version` 会自增。Elasticsearch 使用这个机制来实现**乐观并发控制（Optimistic Concurrency Control）**。

```
┌──────────────────────────────────────────────────────────────┐
│                乐观并发控制流程                                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  线程 A：读取文档（_version = 3）                             │
│  线程 B：读取文档（_version = 3）                             │
│      │                                                      │
│      ▼                                                      │
│  线程 A：更新文档，带上 ?if_seq_no=0&if_primary_term=3        │
│      │                                                      │
│      ├── 成功（版本匹配）→ _version 变为 4                    │
│      │                                                      │
│      ▼                                                      │
│  线程 B：更新文档，带上 ?if_seq_no=0&if_primary_term=3        │
│      │                                                      │
│      └── 失败（版本不匹配，已经是 4 了）                      │
│         → 返回 409 Conflict                                  │
│         → 线程 B 需要重新读取最新版本再更新                    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 7.2 使用 seq_no 和 primary_term

ES 7.x+ 推荐使用 `seq_no` + `primary_term` 代替 `_version`：

```bash
# 读取文档，记下 seq_no 和 primary_term
curl http://localhost:9200/posts/_doc/1?pretty
# 返回："_seq_no": 5, "_primary_term": 1

# 带版本条件更新
curl -X PUT "http://localhost:9200/posts/_doc/1?if_seq_no=5&if_primary_term=1" \
  -H 'Content-Type: application/json' -d '{
  "title": "Kafka 入门教程",
  "content": "更新后的内容",
  "views": 522,
  "author": "browncutie"
}'

# 如果 seq_no 或 primary_term 不匹配，返回 409 Conflict
```

### 7.3 使用外部版本号

如果你的数据源有自己的版本号（如数据库的 version 字段），可以使用外部版本：

```bash
curl -X PUT "http://localhost:9200/posts/_doc/1?version=10&version_type=external" \
  -H 'Content-Type: application/json' -d '{
  "title": "从数据库同步的文章",
  "views": 100,
  "author": "browncutie"
}'

# version_type=external 时：
# - ES 的版本号必须 <= 提供的 version
# - 如果 ES 当前版本是 8，你传 10 → 成功，ES 版本变为 10
# - 如果 ES 当前版本是 12，你传 10 → 失败（409 Conflict）
```

## 八、实战：批量导入博客文章

下面是一个完整的 Python 脚本，从数据源批量导入博客文章到 Elasticsearch。

```python
"""
批量导入博客文章到 Elasticsearch
"""
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime

# 连接 ES
es = Elasticsearch("http://localhost:9200")

# 模拟博客文章数据
posts = [
    {
        "title": "Kafka 高级特性详解",
        "content": "深入讲解 Kafka 的事务机制、幂等生产者、精确一次语义等高级特性",
        "summary": "Kafka 高级特性全面解析",
        "tags": ["kafka", "大数据", "高级"],
        "category": "大数据",
        "author": "browncutie",
        "created_at": "2026-04-12 20:00:00",
        "views": 450,
        "likes": 40,
        "is_published": True,
        "is_top": False,
        "comments": 10,
        "url": "/posts/kafka-advanced"
    },
    {
        "title": "Spark SQL 性能优化实战",
        "content": "通过实际案例讲解 Spark SQL 的性能优化技巧，包括分区策略、缓存、广播变量等",
        "summary": "Spark SQL 优化实战技巧",
        "tags": ["spark", "大数据", "性能优化"],
        "category": "大数据",
        "author": "browncutie",
        "created_at": "2026-04-08 11:30:00",
        "views": 310,
        "likes": 27,
        "is_published": True,
        "is_top": False,
        "comments": 5,
        "url": "/posts/spark-sql-optimize"
    },
    {
        "title": "大数据技术栈选型指南",
        "content": "从存储、计算、消息队列、搜索等维度，分析主流大数据技术组件的选型思路",
        "summary": "如何选择合适的大数据技术栈",
        "tags": ["大数据", "技术选型", "架构"],
        "category": "架构",
        "author": "browncutie",
        "created_at": "2026-03-25 16:00:00",
        "views": 680,
        "likes": 55,
        "is_published": True,
        "is_top": True,
        "comments": 15,
        "url": "/posts/bigdata-selection"
    },
    {
        "title": "PySpark 数据处理实战",
        "content": "使用 PySpark 进行数据清洗、转换、分析的完整实战教程",
        "summary": "PySpark 实战教程",
        "tags": ["spark", "python", "大数据"],
        "category": "大数据",
        "author": "browncutie",
        "created_at": "2026-03-18 09:00:00",
        "views": 250,
        "likes": 22,
        "is_published": True,
        "is_top": False,
        "comments": 4,
        "url": "/posts/pyspark-guide"
    },
    {
        "title": "Hadoop 集群部署指南",
        "content": "详细讲解 Hadoop 集群的规划、安装、配置和运维",
        "summary": "Hadoop 集群从零到一",
        "tags": ["hadoop", "集群", "运维"],
        "category": "运维",
        "author": "browncutie",
        "created_at": "2026-03-12 14:00:00",
        "views": 190,
        "likes": 16,
        "is_published": True,
        "is_top": False,
        "comments": 3,
        "url": "/posts/hadoop-cluster"
    }
]

# 准备批量操作
actions = [
    {
        "_index": "posts",
        "_id": str(10 + i),  # ID 从 10 开始，避免与前面的冲突
        "_source": post
    }
    for i, post in enumerate(posts)
]

# 执行批量写入
success, failed = bulk(es, actions, raise_on_error=False)
print(f"成功写入: {success} 条")
print(f"失败: {failed} 条")

# 验证
count = es.count(index="posts")["count"]
print(f"posts 索引中共有 {count} 篇文章")

es.close()
```

## 九、面试题

### 面试题 1：Elasticsearch 的 _bulk API 为什么比逐条写入快？

**参考答案：**

_bulk API 的性能优势来自以下几个方面：

1. **减少网络开销**：逐条写入每次请求都需要一次 HTTP 往返，_bulk 将多条操作合并为一次请求
2. **减少 Lucene 段刷新次数**：多次写入可能触发多次 refresh 操作，_bulk 批量写入减少 refresh 次数
3. **减少事务日志刷盘次数**：ES 的 translog 在每次操作后可能刷盘，批量操作减少刷盘频率
4. **管道化处理**：ES 可以管道化处理 _bulk 请求中的操作，提高吞吐量

实际使用中，_bulk 比逐条写入快 **5-10 倍**是常见的。

---

### 面试题 2：Elasticsearch 如何实现乐观并发控制？

**参考答案：**

ES 使用 **seq_no + primary_term** 机制实现乐观并发控制：

1. 每个文档有一个 `_seq_no`（序列号）和 `_primary_term`（主项），这两个值组合起来唯一标识文档的一个版本
2. 更新时带上 `if_seq_no` 和 `if_primary_term` 参数
3. ES 会检查当前文档的 seq_no 和 primary_term 是否与请求中的一致
4. 如果一致 → 更新成功，生成新的 seq_no
5. 如果不一致 → 返回 409 Conflict，客户端需要重新读取最新版本再更新

这个机制类似于数据库的乐观锁，适用于读多写少的场景。如果并发冲突频繁，需要考虑其他策略（如使用外部版本号、队列串行化等）。

---

### 面试题 3：删除文档后磁盘空间为什么不会立即释放？

**参考答案：**

因为 Lucene 的**段（Segment）是不可变的**：

1. Lucene 将索引数据存储在段中，段一旦创建就不可修改
2. 删除操作只是在内存中标记文档为 "deleted"，不会立即物理删除
3. 这些标记在 refresh 时写入新段
4. 只有在**段合并（Segment Merge）**时，已标记删除的文档才会被真正物理删除

这是 **写时复制（Copy-on-Write）** 的设计思路，好处是：
- 读取不需要加锁（段不可变）
- 多线程并发安全

如果磁盘空间紧张，可以手动触发段合并（`_forcemerge`），但这会消耗大量 I/O，建议在低峰期执行。

---

## 十、总结

```
┌──────────────────────────────────────────────────────────────┐
│                    本章要点回顾                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 文档是 ES 中最小的数据单元，以 JSON 格式存储               │
│  2. PUT 指定 ID，POST 自动生成 ID                              │
│  3. PUT 全量更新，_update 部分更新，upsert 更新或插入          │
│  4. _bulk 是批量操作的首选方式，比逐条操作快 5-10 倍           │
│  5. 删除是"软删除"，段合并时才物理删除                         │
│  6. 乐观并发控制用 seq_no + primary_term                      │
│  7. _bulk 建议每批 1000-5000 条文档                            │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[03-索引与映射.md](03-索引与映射.md) — 索引与映射**
>
> **下一篇：[05-搜索与Query-DSL.md](05-搜索与Query-DSL.md) — 搜索与 Query DSL**
