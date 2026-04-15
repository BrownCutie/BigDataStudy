# 05 - 搜索与 Query DSL：强大的搜索能力

## 一、搜索的基础概念

在 Elasticsearch 中，搜索是最核心的功能。ES 提供了两种搜索方式：

```
┌──────────────────────────────────────────────────────────────┐
│              ES 的两种搜索方式                                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. URI Search（URL 参数搜索）                                │
│     - 简单查询直接写在 URL 里                                 │
│     - 适合快速调试                                            │
│     - 语法有限，复杂查询写不了                                │
│     例：GET /posts/_search?q=title:Kafka                │
│                                                              │
│  2. Request Body Search（请求体搜索）                         │
│     - 查询条件写在 JSON 请求体中                              │
│     - 功能强大，支持所有查询类型                               │
│     - 生产环境推荐使用                                        │
│     例：POST /posts/_search { "query": { ... } }        │
│                                                              │
│  本教程重点讲解 Request Body Search                            │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 二、URI Search 快速入门

```bash
# 全文搜索
curl -X GET "http://localhost:9200/posts/_search?q=Kafka"

# 指定字段搜索
curl -X GET "http://localhost:9200/posts/_search?q=title:Kafka"

# 带分析器的搜索
curl -X GET "http://localhost:9200/posts/_search?q=title:大数据%20入门"

# 组合查询
curl -X GET "http://localhost:9200/posts/_search?q=+title:Kafka%20+author:browncutie"

# 分页
curl -X GET "http://localhost:9200/posts/_search?q=Kafka&from=0&size=5"
```

> URI Search 简单但不灵活，复杂查询请使用 Request Body Search。

## 三、Request Body Search 详解

### 3.1 match_all — 查询所有文档

```bash
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match_all": {}
  }
}'
```

### 3.2 match — 全文搜索（最常用）

`match` 查询会对搜索关键词进行分词，然后在倒排索引中查找匹配的文档。

```bash
# 搜索单个字段
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "title": "Kafka 入门"
    }
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│              match 查询的执行过程                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  搜索："Kafka 入门"                                          │
│      │                                                      │
│      ▼                                                      │
│  分词（使用 search_analyzer）：["kafka", "入门"]              │
│      │                                                      │
│      ▼                                                      │
│  查倒排索引：                                                 │
│  "kafka" → [文档 1, 文档 10]                                 │
│  "入门"   → [文档 1, 文档 2, 文档 5]                         │
│      │                                                      │
│      ▼                                                      │
│  计算相关性得分（BM25）：                                      │
│  - 文档 1：同时包含两个词 → 得分最高                         │
│  - 文档 2：只包含"入门" → 得分较低                           │
│  - 文档 10：只包含 "kafka" → 得分较低                       │
│      │                                                      │
│      ▼                                                      │
│  按得分排序返回结果                                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**match 查询的可选参数：**

```bash
# operator: and（所有词都必须匹配）
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "title": {
        "query": "Kafka 入门",
        "operator": "and"
      }
    }
  }
}'

# minimum_should_match: 至少匹配几个词
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "title": {
        "query": "Kafka 入门教程",
        "minimum_should_match": 2
      }
    }
  }
}'
```

### 3.3 match_phrase — 短语匹配

要求关键词按顺序连续出现：

```bash
# 搜索短语 "分布式流处理"
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match_phrase": {
      "content": "分布式流处理"
    }
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│            match vs match_phrase 的区别                        │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  文档内容："分布式流处理平台"                                  │
│                                                              │
│  match "流处理 平台"：                                       │
│    分词 → ["流处理", "平台"]                                  │
│    文档包含两个词 → 匹配 ✓                                    │
│                                                              │
│  match_phrase "流处理 平台"：                                 │
│    要求"流处理"和"平台"连续出现，且顺序正确                    │
│    文档中 "流处理" 后面确实是 "平台" → 匹配 ✓                 │
│                                                              │
│  match_phrase "平台 流处理"：                                 │
│    要求"平台"在"流处理"前面                                   │
│    文档中顺序不对 → 不匹配 ✗                                  │
│                                                              │
│  match_phrase 还支持 slop 参数（允许中间插入词）：              │
│  "流处理 slop:1 平台" → 允许中间有 1 个词                     │
│  "分布式流处理平台" 中 "流处理" 和 "平台" 之间有 0 个词         │
│  "流处理大数据平台" 中 "流处理" 和 "平台" 之间有 1 个词        │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 3.4 multi_match — 多字段搜索

同时搜索多个字段：

```bash
# 搜索标题、内容、标签三个字段
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "multi_match": {
      "query": "大数据入门",
      "fields": ["title^3", "content", "tags^2"]
    }
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│            multi_match 的字段权重                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  "fields": ["title^3", "content", "tags^2"]                  │
│                                                              │
│  ^3 表示权重是 3 倍                                           │
│  ^2 表示权重是 2 倍                                           │
│  不带 ^ 表示权重是 1 倍（默认）                                │
│                                                              │
│  效果：                                                       │
│  "大数据" 在 title 中出现 → 得分 × 3                         │
│  "大数据" 在 tags 中出现   → 得分 × 2                         │
│  "大数据" 在 content 中出现 → 得分 × 1                        │
│                                                              │
│  合理的权重设置：标题 > 标签 > 内容                            │
│  因为标题最能代表文章主题                                      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**multi_match 的类型：**

| 类型 | 说明 |
|------|------|
| `best_fields`（默认） | 取得分最高的字段作为文档得分 |
| `most_fields` | 合并多个字段的得分 |
| `cross_fields` | 把多个字段当作一个大字段处理 |
| `phrase` | 在任意字段上进行短语匹配 |

### 3.5 term — 精确匹配

`term` 查询不经过分词，直接精确匹配词项：

```bash
# 精确匹配作者
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "term": {
      "author": "browncutie"
    }
  }
}'

# 精确匹配标签（注意 tags 是 keyword 类型）
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "term": {
      "tags": "kafka"
    }
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│           term 查询的常见坑！                                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  term 查询对 text 字段可能不按预期工作！                       │
│                                                              │
│  错误示例：                                                   │
│  假设 title 字段类型是 text                                   │
│  写入时 "Elasticsearch 入门" 被分词为                         │
│    ["elasticsearch", "入门"]                                  │
│                                                              │
│  term 查询 "Elasticsearch 入门"：                             │
│    → 查找的是完整的 "elasticsearch 入门" 这个词项              │
│    → 分词后不存在这个词项                                     │
│    → 查不到！✗                                               │
│                                                              │
│  正确做法：                                                   │
│  1. 精确匹配用 keyword 类型字段：term { "title.keyword": "E" }│
│  2. 全文搜索用 match 查询：match { "title": "Elasticsearch" }│
│                                                              │
│  记住：term 查的是词项（Term），不是原始文本！                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 3.6 terms — 多值精确匹配

类似 SQL 的 `IN`：

```bash
# 搜索多个标签
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "terms": {
      "tags": ["kafka", "spark", "flink"]
    }
  }
}'
```

### 3.7 range — 范围查询

```bash
# 浏览量大于 200 的文章
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "range": {
      "views": {
        "gte": 200,
        "lte": 500
      }
    }
  }
}'

# 日期范围查询
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "range": {
      "created_at": {
        "gte": "2026-04-01",
        "lte": "2026-04-15"
      }
    }
  }
}'
```

范围操作符：

| 操作符 | 含义 |
|--------|------|
| `gt` | 大于（greater than） |
| `gte` | 大于等于（greater than or equal） |
| `lt` | 小于（less than） |
| `lte` | 小于等于（less than or equal） |

### 3.8 bool — 组合查询（核心！）

`bool` 查询是 ES 中最常用的查询类型，用于组合多个查询条件：

```
┌──────────────────────────────────────────────────────────────┐
│              bool 查询的四个子句                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  must         → 必须匹配（AND），影响相关性得分          │ │
│  │  should       → 应该匹配（OR），影响相关性得分           │ │
│  │  must_not     → 不能匹配（NOT），不参与评分               │ │
│  │  filter       → 必须匹配（AND），不参与评分（性能更好）   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  SQL 类比：                                                   │
│  must    → WHERE condition AND condition                     │
│  should  → WHERE condition OR condition                      │
│  must_not→ WHERE NOT condition                               │
│  filter  → WHERE condition（不计算得分，更快）                │
│                                                              │
│  must vs filter 的区别：                                      │
│  - must：匹配的文档参与相关性评分计算                         │
│  - filter：匹配的文档只做是/否判断，不计算得分                 │
│  - filter 性能更好（结果可以缓存）                            │
│  - 不需要评分的条件用 filter                                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**实战示例：**

```bash
# 搜索标题或内容包含 "大数据" 的已发布文章，
# 浏览量大于 100，排除 author 为 "test" 的
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "大数据",
            "fields": ["title^3", "content", "tags^2"]
          }
        }
      ],
      "filter": [
        { "term": { "is_published": true } },
        { "range": { "views": { "gte": 100 } } }
      ],
      "must_not": [
        { "term": { "author": "test" } }
      ]
    }
  }
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│              上面的查询执行流程                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. must: 标题或内容包含 "大数据"                             │
│     → 找到候选文档集，计算相关性得分                           │
│                                                              │
│  2. filter: is_published = true AND views >= 100             │
│     → 从候选集中过滤，只保留满足条件的                        │
│     → filter 结果可以缓存，下次查询更快                       │
│                                                              │
│  3. must_not: author != "test"                               │
│     → 从候选集中排除 author 为 "test" 的文档                  │
│                                                              │
│  4. 合并结果，按得分排序返回                                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 3.9 wildcard、prefix、regexp

```bash
# wildcard 通配符查询（* 匹配任意字符，? 匹配单个字符）
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "wildcard": {
      "title.keyword": "Kafka*"
    }
  }
}'

# prefix 前缀查询
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "prefix": {
      "title": "Spark"
    }
  }
}'

# regexp 正则查询
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "regexp": {
      "title.keyword": "Kafka.*教程"
    }
  }
}'
```

> **注意：** wildcard、prefix、regexp 查询性能较差（需要遍历词典），尽量避免在生产环境使用。

## 四、排序与分页

### 4.1 排序

```bash
# 按浏览量降序
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "sort": [
    { "views": { "order": "desc" } }
  ]
}'

# 多字段排序：先按是否置顶降序，再按浏览量降序
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "sort": [
    { "is_top": { "order": "desc" } },
    { "views": { "order": "desc" } },
    { "created_at": { "order": "desc" } }
  ]
}'

# 按相关性得分排序（默认行为）
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": { "content": "大数据" }
  },
  "sort": [
    "_score"
  ]
}'
```

### 4.2 分页

```bash
# from + size 分页（浅分页）
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "from": 0,     # 起始位置（偏移量）
  "size": 5      # 每页大小
}'

# 第 2 页
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "from": 5,
  "size": 5
}'
```

```
┌──────────────────────────────────────────────────────────────┐
│              深度分页问题（from + size 的限制）                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  from + size 的分页方式有个问题：                              │
│                                                              │
│  假设有 5 个分片，每页 10 条：                                 │
│  第 1 页（from=0, size=10）：每个分片返回 10 条 → 合并排序     │
│  第 100 页（from=990, size=10）：每个分片返回 1000 条 → 合并   │
│  第 10000 页（from=99990, size=10）：每个分片返回 100000 条    │
│                                                              │
│  from 越大，每个分片需要处理的数据越多，内存和 CPU 开销越大    │
│                                                              │
│  ES 默认限制 from + size <= 10000                             │
│  超过这个值会报错：                                            │
│  "Result window is too large, from + size must be less than  │
│   or equal to: [10000]"                                       │
│                                                              │
│  深度分页的解决方案：                                          │
│  1. search_after：基于排序值的游标分页（推荐）                 │
│  2. scroll：快照分页（适合全量导出，不适合实时查询）            │
│  3. 限制分页深度（产品层面限制最多翻 100 页）                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 4.3 search_after 游标分页

```bash
# 第一页：按 views 降序排序
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "size": 3,
  "sort": [
    { "views": "desc" },
    { "_id": "asc" }
  ]
}'

# 记下最后一条的排序值，假设是 views=310, _id=11
# 下一页：用 search_after
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} },
  "size": 3,
  "sort": [
    { "views": "desc" },
    { "_id": "asc" }
  ],
  "search_after": [310, "11"]
}'
```

## 五、高亮（Highlight）

搜索结果中高亮显示匹配的关键词：

```bash
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "multi_match": {
      "query": "大数据",
      "fields": ["title^3", "content"]
    }
  },
  "highlight": {
    "pre_tags": ["<em>"],
    "post_tags": ["</em>"],
    "fields": {
      "title": {},
      "content": {
        "fragment_size": 200,
        "number_of_fragments": 3
      }
    }
  }
}'
```

返回结果中会多出 `highlight` 字段：

```json
{
  "hits": {
    "hits": [
      {
        "_source": {
          "title": "大数据技术栈选型指南",
          "content": "从存储、计算、消息队列..."
        },
        "highlight": {
          "title": ["<em>大数据</em>技术栈选型指南"],
          "content": ["从存储、计算、消息队列等维度，分析主流<em>大数据</em>技术组件的选型思路"]
        }
      }
    ]
  }
}
```

```
┌──────────────────────────────────────────────────────────────┐
│              高亮配置参数                                      │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  pre_tags / post_tags                                        │
│    高亮标签，默认 <em>                                        │
│                                                              │
│  fragment_size                                               │
│    每个高亮片段的最大字符数，默认 100                          │
│    设为 0 表示返回整个字段                                    │
│                                                              │
│  number_of_fragments                                         │
│    返回几个高亮片段，默认 5                                   │
│    设为 0 表示只返回第一个匹配的片段                          │
│                                                              │
│  require_field_match                                         │
│    是否只在查询命中的字段上高亮，默认 true                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 六、实战：实现数据搜索功能

下面是一个完整的数据搜索功能，组合了多种查询类型：

```bash
# 数据搜索：关键词 + 分类过滤 + 排序 + 分页 + 高亮
curl -X GET "http://localhost:9200/posts/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "大数据",
            "fields": ["title^3", "summary^2", "content", "tags^2"],
            "type": "best_fields",
            "minimum_should_match": 1
          }
        }
      ],
      "filter": [
        { "term": { "is_published": true } }
      ]
    }
  },
  "sort": [
    { "is_top": { "order": "desc" } },
    "_score"
  ],
  "from": 0,
  "size": 10,
  "highlight": {
    "pre_tags": ["<strong>"],
    "post_tags": ["</strong>"],
    "fields": {
      "title": { "fragment_size": 0 },
      "summary": { "fragment_size": 200, "number_of_fragments": 1 }
    }
  },
  "_source": ["title", "summary", "tags", "category", "author", "created_at", "views", "url"]
}'
```

**Python 实现的数据搜索 API：**

```python
"""
数据搜索 API
"""
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

def search_posts(keyword, category=None, author=None, page=1, size=10):
    """
    搜索博客文章

    参数：
        keyword:  搜索关键词
        category: 分类过滤（可选）
        author:   作者过滤（可选）
        page:     页码（从 1 开始）
        size:     每页数量
    """
    # 构建 bool 查询
    must_clauses = []
    filter_clauses = [{"term": {"is_published": True}}]

    # 关键词搜索
    if keyword:
        must_clauses.append({
            "multi_match": {
                "query": keyword,
                "fields": ["title^3", "summary^2", "content", "tags^2"],
                "type": "best_fields"
            }
        })

    # 分类过滤
    if category:
        filter_clauses.append({"term": {"category": category}})

    # 作者过滤
    if author:
        filter_clauses.append({"term": {"author": author}})

    # 组装查询体
    query_body = {"match_all": {}} if not must_clauses else {"bool": {"must": must_clauses}}

    # 执行搜索
    result = es.search(
        index="posts",
        query=query_body,
        post_filter={"bool": {"filter": filter_clauses}},
        sort=[{"is_top": {"order": "desc"}}, "_score"],
        from_=(page - 1) * size,
        size=size,
        highlight={
            "pre_tags": ["<strong>"],
            "post_tags": ["</strong>"],
            "fields": {
                "title": {"fragment_size": 0},
                "summary": {"fragment_size": 200, "number_of_fragments": 1}
            }
        },
        source=["title", "summary", "tags", "category", "author", "created_at", "views", "url"]
    )

    return {
        "total": result["hits"]["total"]["value"],
        "page": page,
        "size": size,
        "hits": [
            {
                **hit["_source"],
                "score": hit["_score"],
                "highlight": hit.get("highlight", {})
            }
            for hit in result["hits"]["hits"]
        ]
    }


# 使用示例
result = search_posts(keyword="大数据入门", category="大数据")
for post in result["hits"]:
    print(f"[{post['score']:.2f}] {post['title']}")
    print(f"  浏览量: {post['views']} | 标签: {', '.join(post['tags'])}")

es.close()
```

## 七、面试题

### 面试题 1：match 查询和 term 查询有什么区别？

**参考答案：**

**match 查询（全文搜索）：**
- 会对搜索关键词先进行分词（使用 search_analyzer）
- 然后在倒排索引中查找匹配的词项
- 支持相关性评分
- 适合 text 类型字段

**term 查询（精确匹配）：**
- 不会对搜索关键词进行分词，直接拿原始值去查
- 在倒排索引中精确查找词项
- 用于 keyword 类型字段
- 常见坑：对 text 字段用 term 查询可能匹配不到（因为 text 字段在写入时被分词了）

**简单记忆：**
- 模糊搜索用 `match`，精确查找用 `term`
- text 字段用 `match`，keyword 字段用 `term`

---

### 面试题 2：bool 查询中 must 和 filter 有什么区别？

**参考答案：**

两者都是"必须匹配"的语义，但有一个关键区别：**是否参与相关性评分**。

**must：**
- 匹配的文档会参与相关性评分计算
- 评分结果会被缓存，但每次查询仍需计算
- 适合需要影响排序的条件（如关键词匹配）

**filter：**
- 只做是/否判断，不参与评分计算
- filter 的结果会被缓存（Bitset 缓存），后续查询非常快
- 适合不需要影响排序的条件（如 is_published=true、views>100）

**性能建议：**
- 不需要评分的条件一律用 filter
- filter 结果被缓存后，重复查询几乎零开销
- bool 查询中尽量将不需要评分的条件放在 filter 中

---

### 面试题 3：Elasticsearch 如何解决深度分页问题？

**参考答案：**

from + size 的分页方式在深度分页时有严重性能问题：from=9990, size=10 意味着每个分片需要返回 10000 条数据，协调节点需要处理 50000 条数据（假设 5 个分片）。

**解决方案：**

1. **search_after（推荐）：** 基于排序值的游标分页。每次查询带上上一页最后一条的排序值，不需要计算偏移量。适合实时查询场景。

2. **scroll：** 创建一个搜索上下文快照，后续请求通过 scroll_id 获取下一批数据。适合全量导出数据，不适合实时查询（因为快照会占用资源）。

3. **产品层面限制：** 限制最大翻页数（如最多 100 页），引导用户使用更精确的搜索条件。

4. **使用数据聚合：** 如果只需要统计信息（如总数），使用 `track_total_hits: false` 减少精确计数开销。

---

## 八、总结

```
┌──────────────────────────────────────────────────────────────┐
│                    本章要点回顾                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. match 全文搜索，term 精确匹配（最基础的两个查询）          │
│  2. multi_match 多字段搜索，^N 设置字段权重                   │
│  3. match_phrase 短语匹配，要求词连续出现                     │
│  4. bool 组合查询：must/should/must_not/filter                │
│  5. 不需要评分的条件用 filter（性能更好，结果可缓存）          │
│  6. from + size 浅分页，search_after 深度分页                │
│  7. highlight 高亮显示匹配的关键词                            │
│  8. wildcard/prefix/regexp 性能差，尽量避免                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[04-文档CRUD操作.md](04-文档CRUD操作.md) — 文档 CRUD 操作**
>
> **下一篇：[06-聚合分析.md](06-聚合分析.md) — 聚合分析**
