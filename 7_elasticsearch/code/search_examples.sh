#!/bin/bash
# ============================================================
# Elasticsearch 搜索查询示例集
# 涵盖全文搜索、过滤、聚合、高亮等常用操作
# ============================================================

# ES 连接配置
ES_HOST="localhost"
ES_PORT="9200"
ES_URL="http://${ES_HOST}:${ES_PORT}"
INDEX_NAME="posts"

# 通用请求函数：发送 JSON 查询并格式化输出
search() {
    local desc="$1"
    local query="$2"

    echo "-------------------------------------------"
    echo " $desc"
    echo "-------------------------------------------"
    curl -s "${ES_URL}/${INDEX_NAME}/_search?pretty" \
        -H "Content-Type: application/json" \
        -d "$query"
    echo ""
    echo ""
}

echo "=========================================="
echo " Elasticsearch 搜索查询示例"
echo "=========================================="
echo ""

# ------------------------------------------
# 1. 全文搜索：在标题和内容中搜索关键词
# ------------------------------------------
search "1. 全文搜索 - 搜索包含 '大数据' 的文章" '{
  "query": {
    "multi_match": {
      "query": "大数据",
      "fields": ["title^3", "content"],
      "type": "best_fields"
    }
  },
  "_source": ["title", "author", "category", "publish_date"],
  "highlight": {
    "fields": {
      "title": {},
      "content": {"fragment_size": 100, "number_of_fragments": 1}
    }
  }
}'

# ------------------------------------------
# 2. 精确过滤：按分类和作者过滤
# ------------------------------------------
search "2. 精确过滤 - 分类为 '流计算' 的文章" '{
  "query": {
    "term": { "category": "流计算" }
  },
  "_source": ["title", "author", "category", "publish_date"],
  "sort": [{"publish_date": {"order": "desc"}}]
}'

# ------------------------------------------
# 3. 组合查询：全文搜索 + 多条件过滤
# ------------------------------------------
search "3. 组合查询 - 搜索 '性能' 关键词且分类为 'Spark'" '{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "content": "性能"
          }
        }
      ],
      "filter": [
        { "term": { "category": "Spark" } }
      ]
    }
  },
  "_source": ["title", "author", "category", "publish_date", "view_count"]
}'

# ------------------------------------------
# 4. 范围查询：按浏览量筛选热门文章
# ------------------------------------------
search "4. 范围查询 - 浏览量超过 10000 的热门文章" '{
  "query": {
    "range": {
      "view_count": { "gte": 10000 }
    }
  },
  "_source": ["title", "author", "view_count", "like_count"],
  "sort": [{"view_count": {"order": "desc"}}]
}'

# ------------------------------------------
# 5. Tag 过滤：包含特定标签的文章
# ------------------------------------------
search "5. Tag 过滤 - 标签包含 'iceberg' 的文章" '{
  "query": {
    "term": { "tags": "iceberg" }
  },
  "_source": ["title", "tags", "publish_date"]
}'

# ------------------------------------------
# 6. 分页查询
# ------------------------------------------
search "6. 分页查询 - 第 1 页，每页 5 条" '{
  "query": {
    "match_all": {}
  },
  "_source": ["title", "author", "publish_date"],
  "from": 0,
  "size": 5,
  "sort": [{"publish_date": {"order": "desc"}}]
}'

# ------------------------------------------
# 7. 聚合分析 - 按分类统计文章数量
# ------------------------------------------
search "7. 聚合分析 - 各分类的文章数量" '{
  "size": 0,
  "aggs": {
    "category_count": {
      "terms": { "field": "category", "size": 20 }
    }
  }
}'

# ------------------------------------------
# 8. 聚合分析 - 按作者统计总浏览量和平均点赞数
# ------------------------------------------
search "8. 聚合分析 - 各作者的统计数据" '{
  "size": 0,
  "aggs": {
    "by_author": {
      "terms": { "field": "author", "size": 10 },
      "aggs": {
        "total_views": { "sum": { "field": "view_count" } },
        "avg_likes": { "avg": { "field": "like_count" } },
        "post_count": { "value_count": { "field": "title" } }
      }
    }
  }
}'

# ------------------------------------------
# 9. 聚合分析 - 按月份统计文章发布趋势
# ------------------------------------------
search "9. 聚合分析 - 按月份的文章发布趋势" '{
  "size": 0,
  "aggs": {
    "publish_trend": {
      "date_histogram": {
        "field": "publish_date",
        "calendar_interval": "month",
        "format": "yyyy-MM"
      }
    }
  }
}'

# ------------------------------------------
# 10. 多字段高亮搜索
# ------------------------------------------
search "10. 高亮搜索 - 搜索 'Kafka' 并高亮匹配内容" '{
  "query": {
    "multi_match": {
      "query": "Kafka",
      "fields": ["title^2", "content", "tags"]
    }
  },
  "_source": ["title", "author", "publish_date"],
  "highlight": {
    "pre_tags": ["<em>"],
    "post_tags": ["</em>"],
    "fields": {
      "title": {},
      "content": {"fragment_size": 150, "number_of_fragments": 2}
    }
  }
}'

# ------------------------------------------
# 11. 短语搜索：精确短语匹配
# ------------------------------------------
search "11. 短语搜索 - 搜索精确短语 '数据湖'" '{
  "query": {
    "match_phrase": {
      "content": "数据湖"
    }
  },
  "_source": ["title", "content"],
  "highlight": {
    "fields": {
      "content": {"fragment_size": 100, "number_of_fragments": 1}
    }
  }
}'

# ------------------------------------------
# 12. 复合排序：按点赞数降序，同分时按浏览量降序
# ------------------------------------------
search "12. 复合排序 - 按点赞数和浏览量排序 Top 5" '{
  "query": { "match_all": {} },
  "_source": ["title", "author", "like_count", "view_count"],
  "sort": [
    { "like_count": { "order": "desc" } },
    { "view_count": { "order": "desc" } }
  ],
  "size": 5
}'

echo "=========================================="
echo " 查询示例执行完毕"
echo "=========================================="
