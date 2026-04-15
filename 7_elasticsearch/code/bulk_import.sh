#!/bin/bash
# ============================================================
# Elasticsearch 批量导入博客文章数据
# 使用 _bulk API 将 posts.json 导入 ES 索引
# ============================================================

# ES 连接配置（ES 9.3.3，安全已关闭）
ES_HOST="localhost"
ES_PORT="9200"
ES_URL="http://${ES_HOST}:${ES_PORT}"

# 索引名称
INDEX_NAME="posts"

# 博客图片路径（相对于脚本所在目录）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_FILE="${SCRIPT_DIR}/data/posts.json"

echo "=========================================="
echo " Elasticsearch 批量导入脚本"
echo "=========================================="
echo "目标地址: ${ES_URL}"
echo "索引名称: ${INDEX_NAME}"
echo "博客图片: ${DATA_FILE}"
echo ""

# 1. 检查 ES 是否可达
echo "[1/5] 检查 Elasticsearch 连接..."
ES_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${ES_URL}")
if [ "$ES_STATUS" != "200" ]; then
    echo "错误: 无法连接到 Elasticsearch (${ES_URL}), HTTP 状态码: ${ES_STATUS}"
    echo "请确认 ES 服务已启动（端口 ${ES_PORT}）"
    exit 1
fi
echo "ES 连接正常，版本信息："
curl -s "${ES_URL}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  版本: {d[\"version\"][\"number\"]}')" 2>/dev/null || echo "  (无法解析版本信息)"
echo ""

# 2. 删除已有索引（如果存在）
echo "[2/5] 清理旧索引..."
curl -s -X DELETE "${ES_URL}/${INDEX_NAME}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  acknowledged: {d.get(\"acknowledged\", \"索引不存在\")}')" 2>/dev/null || echo "  (索引不存在或已删除)"
echo ""

# 3. 创建索引并设置 Mapping
echo "[3/5] 创建索引及 Mapping..."
curl -s -X PUT "${ES_URL}/${INDEX_NAME}" -H "Content-Type: application/json" -d '
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "ik_max_word_analyzer": {
          "type": "custom",
          "tokenizer": "ik_max_word"
        },
        "ik_smart_analyzer": {
          "type": "custom",
          "tokenizer": "ik_smart"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "ik_max_word_analyzer",
        "search_analyzer": "ik_smart_analyzer",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "author": {
        "type": "keyword"
      },
      "category": {
        "type": "keyword"
      },
      "tags": {
        "type": "keyword"
      },
      "content": {
        "type": "text",
        "analyzer": "ik_max_word_analyzer",
        "search_analyzer": "ik_smart_analyzer"
      },
      "publish_date": {
        "type": "date",
        "format": "yyyy-MM-dd"
      },
      "view_count": {
        "type": "integer"
      },
      "like_count": {
        "type": "integer"
      },
      "comment_count": {
        "type": "integer"
      }
    }
  }
}' | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  acknowledged: {d.get(\"acknowledged\", False)}')" 2>/dev/null
echo ""

# 4. 执行批量导入
echo "[4/5] 批量导入数据..."
if [ ! -f "$DATA_FILE" ]; then
    echo "错误: 博客图片不存在: ${DATA_FILE}"
    exit 1
fi

# 使用 _bulk API 导入，添加 Content-Type 头
BULK_RESULT=$(curl -s -X POST "${ES_URL}/_bulk" \
    -H "Content-Type: application/x-ndjson" \
    --data-binary @"${DATA_FILE}")

# 解析导入结果
echo "$BULK_RESULT" | python3 -c "
import sys, json
result = json.loads(sys.stdin.read())
if 'errors' in result:
    took = result.get('took', 'N/A')
    errors = result.get('errors', False)
    items = result.get('items', [])
    success = sum(1 for i in items if i.get('index', {}).get('status') in (200, 201))
    failed = sum(1 for i in items if i.get('index', {}).get('status') not in (200, 201))
    print(f'  耗时: {took}ms')
    print(f'  成功: {success} 条')
    print(f'  失败: {failed} 条')
    if failed > 0:
        for i in items:
            idx = i.get('index', {})
            if idx.get('status') not in (200, 201):
                print(f'  失败详情: id={idx.get(\"_id\")}, 错误={idx.get(\"error\", {}).get(\"type\")}')
else:
    print('  导入结果解析异常')
" 2>/dev/null || echo "  (原始响应: ${BULK_RESULT:0:200}...)"
echo ""

# 5. 验证导入结果
echo "[5/5] 验证导入结果..."
DOC_COUNT=$(curl -s "${ES_URL}/${INDEX_NAME}/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null)
echo "  索引文档总数: ${DOC_COUNT}"

echo ""
echo "=========================================="
echo " 导入完成！"
echo "=========================================="
echo "提示: 运行 search_examples.sh 查看搜索示例"
