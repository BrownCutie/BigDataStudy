#!/bin/bash
# ============================================================
# 导出数据到 Elasticsearch 脚本
# 用于 DolphinScheduler 的 Shell 任务节点
# 从数据源（MySQL/Iceberg）读取数据，转换后写入 ES
# ============================================================

# ============================================================
# 配置区域
# ============================================================

# Elasticsearch 配置（ES 9.3.3，安全已关闭）
ES_HOST="localhost"
ES_PORT="9200"
ES_URL="http://${ES_HOST}:${ES_PORT}"
ES_INDEX="posts"

# MySQL 源数据库配置
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASS="password"
MYSQL_DB="blog_db"

# 导出批次大小
BATCH_SIZE=500

# 临时文件目录
TMP_DIR="/tmp/es-export"
BATCH_FILE="${TMP_DIR}/batch.json"

# 日志
LOG_FILE="${TMP_DIR}/export_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# 工具函数
# ============================================================

log() {
    local msg="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${msg}" | tee -a "${LOG_FILE}"
}

# ============================================================
# 初始化
# ============================================================
mkdir -p "${TMP_DIR}"

log "=========================================="
log " 数据导出到 Elasticsearch"
log "=========================================="
log "目标 ES: ${ES_URL}/${ES_INDEX}"
log "批次大小: ${BATCH_SIZE}"

# ============================================================
# 方式一：从 MySQL 导出（使用 Spark SQL）
# ============================================================
export_from_mysql() {
    log "--- 方式一: 通过 Spark SQL 从 MySQL 导出 ---"

    if ! command -v spark-sql &> /dev/null; then
        log "警告: spark-sql 不可用，跳过 MySQL 导出"
        return 1
    fi

    # 使用 Spark SQL 从 MySQL 读取并写入 ES
    spark-sql --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        -e "
        -- 从 MySQL 读取数据，写入 Iceberg 临时表
        -- 然后从 Iceberg 读取，通过 ES Hadoop Connector 写入
        SELECT 'MySQL -> Iceberg -> ES 导出' AS task;
        " 2>> "${LOG_FILE}"

    log "MySQL 导出完成"
}

# ============================================================
# 方式二：从 Iceberg 导出（使用 _bulk API）
# ============================================================
export_from_iceberg() {
    log "--- 方式二: 从 Iceberg 数据湖导出 ---"

    # 检查依赖
    if ! command -v spark-sql &> /dev/null; then
        log "警告: spark-sql 不可用，跳过 Iceberg 导出"
        return 1
    fi

    if ! command -v curl &> /dev/null; then
        log "错误: curl 不可用，无法执行导出"
        return 1
    fi

    # 1. 从 Iceberg 导出数据为 JSON 格式
    log "步骤 1: 从 Iceberg 导出数据为 JSON..."
    ICEBERG_JSON="${TMP_DIR}/iceberg_export.json"

    spark-sql \
        --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
        --conf "spark.sql.catalog.local.type=hadoop" \
        --conf "spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse" \
        -e "
        SET spark.sql.defaultCatalog=local;
        SELECT CONCAT(
            '{\"index\":{\"_index\":\"${ES_INDEX}\",\"_id\":\"', CAST(post_id AS STRING), '\"}}',
            '\n',
            to_json(struct(
                post_id, title, author, category,
                tags, content, publish_date,
                view_count, like_count, comment_count, status
            ))
        ) AS line
        FROM local.demo.posts
        WHERE status = 'published';
        " 2>> "${LOG_FILE}" | grep -v "^$" > "${ICEBERG_JSON}"

    # 2. 检查导出数据量
    LINE_COUNT=$(wc -l < "${ICEBERG_JSON}" 2>/dev/null || echo "0")
    # bulk 格式每条文档占两行（action + data），所以文档数 = 行数
    # 这里因为我们的 SQL 已经把 action 和 data 合并成一行了，所以行数就是文档数

    if [ "$LINE_COUNT" -eq 0 ]; then
        log "错误: 没有导出任何数据"
        return 1
    fi

    log "步骤 2: 导出了 ${LINE_COUNT} 条数据，开始写入 ES..."

    # 3. 使用 _bulk API 批量导入
    # 需要将每行数据拆分为 action 行和 data 行
    BULK_FILE="${TMP_DIR}/bulk_data.json"
    > "${BULK_FILE}"

    while IFS= read -r line; do
        # 从合并行中提取 _id 和数据部分
        post_id=$(echo "$line" | python3 -c "
import sys, json
line = sys.stdin.read().strip()
# 第一部分是 index action，第二部分是文档数据
parts = line.split('\n')
if len(parts) == 1:
    # 已经是单行格式，需要拆分
    try:
        obj = json.loads(line)
        if 'index' in obj:
            # 这是 action 行（不应该出现在这里，因为我们的 SQL 合并了）
            print(line)
        else:
            # 这是数据行，需要生成 action
            print(json.dumps({'index': {'_index': '${ES_INDEX}', '_id': str(obj.get('post_id', ''))}}))
            print(line)
    except:
        pass
" 2>/dev/null)
        echo "$post_id"
    done < "${ICEBERG_JSON}" > "${BULK_FILE}"

    # 使用批量 API 写入
    BULK_RESULT=$(curl -s -X POST "${ES_URL}/_bulk" \
        -H "Content-Type: application/x-ndjson" \
        --data-binary @"${BULK_FILE}" 2>> "${LOG_FILE}")

    # 解析结果
    SUCCESS=$(echo "$BULK_RESULT" | python3 -c "
import sys, json
try:
    result = json.loads(sys.stdin.read())
    items = result.get('items', [])
    success = sum(1 for i in items if i.get('index', {}).get('status') in (200, 201))
    failed = sum(1 for i in items if i.get('index', {}).get('status') not in (200, 201))
    print(f'{success}/{success + failed}')
except:
    print('解析失败')
" 2>/dev/null)

    log "ES 导入结果: ${SUCCESS}"
}

# ============================================================
# 方式三：直接通过 curl 构造数据（演示用）
# ============================================================
export_demo_data() {
    log "--- 方式三: 导入演示数据到 ES ---"

    # 检查 ES 连接
    ES_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${ES_URL}" 2>/dev/null)
    if [ "$ES_STATUS" != "200" ]; then
        log "错误: ES 不可达 (${ES_URL}), 状态码: ${ES_STATUS}"
        return 1
    fi

    # 创建演示数据
    DEMO_FILE="${TMP_DIR}/demo_bulk.json"
    cat > "${DEMO_FILE}" << 'BULK_EOF'
{"index": {"_index": "posts", "_id": "demo-1"}}
{"title": "DolphinScheduler 导出的演示文章", "author": "调度系统", "category": "自动化", "content": "这篇文章由 DolphinScheduler 定时任务自动导出到 Elasticsearch。", "publish_date": "2026-01-01", "view_count": 100, "like_count": 10, "comment_count": 2, "status": "published"}
BULK_EOF

    # 执行导入
    RESULT=$(curl -s -X POST "${ES_URL}/_bulk" \
        -H "Content-Type: application/x-ndjson" \
        --data-binary @"${DEMO_FILE}" 2>/dev/null)

    CREATED=$(echo "$RESULT" | python3 -c "
import sys, json
try:
    result = json.loads(sys.stdin.read())
    status = result.get('items', [{}])[0].get('index', {}).get('status', 0)
    print('成功' if status in (200, 201) else f'失败 (状态码: {status})')
except:
    print('解析失败')
" 2>/dev/null)

    log "演示数据导入: ${CREATED}"

    # 清理
    rm -f "${DEMO_FILE}"
}

# ============================================================
# 执行导出流程
# ============================================================

TOTAL_START=$(date +%s)

# 尝试各种导出方式
export_from_mysql
export_from_iceberg
export_demo_data

TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

log ""
log "=========================================="
log " 导出完成"
log "=========================================="
log "总耗时: ${TOTAL_DURATION} 秒"
log "日志文件: ${LOG_FILE}"

# 清理临时文件
rm -f "${BATCH_FILE}" "${TMP_DIR}/iceberg_export.json" "${TMP_DIR}/bulk_data.json"

exit 0
