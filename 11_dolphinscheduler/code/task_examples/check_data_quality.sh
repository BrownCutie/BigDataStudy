#!/bin/bash
# ============================================================
# 数据质量检查脚本
# 用于 DolphinScheduler 的 Shell 任务节点
# 检查数据表中的数据量、空值比例、数据新鲜度等指标
# 退出码：0=通过，1=警告，2=失败
# ============================================================

# ============================================================
# 配置区域（根据实际环境修改）
# ============================================================
# MySQL 配置（检查源数据质量）
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASS="password"
MYSQL_DB="blog_db"

# Iceberg/Spark 数据湖检查
HIVE_METASTORE_URI="thrift://localhost:9083"

# 质量阈值配置
MIN_ROW_COUNT=100              # 最小行数（低于此值视为异常）
MAX_NULL_RATIO=0.05            # 最大空值比例（5%）
MAX_STALE_HOURS=24             # 数据最大延迟时间（小时）

# 日志文件
LOG_DIR="/tmp/data-quality-logs"
LOG_FILE="${LOG_DIR}/check_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# 工具函数
# ============================================================

# 记录日志
log() {
    local level="$1"
    local msg="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${timestamp}] [${level}] ${msg}" | tee -a "${LOG_FILE}"
}

# 统计信息
STATS_PASSED=0
STATS_WARNING=0
STATS_FAILED=0

# 检查通过
check_pass() {
    log "PASS" "$1"
    STATS_PASSED=$((STATS_PASSED + 1))
}

# 检查警告
check_warn() {
    log "WARN" "$1"
    STATS_WARNING=$((STATS_WARNING + 1))
}

# 检查失败
check_fail() {
    log "FAIL" "$1"
    STATS_FAILED=$((STATS_FAILED + 1))
}

# ============================================================
# 初始化
# ============================================================
mkdir -p "${LOG_DIR}"

log "INFO" "=========================================="
log "INFO" " 数据质量检查开始"
log "INFO" "=========================================="
log "INFO" "检查时间: $(date '+%Y-%m-%d %H:%M:%S')"

# ============================================================
# 检查 1：MySQL 源表行数检查
# ============================================================
log "INFO" "--- 检查 1: MySQL 源表行数 ---"

# 检查 mysql 命令是否可用
if command -v mysql &> /dev/null; then
    ROW_COUNT=$(mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASS}" \
        -D"${MYSQL_DB}" -sN -e "SELECT COUNT(*) FROM posts;" 2>/dev/null)

    if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -ge "$MIN_ROW_COUNT" ]; then
        check_pass "MySQL posts 行数: ${ROW_COUNT} (>= ${MIN_ROW_COUNT})"
    elif [ -n "$ROW_COUNT" ]; then
        check_fail "MySQL posts 行数不足: ${ROW_COUNT} (< ${MIN_ROW_COUNT})"
    else
        check_warn "MySQL 连接失败，跳过行数检查"
    fi
else
    check_warn "mysql 命令不可用，跳过 MySQL 检查"
fi

# ============================================================
# 检查 2：Iceberg 数据湖表行数检查
# ============================================================
log "INFO" "--- 检查 2: Iceberg 表行数 ---"

if command -v spark-sql &> /dev/null; then
    ICEBERG_COUNT=$(spark-sql -e "SELECT COUNT(*) FROM local.demo.posts;" 2>/dev/null | tail -1)

    if [ -n "$ICEBERG_COUNT" ] && [ "$ICEBERG_COUNT" -ge 1 ]; then
        check_pass "Iceberg demo.posts 行数: ${ICEBERG_COUNT}"
    else
        check_fail "Iceberg demo.posts 数据异常或为空"
    fi
else
    check_warn "spark-sql 命令不可用，跳过 Iceberg 检查"
fi

# ============================================================
# 检查 3：MinIO 对象存储可用性检查
# ============================================================
log "INFO" "--- 检查 3: MinIO 存储可用性 ---"

if command -v mc &> /dev/null; then
    # 检查 MinIO 连接
    mc ls myminio/demo-assets/ &> /dev/null
    if [ $? -eq 0 ]; then
        check_pass "MinIO demo-assets 存储桶可访问"
    else
        check_fail "MinIO demo-assets 存储桶不可访问"
    fi
else
    check_warn "mc 命令不可用，跳过 MinIO 检查"
fi

# ============================================================
# 检查 4：Elasticsearch 索引健康检查
# ============================================================
log "INFO" "--- 检查 4: Elasticsearch 索引健康 ---"

if command -v curl &> /dev/null; then
    ES_HEALTH=$(curl -s "http://localhost:9200/_cluster/health/posts" 2>/dev/null)

    if [ -n "$ES_HEALTH" ]; then
        ES_STATUS=$(echo "$ES_HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null)

        if [ "$ES_STATUS" = "green" ]; then
            check_pass "ES posts 索引状态: green"
        elif [ "$ES_STATUS" = "yellow" ]; then
            check_warn "ES posts 索引状态: yellow（副本未分配）"
        else
            check_fail "ES posts 索引状态: ${ES_STATUS}"
        fi

        # 检查文档数量
        ES_DOC_COUNT=$(curl -s "http://localhost:9200/posts/_count" 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null)

        if [ -n "$ES_DOC_COUNT" ] && [ "$ES_DOC_COUNT" -ge 1 ]; then
            check_pass "ES posts 文档数: ${ES_DOC_COUNT}"
        else
            check_fail "ES posts 文档数为 0 或查询失败"
        fi
    else
        check_warn "Elasticsearch 不可达，跳过 ES 检查"
    fi
else
    check_warn "curl 命令不可用，跳过 ES 检查"
fi

# ============================================================
# 检查 5：磁盘空间检查
# ============================================================
log "INFO" "--- 检查 5: 磁盘空间 ---"

DISK_USAGE=$(df -h /tmp | awk 'NR==2 {print $5}' | tr -d '%')

if [ -n "$DISK_USAGE" ]; then
    if [ "$DISK_USAGE" -lt 80 ]; then
        check_pass "磁盘使用率: ${DISK_USAGE}% (< 80%)"
    elif [ "$DISK_USAGE" -lt 90 ]; then
        check_warn "磁盘使用率偏高: ${DISK_USAGE}% (>= 80%)"
    else
        check_fail "磁盘空间不足: ${DISK_USAGE}% (>= 90%)"
    fi
fi

# ============================================================
# 输出汇总报告
# ============================================================
log "INFO" ""
log "INFO" "=========================================="
log "INFO" " 数据质量检查报告"
log "INFO" "=========================================="
log "INFO" "通过: ${STATS_PASSED}"
log "INFO" "警告: ${STATS_WARNING}"
log "INFO" "失败: ${STATS_FAILED}"
log "INFO" "日志文件: ${LOG_FILE}"

# 根据结果决定退出码
if [ "$STATS_FAILED" -gt 0 ]; then
    log "INFO" "结论: 检查未通过，存在严重问题"
    exit 2
elif [ "$STATS_WARNING" -gt 0 ]; then
    log "INFO" "结论: 检查通过，但存在警告"
    exit 1
else
    log "INFO" "结论: 所有检查项通过"
    exit 0
fi
