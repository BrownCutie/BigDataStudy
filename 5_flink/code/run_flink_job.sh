#!/bin/bash
# ==============================================================================
# Flink 数据平台实时统计作业提交脚本
# ==============================================================================
# 功能：
#   1. 检查 Flink 和 Kafka 是否运行
#   2. 确保 blog-events Topic 已创建
#   3. 提交 Flink SQL 作业（博客事件实时统计）
#   4. 提交 Flink Java 作业（如果 JAR 包存在）
#
# 前置条件：
#   - Flink 2.2.0 Standalone 集群已启动
#   - Kafka 4.2.0 已启动（KRaft 模式，端口 9092）
#   - blog-events Topic 已创建
#
# 使用方法：
#   chmod +x run_flink_job.sh
#   ./run_flink_job.sh              # 提交 SQL 作业
#   ./run_flink_job.sh java         # 提交 Java 作业
#   ./run_flink_job.sh stop         # 停止所有数据平台统计作业
#   ./run_flink_job.sh status       # 查看作业状态
# ==============================================================================

set -e

# ==================== 配置 ====================
FLINK_HOME="${FLINK_HOME:-/opt/flink/2.2.0}"
KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="blog-events"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ==================== 检查 Flink 集群 ====================
check_flink() {
    echo -e "${YELLOW}检查 Flink 集群状态...${NC}"

    if ! lsof -i :8081 > /dev/null 2>&1; then
        echo -e "${RED}错误: Flink 未在端口 8081 运行！${NC}"
        echo "请先启动 Flink 集群："
        echo "  start-cluster.sh"
        return 1
    fi

    # 检查 Flink 版本
    local flink_version
    flink_version=$("${FLINK_HOME}/bin/flink" --version 2>&1 | head -1 || echo "未知版本")
    echo -e "${GREEN}Flink 集群运行中 (${flink_version}) ✓${NC}"
    return 0
}

# ==================== 检查 Kafka ====================
check_kafka() {
    echo -e "${YELLOW}检查 Kafka 状态...${NC}"

    if ! lsof -i :9092 > /dev/null 2>&1; then
        echo -e "${RED}错误: Kafka 未在端口 9092 运行！${NC}"
        echo "请先启动 Kafka："
        echo "  kafka-server-start.sh \$KAFKA_HOME/config/server.properties"
        return 1
    fi

    echo -e "${GREEN}Kafka 运行中 (端口 9092) ✓${NC}"
    return 0
}

# ==================== 检查 Topic ====================
check_topic() {
    echo -e "${YELLOW}检查 Topic: ${TOPIC}...${NC}"

    if kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP}" --list 2>/dev/null | grep -q "^${TOPIC}$"; then
        echo -e "${GREEN}Topic '${TOPIC}' 已存在 ✓${NC}"
    else
        echo -e "${YELLOW}Topic '${TOPIC}' 不存在，正在创建...${NC}"
        kafka-topics.sh --create \
            --topic "${TOPIC}" \
            --bootstrap-server "${KAFKA_BOOTSTRAP}" \
            --partitions 3 \
            --replication-factor 1 \
            2>/dev/null
        echo -e "${GREEN}Topic '${TOPIC}' 创建成功 ✓${NC}"
    fi
    return 0
}

# ==================== 提交 SQL 作业 ====================
submit_sql_job() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  提交 Flink SQL 作业${NC}"
    echo -e "${BLUE}========================================${NC}"

    local sql_file="${SCRIPT_DIR}/blog_pipeline.sql"

    if [ ! -f "${sql_file}" ]; then
        echo -e "${RED}错误: SQL 文件不存在: ${sql_file}${NC}"
        return 1
    fi

    echo -e "${YELLOW}SQL 文件: ${sql_file}${NC}"
    echo -e "${YELLOW}正在提交作业...${NC}"

    # 使用 Flink SQL CLI 提交
    "${FLINK_HOME}/bin/sql-client.sh" -f "${sql_file}"

    echo -e "${GREEN}SQL 作业提交完成 ✓${NC}"
    echo ""
    echo "在 Web UI 中查看作业状态: http://localhost:8081"
}

# ==================== 提交 Java 作业 ====================
submit_java_job() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  提交 Flink Java 作业${NC}"
    echo -e "${BLUE}========================================${NC}"

    local jar_file="${SCRIPT_DIR}/target/demo-flink-1.0.jar"

    if [ ! -f "${jar_file}" ]; then
        echo -e "${RED}错误: JAR 文件不存在: ${jar_file}${NC}"
        echo "请先编译项目："
        echo "  cd ${SCRIPT_DIR} && mvn clean package"
        return 1
    fi

    echo -e "${YELLOW}JAR 文件: ${jar_file}${NC}"
    echo -e "${YELLOW}正在提交作业...${NC}"

    "${FLINK_HOME}/bin/flink" run -c com.demo.flink.BlogEventStatistics "${jar_file}"

    echo -e "${GREEN}Java 作业提交完成 ✓${NC}"
    echo ""
    echo "在 Web UI 中查看作业状态: http://localhost:8081"
}

# ==================== 停止作业 ====================
stop_jobs() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  停止数据平台统计作业${NC}"
    echo -e "${BLUE}========================================${NC}"

    # 列出所有运行中的作业
    local jobs
    jobs=$("${FLINK_HOME}/bin/flink" list 2>/dev/null | grep -E "demo|Demo" || true)

    if [ -z "${jobs}" ]; then
        echo -e "${YELLOW}没有找到运行中的数据平台统计作业${NC}"
        return 0
    fi

    echo -e "${YELLOW}找到以下作业:${NC}"
    echo "${jobs}"

    # 逐个取消作业
    while read -r line; do
        local job_id
        job_id=$(echo "${line}" | awk '{print $4}')
        if [ -n "${job_id}" ]; then
            echo -e "${YELLOW}取消作业: ${job_id}${NC}"
            "${FLINK_HOME}/bin/flink" cancel "${job_id}" 2>/dev/null || true
        fi
    done <<< "${jobs}"

    echo -e "${GREEN}作业已停止 ✓${NC}"
}

# ==================== 查看作业状态 ====================
show_status() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  作业状态${NC}"
    echo -e "${BLUE}========================================${NC}"

    echo ""
    echo "运行中的作业："
    "${FLINK_HOME}/bin/flink" list 2>/dev/null || true

    echo ""
    echo "所有作业（含已完成）："
    "${FLINK_HOME}/bin/flink" list -a 2>/dev/null || true

    echo ""
    echo "Web UI: http://localhost:8081"
}

# ==================== 主逻辑 ====================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Flink 数据平台实时统计部署脚本${NC}"
echo -e "${BLUE}========================================${NC}"

case "${1:-sql}" in
    sql)
        check_flink && check_kafka && check_topic && submit_sql_job
        ;;
    java)
        check_flink && check_kafka && check_topic && submit_java_job
        ;;
    stop)
        stop_jobs
        ;;
    status)
        show_status
        ;;
    *)
        echo ""
        echo "用法: $0 {sql|java|stop|status}"
        echo ""
        echo "命令说明："
        echo "  sql    - 提交 Flink SQL 作业（默认）"
        echo "  java   - 提交 Flink Java 作业"
        echo "  stop   - 停止所有数据平台统计作业"
        echo "  status - 查看作业状态"
        exit 1
        ;;
esac
