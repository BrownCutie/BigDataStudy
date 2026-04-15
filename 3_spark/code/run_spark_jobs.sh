#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# 博客数据 Spark 作业运行脚本
# 功能：用 spark-submit 提交 PySpark 作业，用 spark-sql 执行查询
# 使用方式：bash run_spark_jobs.sh

set -euo pipefail

# ============================================================
# 配置区域
# ============================================================

# 脚本所在目录（即 code/ 目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
OUTPUT_DIR="${SCRIPT_DIR}/output"

# Spark 配置
SPARK_MASTER="local[*]"
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="2g"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # 无颜色

# ============================================================
# 工具函数
# ============================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_files() {
    """检查必要的博客图片是否存在"""
    log_info "检查博客图片..."

    local missing=0
    for f in "${DATA_DIR}/posts.csv" "${DATA_DIR}/access_logs.csv"; do
        if [ ! -f "$f" ]; then
            log_error "博客图片缺失: $f"
            missing=$((missing + 1))
        else
            log_info "博客图片存在: $f"
        fi
    done

    if [ $missing -gt 0 ]; then
        log_error "缺少 $missing 个博客图片，请先准备数据"
        exit 1
    fi
}

check_spark() {
    """检查 Spark 是否可用"""
    log_info "检查 Spark 环境..."

    # 检查 spark-submit 命令
    if command -v spark-submit &> /dev/null; then
        log_info "spark-submit 已安装: $(spark-submit --version 2>&1 | head -1)"
    elif [ -n "${SPARK_HOME:-}" ]; then
        log_info "使用 SPARK_HOME: ${SPARK_HOME}"
    else
        log_warn "未找到 spark-submit，尝试使用 PySpark 直接运行"
        return 1
    fi

    # 检查 Python 环境
    if command -v python3 &> /dev/null; then
        local py_ver=$(python3 --version 2>&1)
        log_info "Python 版本: ${py_ver}"
    fi

    return 0
}

# ============================================================
# 作业 1: ETL 数据清洗
# ============================================================

run_etl_job() {
    log_info "=========================================="
    log_info "作业 1: 运行 ETL 数据清洗"
    log_info "=========================================="

    local etl_script="${SCRIPT_DIR}/etl_demo.py"

    if [ ! -f "$etl_script" ]; then
        log_error "ETL 脚本不存在: $etl_script"
        return 1
    fi

    if check_spark; then
        # 使用 spark-submit 提交
        log_info "使用 spark-submit 提交 ETL 作业..."

        spark-submit \
            --master "${SPARK_MASTER}" \
            --driver-memory "${DRIVER_MEMORY}" \
            --executor-memory "${EXECUTOR_MEMORY}" \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.shuffle.partitions=10 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.catalogImplementation=hive \
            "$etl_script" \
            2>&1 | tee "${OUTPUT_DIR}/etl_$(date +%Y%m%d_%H%M%S).log"
    else
        # 回退到直接使用 python 运行（依赖 pip 安装的 pyspark）
        log_warn "使用 python3 直接运行 ETL 脚本..."
        python3 "$etl_script" \
            2>&1 | tee "${OUTPUT_DIR}/etl_$(date +%Y%m%d_%H%M%S).log"
    fi
}

# ============================================================
# 作业 2: 数据分析
# ============================================================

run_analysis_job() {
    log_info "=========================================="
    log_info "作业 2: 运行数据分析"
    log_info "=========================================="

    local analysis_script="${SCRIPT_DIR}/blog_analysis.py"

    if [ ! -f "$analysis_script" ]; then
        log_error "分析脚本不存在: $analysis_script"
        return 1
    fi

    if check_spark; then
        log_info "使用 spark-submit 提交分析作业..."

        spark-submit \
            --master "${SPARK_MASTER}" \
            --driver-memory "${DRIVER_MEMORY}" \
            --executor-memory "${EXECUTOR_MEMORY}" \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.adaptive.skewJoin.enabled=true \
            --conf spark.sql.shuffle.partitions=10 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            "$analysis_script" \
            2>&1 | tee "${OUTPUT_DIR}/analysis_$(date +%Y%m%d_%H%M%S).log"
    else
        log_warn "使用 python3 直接运行分析脚本..."
        python3 "$analysis_script" \
            2>&1 | tee "${OUTPUT_DIR}/analysis_$(date +%Y%m%d_%H%M%S).log"
    fi
}

# ============================================================
# 作业 3: spark-sql 交互式查询
# ============================================================

run_sql_queries() {
    log_info "=========================================="
    log_info "作业 3: 执行 spark-sql 查询"
    log_info "=========================================="

    # 创建临时 SQL 文件
    local sql_file="${OUTPUT_DIR}/queries_$(date +%Y%m%d_%H%M%S).sql"

    cat > "$sql_file" << 'SQLEOF'
-- ============================================
-- 博客数据 spark-sql 查询示例
-- ============================================

-- 设置数据库
CREATE DATABASE IF NOT EXISTS blog_db;
USE blog_db;

-- 查询 1：查看所有文章的浏览量排行
SELECT
    id,
    title,
    author,
    category,
    views,
    likes,
    comments,
    publish_date
FROM blog_db.posts
ORDER BY views DESC
LIMIT 10;

-- 查询 2：各分类文章统计
SELECT
    category,
    COUNT(*) AS article_count,
    SUM(views) AS total_views,
    ROUND(AVG(views), 2) AS avg_views
FROM blog_db.posts
GROUP BY category
ORDER BY total_views DESC;

-- 查询 3：作者排名
SELECT
    author,
    COUNT(*) AS article_count,
    SUM(views) AS total_views,
    MAX(views) AS max_views,
    SUM(likes) AS total_likes
FROM blog_db.posts
GROUP BY author
ORDER BY total_views DESC;

-- 查询 4：使用窗口函数计算文章排名
SELECT
    id,
    title,
    author,
    views,
    RANK() OVER (ORDER BY views DESC) AS global_rank,
    RANK() OVER (PARTITION BY category ORDER BY views DESC) AS category_rank,
    DENSE_RANK() OVER (ORDER BY views DESC) AS dense_rank
FROM blog_db.posts
ORDER BY global_rank;

-- 查询 5：访问日志统计
SELECT
    action,
    COUNT(*) AS action_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT post_id) AS unique_posts
FROM blog_db.access_logs
GROUP BY action
ORDER BY action_count DESC;
SQLEOF

    if command -v spark-sql &> /dev/null; then
        log_info "使用 spark-sql 执行查询文件: $sql_file"

        spark-sql \
            --master "${SPARK_MASTER}" \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.shuffle.partitions=10 \
            -f "$sql_file" \
            2>&1 | tee "${OUTPUT_DIR}/sql_results_$(date +%Y%m%d_%H%M%S).log"
    else
        log_warn "未找到 spark-sql 命令，跳过 SQL 查询"
        log_info "SQL 查询文件已生成: $sql_file"
        log_info "可手动执行: spark-sql -f $sql_file"
    fi
}

# ============================================================
# 作业 4: 全部执行
# ============================================================

run_all() {
    log_info "=========================================="
    log_info "执行全部 Spark 作业"
    log_info "=========================================="

    mkdir -p "${OUTPUT_DIR}"

    run_etl_job
    echo ""
    run_analysis_job
    echo ""
    run_sql_queries

    log_info "=========================================="
    log_info "全部作业执行完成!"
    log_info "结果目录: ${OUTPUT_DIR}"
    log_info "=========================================="
}

# ============================================================
# 帮助信息
# ============================================================

show_help() {
    echo "博客数据 Spark 作业运行脚本"
    echo ""
    echo "使用方式:"
    echo "  bash run_spark_jobs.sh etl        # 运行 ETL 数据清洗"
    echo "  bash run_spark_jobs.sh analysis   # 运行数据分析"
    echo "  bash run_spark_jobs.sh sql        # 执行 spark-sql 查询"
    echo "  bash run_spark_jobs.sh all        # 执行全部作业"
    echo "  bash run_spark_jobs.sh help       # 显示帮助信息"
    echo ""
    echo "配置参数（可在脚本中修改）:"
    echo "  SPARK_MASTER      = ${SPARK_MASTER}"
    echo "  DRIVER_MEMORY     = ${DRIVER_MEMORY}"
    echo "  EXECUTOR_MEMORY   = ${EXECUTOR_MEMORY}"
    echo "  DATA_DIR          = ${DATA_DIR}"
    echo "  OUTPUT_DIR        = ${OUTPUT_DIR}"
}

# ============================================================
# 主入口
# ============================================================

# 默认执行全部
ACTION="${1:-all}"

case "$ACTION" in
    etl)
        check_files
        mkdir -p "${OUTPUT_DIR}"
        run_etl_job
        ;;
    analysis)
        check_files
        mkdir -p "${OUTPUT_DIR}"
        run_analysis_job
        ;;
    sql)
        check_files
        mkdir -p "${OUTPUT_DIR}"
        run_sql_queries
        ;;
    all)
        check_files
        run_all
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "未知命令: $ACTION"
        show_help
        exit 1
        ;;
esac
