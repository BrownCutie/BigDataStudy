#!/bin/bash
# ============================================================
# 发送通知脚本
# 用于 DolphinScheduler 的 Shell 任务节点
# 在工作流完成后发送通知（支持邮件、Webhook、日志记录）
# 适用于博客数据处理管道的运行状态通知
# ============================================================

# ============================================================
# 配置区域（根据实际环境修改）
# ============================================================

# 通知方式: webhook / email / log / all
NOTIFY_METHOD="${NOTIFY_METHOD:-log}"

# Webhook 配置（如企业社交平台、钉钉、飞书等）
WEBHOOK_URL="${WEBHOOK_URL:-}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"  # 签名密钥（可选）

# 邮件配置
SMTP_HOST="${SMTP_HOST:-smtp.example.com}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_USER="${SMTP_USER:-}"
SMTP_PASS="${SMTP_PASS:-}"
MAIL_FROM="${MAIL_FROM:-noreply@demo.com}"
MAIL_TO="${MAIL_TO:-admin@demo.com}"

# 日志目录
LOG_DIR="/tmp/notification-logs"
LOG_FILE="${LOG_DIR}/notify_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# DolphinScheduler 任务参数（由调度系统传入）
# ============================================================
# 这些变量可从 DolphinScheduler 的 Shell 任务参数中获取
TASK_NAME="${TASK_NAME:-未命名任务}"
PROCESS_INSTANCE_ID="${PROCESS_INSTANCE_ID:-0}"
EXECUTION_STATUS="${EXECUTION_STATUS:-success}"  # success / failure / warning
DAG_NAME="${DAG_NAME:-博客数据管道}"
EXECUTE_TIME="${EXECUTE_TIME:-$(date '+%Y-%m-%d %H:%M:%S')}"
DURATION="${DURATION:-0s}"
MESSAGE="${MESSAGE:-任务执行完成}"
ERROR_MSG="${ERROR_MSG:-}"

# ============================================================
# 工具函数
# ============================================================

log() {
    local msg="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${msg}" | tee -a "${LOG_FILE}"
}

# 构建通知消息体
build_message() {
    # 根据执行状态选择图标和标题
    case "$EXECUTION_STATUS" in
        success)
            STATUS_TEXT="成功"
            STATUS_ICON="OK"
            ;;
        failure)
            STATUS_TEXT="失败"
            STATUS_ICON="FAIL"
            ;;
        warning)
            STATUS_TEXT="警告"
            STATUS_ICON="WARN"
            ;;
        *)
            STATUS_TEXT="未知"
            STATUS_ICON="???"
            ;;
    esac

    # 构建消息内容
    cat <<EOF
========================================
 DolphinScheduler 任务通知
========================================
工作流:  ${DAG_NAME}
任务:    ${TASK_NAME}
状态:    [${STATUS_ICON}] ${STATUS_TEXT}
执行时间: ${EXECUTE_TIME}
耗时:    ${DURATION}
流程实例: ${PROCESS_INSTANCE_ID}
----------------------------------------
${MESSAGE}
EOF

    # 如果有错误信息，追加
    if [ -n "$ERROR_MSG" ]; then
        echo "----------------------------------------"
        echo "错误详情:"
        echo "$ERROR_MSG"
    fi

    echo "========================================"
}

# ============================================================
# 通知方式实现
# ============================================================

# 发送到 Webhook（企业社交平台、钉钉、飞书等通用格式）
send_webhook() {
    if [ -z "$WEBHOOK_URL" ]; then
        log "Webhook URL 未配置，跳过 Webhook 通知"
        return 1
    fi

    local msg_body
    msg_body=$(build_message)

    # 构建 JSON 负载（兼容企业社交平台/钉钉/飞书的通用格式）
    local json_payload
    json_payload=$(python3 -c "
import json, sys

status_text = '${STATUS_TEXT}'
color = '#2ecc71' if status_text == '成功' else '#e74c3c' if status_text == '失败' else '#f39c12'

payload = {
    'msgtype': 'markdown',
    'markdown': {
        'content': '''## DolphinScheduler 任务通知

> **状态**: **${status_text}**

**工作流**: ${DAG_NAME}
**任务**: ${TASK_NAME}
**执行时间**: ${EXECUTE_TIME}
**耗时**: ${DURATION}
**流程实例ID**: ${PROCESS_INSTANCE_ID}

---

$MESSAGE
'''
    }
}

print(json.dumps(payload, ensure_ascii=False))
" 2>/dev/null)

    if [ -z "$json_payload" ]; then
        log "Webhook JSON 构建失败"
        return 1
    fi

    # 发送请求
    local response
    response=$(curl -s -X POST "${WEBHOOK_URL}" \
        -H "Content-Type: application/json" \
        -d "$json_payload" 2>/dev/null)

    log "Webhook 发送完成, 响应: ${response}"
}

# 发送邮件通知
send_email() {
    if [ -z "$SMTP_USER" ] || [ -z "$MAIL_TO" ]; then
        log "邮件配置不完整，跳过邮件通知"
        return 1
    fi

    local msg_body
    msg_body=$(build_message)

    # 构建邮件内容
    local subject="[DolphinScheduler] ${DAG_NAME} - ${TASK_NAME} [${STATUS_TEXT}]"

    # 使用 python 发送邮件（更可靠）
    python3 << PYEOF 2>> "${LOG_FILE}"
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    msg = MIMEMultipart()
    msg['From'] = '${MAIL_FROM}'
    msg['To'] = '${MAIL_TO}'
    msg['Subject'] = '''${subject}'''

    body = '''${msg_body}'''
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    with smtplib.SMTP('${SMTP_HOST}', ${SMTP_PORT}) as server:
        server.starttls()
        server.login('${SMTP_USER}', '${SMTP_PASS}')
        server.send_message(msg)
    print('邮件发送成功')
except Exception as e:
    print(f'邮件发送失败: {e}')
PYEOF
}

# 记录到日志文件
send_log() {
    local msg_body
    msg_body=$(build_message)

    log "===== 通知记录 ====="
    echo "$msg_body" >> "${LOG_FILE}"
    log "通知已记录到日志: ${LOG_FILE}"
}

# ============================================================
# 主流程
# ============================================================
mkdir -p "${LOG_DIR}"

log "=========================================="
log " 通知发送脚本启动"
log "=========================================="

# 根据配置的通知方式执行
case "$NOTIFY_METHOD" in
    webhook)
        send_webhook
        ;;
    email)
        send_email
        ;;
    log)
        send_log
        ;;
    all)
        send_webhook
        send_email
        send_log
        ;;
    *)
        log "未知通知方式: ${NOTIFY_METHOD}，使用默认 log"
        send_log
        ;;
esac

log "通知脚本执行完毕"
exit 0
