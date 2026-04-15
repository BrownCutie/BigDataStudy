#!/bin/bash
# ==============================================================================
# Kafka 博客事件管道演示脚本
# ==============================================================================
# 功能：
#   1. 创建 blog-events Topic（如果不存在）
#   2. 运行消费者（在新终端中）
#   3. 运行生产者，发送测试数据
#
# 前置条件：
#   - Kafka 4.2.0 已启动（KRaft 模式，端口 9092）
#   - Python 3 已安装，且已安装 kafka-python（pip install kafka-python）
#
# 使用方法：
#   chmod +x run_kafka_demo.sh
#   ./run_kafka_demo.sh
# ==============================================================================

set -e  # 遇到错误立即退出

# ==================== 配置 ====================
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka/4.2.0}"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC="blog-events"
PARTITIONS=3
REPLICATION_FACTOR=1
CONSUMER_GROUP="demo-event-consumer"

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # 无颜色

# ==================== 检查 Kafka 是否运行 ====================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Kafka 博客事件管道演示${NC}"
echo -e "${BLUE}========================================${NC}"

echo ""
echo -e "${YELLOW}[1/5] 检查 Kafka 是否运行...${NC}"

if ! lsof -i :9092 > /dev/null 2>&1; then
    echo -e "${RED}错误: Kafka 未在端口 9092 运行！${NC}"
    echo "请先启动 Kafka："
    echo "  kafka-server-start.sh \$KAFKA_HOME/config/server.properties"
    exit 1
fi

echo -e "${GREEN}Kafka 正在运行 ✓${NC}"

# ==================== 检查 Python 依赖 ====================
echo ""
echo -e "${YELLOW}[2/5] 检查 Python 依赖...${NC}"

if ! python3 -c "import kafka" 2>/dev/null; then
    echo -e "${RED}错误: kafka-python 未安装！${NC}"
    echo "请执行：pip install kafka-python"
    exit 1
fi

echo -e "${GREEN}kafka-python 已安装 ✓${NC}"

# ==================== 创建 Topic ====================
echo ""
echo -e "${YELLOW}[3/5] 创建 Topic: ${TOPIC}...${NC}"

# 检查 Topic 是否已存在
if kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo -e "${GREEN}Topic '${TOPIC}' 已存在 ✓${NC}"
else
    kafka-topics.sh --create \
        --topic "$TOPIC" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        2>/dev/null

    if kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -q "^${TOPIC}$"; then
        echo -e "${GREEN}Topic '${TOPIC}' 创建成功 ✓${NC}"
    else
        echo -e "${RED}Topic 创建失败！${NC}"
        exit 1
    fi
fi

# 显示 Topic 信息
echo ""
echo "Topic 详情："
kafka-topics.sh --describe \
    --topic "$TOPIC" \
    --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null

# ==================== 启动消费者 ====================
echo ""
echo -e "${YELLOW}[4/5] 启动消费者...${NC}"
echo -e "${BLUE}消费者将在新终端窗口中运行${NC}"
echo -e "${BLUE}（按 Ctrl+C 可以停止消费者）${NC}"
echo ""

# 在新的终端窗口中启动消费者
osascript -e "tell application \"Terminal\" to do script \"cd ${SCRIPT_DIR} && python3 consumer.py\"" 2>/dev/null || \
    osascript -e "tell application \"iTerm\" to create window with default profile command \"cd ${SCRIPT_DIR} && python3 consumer.py\"" 2>/dev/null || \
    echo -e "${YELLOW}提示: 请手动在新终端中运行: cd ${SCRIPT_DIR} && python3 consumer.py${NC}"

# 等待消费者启动
echo -e "${GREEN}等待消费者启动（3 秒）...${NC}"
sleep 3

# ==================== 运行生产者 ====================
echo ""
echo -e "${YELLOW}[5/5] 运行生产者，发送测试数据...${NC}"
echo ""

python3 "${SCRIPT_DIR}/producer.py"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  演示完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "消费者终端窗口中应该能看到实时统计信息。"
echo ""
echo "你也可以手动发送消息进行测试："
echo "  kafka-console-producer.sh --topic ${TOPIC} --bootstrap-server ${BOOTSTRAP_SERVER}"
echo ""
echo "查看消费者组的消费进度："
echo "  kafka-consumer-groups.sh --describe --group ${CONSUMER_GROUP} --bootstrap-server ${BOOTSTRAP_SERVER}"
