#!/bin/bash
# ============================================================
# Hadoop MapReduce WordCount 运行脚本
#
# 功能：
#   1. 上传测试数据到 HDFS
#   2. 运行 Java 版 WordCount
#   3. 运行 Python Streaming 版 WordCount
#   4. 查看并对比输出结果
#
# 使用方式：
#   chmod +x run_wordcount.sh
#   ./run_wordcount.sh
#
# 前置条件：
#   - Hadoop 已启动（HDFS 和 YARN）
#   - Java 21 已安装
#   - Python 3 已安装
# ============================================================

# --------------- 配置区 ---------------

# Hadoop 安装目录（根据实际环境修改）
HADOOP_HOME="${HADOOP_HOME:-$HOME/Programme/hadoop/hadoop}"

# HDFS 上的输入输出路径
HDFS_INPUT="/user/$(whoami)/wordcount/input"
HDFS_OUTPUT_JAVA="/user/$(whoami)/wordcount/output_java"
HDFS_OUTPUT_PYTHON="/user/$(whoami)/wordcount/output_python"

# 本地文件路径
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_FILE="$SCRIPT_DIR/data/word_count_input.txt"
JAVA_SRC="$SCRIPT_DIR/WordCount.java"
MAPPER_PY="$SCRIPT_DIR/mapper.py"
REDUCER_PY="$SCRIPT_DIR/reducer.py"

# Hadoop Streaming JAR 路径
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"

# --------------- 函数定义 ---------------

# 打印分隔线
print_separator() {
    echo "============================================================"
}

# 打印步骤标题
print_step() {
    echo ""
    print_separator
    echo "  $1"
    print_separator
}

# --------------- 环境检查 ---------------

print_step "步骤 0：环境检查"

# 检查 Hadoop 是否可用
if ! command -v hadoop &> /dev/null; then
    echo "[错误] 未找到 hadoop 命令，请确认 Hadoop 已安装并配置到 PATH 中"
    exit 1
fi
echo "[OK] Hadoop 命令可用"

# 检查 Java 是否可用
if ! command -v java &> /dev/null; then
    echo "[错误] 未找到 java 命令，请确认 Java 已安装并配置到 PATH 中"
    exit 1
fi
echo "[OK] Java 命令可用：$(java -version 2>&1 | head -1)"

# 检查 Python 是否可用
if ! command -v python3 &> /dev/null; then
    echo "[错误] 未找到 python3 命令，请确认 Python 3 已安装"
    exit 1
fi
echo "[OK] Python3 命令可用：$(python3 --version 2>&1)"

# 检查博客图片是否存在
if [ ! -f "$DATA_FILE" ]; then
    echo "[错误] 博客图片不存在：$DATA_FILE"
    exit 1
fi
echo "[OK] 博客图片存在：$DATA_FILE"

# 检查 Java 源文件是否存在
if [ ! -f "$JAVA_SRC" ]; then
    echo "[错误] Java 源文件不存在：$JAVA_SRC"
    exit 1
fi
echo "[OK] Java 源文件存在：$JAVA_SRC"

# 检查 Streaming JAR 是否存在
if [ ! -f "$STREAMING_JAR" ]; then
    echo "[错误] Hadoop Streaming JAR 不存在：$STREAMING_JAR"
    exit 1
fi
echo "[OK] Streaming JAR 存在"

# --------------- 上传数据到 HDFS ---------------

print_step "步骤 1：上传输入数据到 HDFS"

# 创建 HDFS 输入目录（如果不存在）
echo "创建 HDFS 目录：$HDFS_INPUT"
hadoop fs -mkdir -p "$HDFS_INPUT"

# 上传本地博客图片到 HDFS
echo "上传博客图片到 HDFS..."
hadoop fs -put -f "$DATA_FILE" "$HDFS_INPUT/"

# 验证上传结果
echo "验证 HDFS 上的文件："
hadoop fs -ls "$HDFS_INPUT/"
echo "文件内容前 5 行："
hadoop fs -cat "$HDFS_INPUT/$(basename "$DATA_FILE")" | head -5

# --------------- 编译 Java 版 WordCount ---------------

print_step "步骤 2：编译 Java 版 WordCount"

# 设置编译输出目录
BUILD_DIR="$SCRIPT_DIR/build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# 获取 Hadoop classpath
HADOOP_CLASSPATH=$(hadoop classpath)

# 编译 Java 源文件
echo "编译 WordCount.java..."
javac -classpath "$HADOOP_CLASSPATH" -d "$BUILD_DIR" "$JAVA_SRC"

if [ $? -ne 0 ]; then
    echo "[错误] 编译失败，请检查 Java 源代码"
    exit 1
fi
echo "[OK] 编译成功"

# 打包为 JAR 文件
JAR_FILE="$SCRIPT_DIR/wordcount.jar"
echo "打包为 $JAR_FILE..."
jar cf "$JAR_FILE" -C "$BUILD_DIR" .

if [ $? -ne 0 ]; then
    echo "[错误] 打包失败"
    exit 1
fi
echo "[OK] 打包成功：$JAR_FILE"

# --------------- 运行 Java 版 WordCount ---------------

print_step "步骤 3：运行 Java 版 WordCount"

# 删除旧的输出目录（如果存在）
echo "清理旧的输出目录..."
hadoop fs -rm -r -f "$HDFS_OUTPUT_JAVA" 2>/dev/null

# 运行 MapReduce 作业
echo "提交 MapReduce 作业..."
echo "  输入路径：$HDFS_INPUT"
echo "  输出路径：$HDFS_OUTPUT_JAVA"
echo ""

hadoop jar "$JAR_FILE" WordCount "$HDFS_INPUT" "$HDFS_OUTPUT_JAVA"

if [ $? -ne 0 ]; then
    echo "[错误] Java 版 WordCount 运行失败"
    exit 1
fi
echo ""
echo "[OK] Java 版 WordCount 运行成功"

# --------------- 运行 Python Streaming 版 WordCount ---------------

print_step "步骤 4：运行 Python Streaming 版 WordCount"

# 删除旧的输出目录（如果存在）
echo "清理旧的输出目录..."
hadoop fs -rm -r -f "$HDFS_OUTPUT_PYTHON" 2>/dev/null

# 运行 Streaming 作业
echo "提交 Streaming 作业..."
echo "  输入路径：$HDFS_INPUT"
echo "  输出路径：$HDFS_OUTPUT_PYTHON"
echo "  Mapper：$MAPPER_PY"
echo "  Reducer：$REDUCER_PY"
echo ""

hadoop jar "$STREAMING_JAR" \
    -input "$HDFS_INPUT" \
    -output "$HDFS_OUTPUT_PYTHON" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file "$MAPPER_PY" \
    -file "$REDUCER_PY"

if [ $? -ne 0 ]; then
    echo "[错误] Python Streaming 版 WordCount 运行失败"
    exit 1
fi
echo ""
echo "[OK] Python Streaming 版 WordCount 运行成功"

# --------------- 查看输出结果 ---------------

print_step "步骤 5：查看输出结果"

echo ""
echo "--- Java 版 WordCount 输出（按出现次数降序排列前 20 个）---"
hadoop fs -cat "$HDFS_OUTPUT_JAVA/part-*" | sort -t$'\t' -k2 -nr | head -20

echo ""
echo "--- Python Streaming 版 WordCount 输出（按出现次数降序排列前 20 个）---"
hadoop fs -cat "$HDFS_OUTPUT_PYTHON/part-*" | sort -t$'\t' -k2 -nr | head -20

# --------------- 清理 ---------------

print_step "步骤 6：清理编译产物"

echo "清理编译目录：$BUILD_DIR"
rm -rf "$BUILD_DIR"

echo ""
print_separator
echo "  全部任务执行完成！"
echo ""
echo "  Java 版输出：hadoop fs -cat $HDFS_OUTPUT_JAVA/part-*"
echo "  Python版输出：hadoop fs -cat $HDFS_OUTPUT_PYTHON/part-*"
echo ""
echo "  如需清理 HDFS 上的输出："
echo "    hadoop fs -rm -r $HDFS_OUTPUT_JAVA"
echo "    hadoop fs -rm -r $HDFS_OUTPUT_PYTHON"
print_separator
