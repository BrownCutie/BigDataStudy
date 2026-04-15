#!/bin/bash
# ============================================================
# MinIO 批量上传博客图片示例
# 使用 mc 命令行工具将本地图片批量上传到 MinIO
# ============================================================

# MinIO 配置
MINIO_ALIAS="myminio"                   # mc 别名（需先配置）
MINIO_HOST="localhost:9002"              # MinIO API 地址
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET="demo-assets"                     # 目标存储桶

# 本地图片目录
LOCAL_DIR="./sample_images"

# 远程目标前缀（类似文件夹概念）
REMOTE_PREFIX="demo/images"

echo "=========================================="
echo " MinIO 批量上传博客图片"
echo "=========================================="
echo "目标存储桶: ${BUCKET}"
echo "远程前缀:   ${REMOTE_PREFIX}"
echo "本地目录:   ${LOCAL_DIR}"
echo ""

# 1. 配置 mc 别名（如果尚未配置）
echo "[1/4] 配置 mc 连接..."
mc alias set "${MINIO_ALIAS}" "http://${MINIO_HOST}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "  mc 别名 '${MINIO_ALIAS}' 配置成功"
else
    echo "  mc 别名 '${MINIO_ALIAS}' 已存在或配置失败"
fi

# 2. 确保存储桶存在
echo ""
echo "[2/4] 确保存储桶存在..."
mc mb "${MINIO_ALIAS}/${BUCKET}" --ignore-existing 2>/dev/null
echo "  存储桶 '${BUCKET}' 已就绪"

# 3. 创建本地示例图片目录（如果没有真实图片则创建测试文件）
echo ""
echo "[3/4] 准备本地文件..."
if [ ! -d "${LOCAL_DIR}" ]; then
    mkdir -p "${LOCAL_DIR}"
    # 创建一些测试文件用于演示
    echo "<!-- 数据平台 Logo 占位 -->" > "${LOCAL_DIR}/logo.png"
    echo "<!-- 数据平台 Banner 占位 -->" > "${LOCAL_DIR}/banner.jpg"
    echo "<!-- 数据平台头像占位 -->" > "${LOCAL_DIR}/avatar.jpg"
    echo "<!-- 文章配图1 -->" > "${LOCAL_DIR}/post-image-1.png"
    echo "<!-- 文章配图2 -->" > "${LOCAL_DIR}/post-image-2.png"
    echo "  已创建测试文件目录: ${LOCAL_DIR}"
else
    echo "  使用已有目录: ${LOCAL_DIR}"
fi

# 4. 批量上传
echo ""
echo "[4/4] 批量上传文件..."
FILE_COUNT=0
SUCCESS_COUNT=0

# 遍历本地目录中的所有文件
for file in "${LOCAL_DIR}"/*; do
    if [ -f "$file" ]; then
        FILENAME=$(basename "$file")
        REMOTE_PATH="${REMOTE_PREFIX}/${FILENAME}"
        FILE_COUNT=$((FILE_COUNT + 1))

        # 使用 mc cp 上传单个文件
        mc cp "$file" "${MINIO_ALIAS}/${BUCKET}/${REMOTE_PATH}" 2>/dev/null
        if [ $? -eq 0 ]; then
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            echo "  [成功] ${FILENAME} -> ${BUCKET}/${REMOTE_PATH}"
        else
            echo "  [失败] ${FILENAME}"
        fi
    fi
done

echo ""
echo "=========================================="
echo " 上传完成: ${SUCCESS_COUNT}/${FILE_COUNT} 个文件成功"
echo "=========================================="
echo ""

# 显示上传后的文件列表
echo "存储桶文件列表:"
mc ls "${MINIO_ALIAS}/${BUCKET}/${REMOTE_PREFIX}/" 2>/dev/null || echo "  (无文件)"

echo ""
echo "提示: 可通过 http://${MINIO_HOST##*:}/访问 MinIO 控制台（端口 9001）查看上传结果"
