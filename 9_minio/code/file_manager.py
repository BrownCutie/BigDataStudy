#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MinIO 文件管理器
使用 minio-py SDK 实现文件的上传、下载、列表、删除和预签名 URL 功能
适用于数据平台的资源文件管理场景
"""

import os
import sys
import mimetypes
from datetime import timedelta

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("错误: 请先安装 minio SDK: pip install minio")
    sys.exit(1)


# ============================================================
# 配置信息
# ============================================================
MINIO_ENDPOINT = "localhost:9002"       # MinIO API 端口
MINIO_ACCESS_KEY = "minioadmin"          # 默认用户名
MINIO_SECRET_KEY = "minioadmin"          # 默认密码
MINIO_SECURE = False                     # 不使用 HTTPS

# 数据平台使用的存储桶
BUCKET_ASSETS = "demo-assets"            # 静态资源（图片、CSS、JS）
BUCKET_DATA = "demo-data"                # 博客图片（JSON、CSV）


def get_client():
    """创建并返回 MinIO 客户端实例"""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    return client


def ensure_bucket(client, bucket_name):
    """确保存储桶存在，不存在则创建"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"  [创建] 存储桶 '{bucket_name}' 已创建")
    else:
        print(f"  [存在] 存储桶 '{bucket_name}' 已存在")


def list_buckets(client):
    """列出所有存储桶"""
    print("\n=== 存储桶列表 ===")
    buckets = client.list_buckets()
    for bucket in buckets:
        print(f"  {bucket.name} (创建时间: {bucket.creation_date})")
    print(f"  共 {len(buckets)} 个存储桶\n")


def upload_file(client, bucket_name, file_path, object_name=None):
    """
    上传文件到指定存储桶

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        file_path: 本地文件路径
        object_name: 对象名称（不指定则使用文件名）
    """
    if not os.path.isfile(file_path):
        print(f"  错误: 文件不存在 - {file_path}")
        return False

    # 默认使用文件名作为对象名
    if object_name is None:
        object_name = os.path.basename(file_path)

    # 自动检测 Content-Type
    content_type, _ = mimetypes.guess_type(file_path)
    if content_type is None:
        content_type = "application/octet-stream"

    try:
        result = client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type
        )
        print(f"  [上传成功] {file_path} -> {bucket_name}/{object_name}")
        print(f"           ETAG: {result.etag}, 大小: {result.object_size} bytes")
        return True
    except S3Error as e:
        print(f"  [上传失败] {e}")
        return False


def download_file(client, bucket_name, object_name, download_path):
    """
    从存储桶下载文件

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        object_name: 对象名称
        download_path: 本地保存路径
    """
    try:
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=download_path
        )
        file_size = os.path.getsize(download_path)
        print(f"  [下载成功] {bucket_name}/{object_name} -> {download_path} ({file_size} bytes)")
        return True
    except S3Error as e:
        print(f"  [下载失败] {e}")
        return False


def list_objects(client, bucket_name, prefix=None):
    """
    列出存储桶中的对象

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        prefix: 对象名前缀过滤（可选）
    """
    print(f"\n=== {bucket_name} 中的对象列表 ===")
    count = 0
    objects = client.list_objects(bucket_name, prefix=prefix)
    for obj in objects:
        size_kb = obj.size / 1024 if obj.size else 0
        print(f"  {obj.object_name}  ({size_kb:.1f} KB, 修改时间: {obj.last_modified})")
        count += 1
    print(f"  共 {count} 个对象\n")


def delete_object(client, bucket_name, object_name):
    """
    删除存储桶中的单个对象

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        object_name: 对象名称
    """
    try:
        client.remove_object(bucket_name, object_name)
        print(f"  [删除成功] {bucket_name}/{object_name}")
        return True
    except S3Error as e:
        print(f"  [删除失败] {e}")
        return False


def generate_presigned_url(client, bucket_name, object_name, expires_hours=24):
    """
    生成预签名 URL，允许无凭证访问对象

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        object_name: 对象名称
        expires_hours: URL 有效期（小时）
    """
    try:
        url = client.presigned_get_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=timedelta(hours=expires_hours)
        )
        print(f"  [预签名 URL] {bucket_name}/{object_name}")
        print(f"  有效期: {expires_hours} 小时")
        print(f"  URL: {url}")
        return url
    except S3Error as e:
        print(f"  [生成失败] {e}")
        return None


def generate_presigned_upload_url(client, bucket_name, object_name, expires_hours=1):
    """
    生成预签名上传 URL，允许客户端直接上传文件

    参数:
        client: MinIO 客户端
        bucket_name: 存储桶名称
        object_name: 对象名称
        expires_hours: URL 有效期（小时）
    """
    try:
        url = client.presigned_put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=timedelta(hours=expires_hours)
        )
        print(f"  [预签名上传 URL] {bucket_name}/{object_name}")
        print(f"  有效期: {expires_hours} 小时")
        print(f"  URL: {url}")
        return url
    except S3Error as e:
        print(f"  [生成失败] {e}")
        return None


def get_object_info(client, bucket_name, object_name):
    """获取对象的详细信息（元数据）"""
    try:
        stat = client.stat_object(bucket_name, object_name)
        print(f"\n=== 对象信息: {bucket_name}/{object_name} ===")
        print(f"  大小: {stat.size} bytes ({stat.size / 1024:.1f} KB)")
        print(f"  Content-Type: {stat.content_type}")
        print(f"  最后修改: {stat.last_modified}")
        print(f"  ETag: {stat.etag}")
        if stat.metadata:
            print(f"  自定义元数据:")
            for key, value in stat.metadata.items():
                if not key.startswith("x-amz-"):
                    print(f"    {key}: {value}")
        return stat
    except S3Error as e:
        print(f"  [获取失败] {e}")
        return None


# ============================================================
# 演示主流程
# ============================================================
def main():
    print("=" * 50)
    print(" MinIO 文件管理器演示")
    print("=" * 50)

    # 创建客户端
    client = get_client()
    print(f"\n连接到 MinIO: {MINIO_ENDPOINT}")

    # 测试连接
    try:
        # 确保存储桶存在
        ensure_bucket(client, BUCKET_ASSETS)
        ensure_bucket(client, BUCKET_DATA)
    except S3Error as e:
        print(f"连接失败: {e}")
        print("请确认 MinIO 服务已启动（端口 9002）")
        sys.exit(1)

    # 列出所有存储桶
    list_buckets(client)

    # 演示上传：创建一个测试文件
    test_file = "/tmp/minio_test_upload.txt"
    with open(test_file, "w", encoding="utf-8") as f:
        f.write("这是一个 MinIO 文件管理器的测试文件。\n")
        f.write("用于演示文件的上传、下载和删除功能。\n")
    print(f"\n=== 创建测试文件: {test_file} ===")

    # 上传测试文件
    print("\n=== 上传文件 ===")
    upload_file(client, BUCKET_DATA, test_file, "test/minio_test_upload.txt")

    # 上传同一文件到资源桶（演示不同存储桶）
    upload_file(client, BUCKET_ASSETS, test_file, "test/test_upload.txt")

    # 列出对象
    list_objects(client, BUCKET_DATA)
    list_objects(client, BUCKET_ASSETS)

    # 获取对象信息
    get_object_info(client, BUCKET_DATA, "test/minio_test_upload.txt")

    # 生成预签名 URL
    print("\n=== 生成预签名下载 URL ===")
    generate_presigned_url(client, BUCKET_DATA, "test/minio_test_upload.txt", expires_hours=1)

    print("\n=== 生成预签名上传 URL ===")
    generate_presigned_upload_url(client, BUCKET_DATA, "test/new_file.txt", expires_hours=1)

    # 下载文件
    download_path = "/tmp/minio_test_download.txt"
    print("\n=== 下载文件 ===")
    download_file(client, BUCKET_DATA, "test/minio_test_upload.txt", download_path)

    # 删除测试文件
    print("\n=== 删除测试对象 ===")
    delete_object(client, BUCKET_DATA, "test/minio_test_upload.txt")
    delete_object(client, BUCKET_ASSETS, "test/test_upload.txt")

    # 清理本地文件
    for f in [test_file, download_path]:
        if os.path.exists(f):
            os.remove(f)

    # 再次列出确认已删除
    list_objects(client, BUCKET_DATA)

    print("=" * 50)
    print(" 演示完成！")
    print("=" * 50)


if __name__ == "__main__":
    main()
