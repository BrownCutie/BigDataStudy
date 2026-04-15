# 04 - Python SDK 编程：用代码操作对象存储

## 一、课前回顾

在上一节 [03-mc命令行工具.md](03-mc命令行工具.md) 中，我们系统学习了 mc 命令行工具的使用，掌握了 Bucket 管理、文件操作、用户管理、策略配置等常用操作。本节我们将学习如何用 Python 代码来操作 MinIO，这是实际开发中最常用的方式。

## 二、两种 Python SDK 对比

Python 操作 MinIO 有两个主要的 SDK，选型时需要了解它们的区别：

```
┌─────────────────────────────────────────────────────────────────────┐
│                   两种 Python SDK 对比                                │
│                                                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────┐        │
│  │     minio-py（官方）      │  │     boto3（AWS 官方）     │        │
│  │                          │  │                          │        │
│  │  pip install minio       │  │  pip install boto3       │        │
│  │                          │  │                          │        │
│  │  优点：                   │  │  优点：                   │        │
│  │  - MinIO 官方维护         │  │  - AWS 官方维护           │        │
│  │  - API 设计简洁           │  │  - S3 标准接口            │        │
│  │  - 支持分片上传           │  │  - 生态最丰富             │        │
│  │  - 专门的 fput_object     │  │  - 迁移 AWS 无需改代码    │        │
│  │                          │  │                          │        │
│  │  缺点：                   │  │  缺点：                   │        │
│  │  - 只能用 MinIO           │  │  - 需要额外配置           │        │
│  │  - 社区较小               │  │  - API 较复杂             │        │
│  │                          │  │                          │        │
│  │  适用：专门用 MinIO       │  │  适用：可能迁移到 AWS S3  │        │
│  └──────────────────────────┘  └──────────────────────────┘        │
│                                                                     │
│  本节主要讲解 minio-py，也会介绍 boto3 的用法                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 三、minio-py SDK 安装与连接

### 3.1 安装

```bash
pip3 install minio
```

### 3.2 连接 MinIO

```python
from minio import Minio
from minio.error import S3Error

# 创建 MinIO 客户端
client = Minio(
    endpoint="localhost:9002",       # API 端点
    access_key="minioadmin",          # 用户名
    secret_key="minioadmin123",       # 密码
    secure=False                      # 本地开发使用 HTTP，不用 HTTPS
)

# 测试连接
try:
    buckets = client.list_buckets()
    for bucket in buckets:
        print(f"Bucket: {bucket.name}, 创建时间: {bucket.creation_date}")
    print("连接成功！")
except S3Error as e:
    print(f"连接失败: {e}")
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    连接参数说明                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  endpoint  : MinIO 的 API 地址（不含协议前缀）                   │
│              本地开发 → "localhost:9002"                         │
│              远程服务器 → "192.168.1.100:9002"                   │
│              生产环境 → "minio.example.com"                     │
│                                                                 │
│  access_key: 访问密钥（用户名）                                  │
│  secret_key: 密钥（密码）                                        │
│  secure    : 是否使用 HTTPS                                      │
│              本地开发用 False                                     │
│              生产环境用 True                                      │
│                                                                 │
│  region    : 区域（可选，默认 us-east-1）                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 四、Bucket 操作

### 4.1 创建 Bucket

```python
from minio.error import S3Error

def create_bucket(bucket_name):
    """创建存储桶"""
    try:
        # 先检查桶是否已存在
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' 创建成功")
        else:
            print(f"Bucket '{bucket_name}' 已存在")
    except S3Error as e:
        print(f"创建失败: {e}")

# 使用
create_bucket("demo-assets")
create_bucket("demo-data")
```

### 4.2 列出所有 Bucket

```python
def list_buckets():
    """列出所有存储桶"""
    buckets = client.list_buckets()
    for bucket in buckets:
        print(f"  {bucket.name}  (创建于 {bucket.creation_date})")

list_buckets()
```

### 4.3 删除 Bucket

```python
def delete_bucket(bucket_name):
    """删除存储桶（桶必须为空）"""
    try:
        client.remove_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' 删除成功")
    except S3Error as e:
        print(f"删除失败: {e}")

# 注意：只能删除空的 Bucket
# 如果桶中有文件，需要先删除所有文件
```

## 五、文件上传

### 5.1 普通上传（小文件）

```python
def upload_file(bucket_name, object_name, file_path, content_type=None):
    """
    上传文件到 MinIO

    参数:
        bucket_name: 存储桶名称
        object_name: 对象键（MinIO 中的文件名）
        file_path:   本地文件路径
        content_type: MIME 类型（可选）
    """
    try:
        result = client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type
        )
        print(f"上传成功: {result.object_name}")
        print(f"  ETag: {result.etag}")
        print(f"  Version ID: {result.version_id}")
    except S3Error as e:
        print(f"上传失败: {e}")

# 上传一张数据平台封面图片
upload_file(
    bucket_name="demo-assets",
    object_name="images/posts/spark-overview-cover.jpg",
    file_path="/tmp/spark-cover.jpg",
    content_type="image/jpeg"
)
```

### 5.2 从内存上传（适合程序生成的数据）

```python
from io import BytesIO

def upload_from_memory(bucket_name, object_name, data, content_type="text/plain"):
    """从内存直接上传数据"""
    try:
        data_bytes = data.encode("utf-8") if isinstance(data, str) else data
        result = client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(data_bytes),
            length=len(data_bytes),
            content_type=content_type
        )
        print(f"上传成功: {result.object_name}")
    except S3Error as e:
        print(f"上传失败: {e}")

# 上传一段 JSON 配置
import json
config = {"theme": "dark", "language": "zh-CN"}
upload_from_memory(
    bucket_name="demo-assets",
    object_name="config/theme.json",
    data=json.dumps(config, ensure_ascii=False),
    content_type="application/json"
)
```

### 5.3 分片上传（大文件）

```
┌─────────────────────────────────────────────────────────────────┐
│                    分片上传原理                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  小文件（< 5MB）：直接一次上传                                    │
│                                                                 │
│  大文件（> 5MB）：自动分片上传                                    │
│                                                                 │
│  原始文件（100MB）                                               │
│  ┌──────────────────────────────────────────┐                   │
│  │ Part 1 (5MB) │ Part 2 (5MB) │ ... │ Part 20 │              │
│  └──────┬───────┴──────┬───────┴─────┴────┬───┘                │
│         │              │                 │                     │
│         ▼              ▼                 ▼                     │
│  ┌──────────┐  ┌──────────┐       ┌──────────┐               │
│  │ Upload 1 │  │ Upload 2 │  ...  │ Upload N │               │
│  └──────────┘  └──────────┘       └──────────┘               │
│         │              │                 │                     │
│         ▼              ▼                 ▼                     │
│  ┌──────────────────────────────────────────┐                   │
│  │         Complete Multipart Upload         │                   │
│  │         合并所有分片，生成最终对象          │                   │
│  └──────────────────────────────────────────┘                   │
│                                                                 │
│  minio-py 会在文件 >= 5MB 时自动启用分片上传                     │
│  你也可以使用 fput_object 的高级参数控制分片大小                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```python
def upload_large_file(bucket_name, object_name, file_path, part_size=10*1024*1024):
    """
    分片上传大文件

    参数:
        part_size: 每个分片的大小（默认 10MB）
    """
    try:
        result = client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            part_size=part_size,       # 分片大小
            num_parallel_uploads=3     # 并行上传线程数
        )
        print(f"大文件上传成功: {result.object_name}")
    except S3Error as e:
        print(f"上传失败: {e}")

# 上传一个大的备份文件
upload_large_file(
    bucket_name="demo-data",
    object_name="exports/full-backup-2024.tar.gz",
    file_path="/tmp/backup.tar.gz",
    part_size=16*1024*1024  # 16MB 分片
)
```

## 六、文件下载

### 6.1 下载到本地文件

```python
def download_file(bucket_name, object_name, file_path):
    """下载文件到本地"""
    try:
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path
        )
        print(f"下载成功: {file_path}")
    except S3Error as e:
        print(f"下载失败: {e}")

# 下载数据平台封面图片
download_file(
    bucket_name="demo-assets",
    object_name="images/posts/spark-overview-cover.jpg",
    file_path="/tmp/downloaded-cover.jpg"
)
```

### 6.2 下载到内存

```python
def download_to_memory(bucket_name, object_name):
    """下载文件到内存"""
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except S3Error as e:
        print(f"下载失败: {e}")
        return None

# 读取 JSON 配置
import json
data = download_to_memory("demo-assets", "config/theme.json")
if data:
    config = json.loads(data.decode("utf-8"))
    print(f"主题配置: {config}")
```

## 七、文件列表与查询

### 7.1 列出所有对象

```python
def list_objects(bucket_name, prefix=""):
    """列出 Bucket 中的对象"""
    objects = client.list_objects(bucket_name, prefix=prefix)
    for obj in objects:
        print(f"  {obj.object_name}  ({obj.size} bytes, 修改于 {obj.last_modified})")

# 列出所有文件
list_objects("demo-assets")

# 只列出 images 目录下的文件
list_objects("demo-assets", prefix="images/")
```

### 7.2 检查文件是否存在

```python
def object_exists(bucket_name, object_name):
    """检查对象是否存在"""
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise

# 使用
if object_exists("demo-assets", "images/posts/cover.jpg"):
    print("文件存在")
else:
    print("文件不存在")
```

## 八、预签名 URL（Presigned URL）

这是对象存储中非常实用的功能——生成一个带签名的临时 URL，允许没有账号的人临时访问文件。

```
┌─────────────────────────────────────────────────────────────────┐
│                    预签名 URL 的工作原理                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  场景：你想分享一个私有文件给朋友，但不想公开整个 Bucket             │
│                                                                 │
│  方案 1：设置 Bucket 为公开（不安全，所有人都能访问）              │
│                                                                 │
│  方案 2：使用预签名 URL（推荐）                                   │
│                                                                 │
│  ┌──────────┐     生成预签名 URL      ┌──────────────┐         │
│  │  Python  │ ─────────────────────→ │  临时 URL     │         │
│  │  后端    │                         │  (7天后过期)  │         │
│  └──────────┘                         └──────┬───────┘         │
│                                              │                  │
│                                              │ 发送给朋友        │
│                                              ▼                  │
│                                       ┌──────────────┐         │
│                                       │  朋友打开 URL │         │
│                                       │  直接下载文件  │         │
│                                       └──────────────┘         │
│                                                                 │
│  URL 中包含签名信息，MinIO 会验证签名是否有效                    │
│  签名过期后，URL 自动失效                                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```python
from datetime import timedelta

def generate_download_url(bucket_name, object_name, expires=timedelta(hours=1)):
    """生成预签名下载 URL"""
    try:
        url = client.presigned_get_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=expires
        )
        return url
    except S3Error as e:
        print(f"生成 URL 失败: {e}")
        return None

def generate_upload_url(bucket_name, object_name, expires=timedelta(hours=1)):
    """生成预签名上传 URL（允许别人上传文件到你的 Bucket）"""
    try:
        url = client.presigned_put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            expires=expires
        )
        return url
    except S3Error as e:
        print(f"生成 URL 失败: {e}")
        return None

# 生成一个 1 小时有效的下载链接
url = generate_download_url("demo-assets", "images/posts/cover.jpg")
print(f"下载链接: {url}")
print(f"链接 1 小时后过期")

# 生成一个 7 天有效的下载链接
url = generate_download_url(
    "demo-assets",
    "attachments/whitepaper.pdf",
    expires=timedelta(days=7)
)
print(f"7天有效链接: {url}")

# 生成上传链接（前端可以直接用这个 URL 上传文件，不需要后端中转）
url = generate_upload_url("demo-assets", "images/new-upload.jpg")
print(f"上传链接: {url}")
```

## 九、删除操作

```python
def delete_object(bucket_name, object_name):
    """删除单个对象"""
    try:
        client.remove_object(bucket_name, object_name)
        print(f"删除成功: {object_name}")
    except S3Error as e:
        print(f"删除失败: {e}")

def delete_objects(bucket_name, object_list):
    """批量删除多个对象"""
    try:
        errors = client.remove_objects(bucket_name, object_list)
        for error in errors:
            print(f"删除失败: {error}")
        print(f"批量删除完成，共 {len(object_list)} 个对象")
    except S3Error as e:
        print(f"批量删除失败: {e}")

# 删除单个文件
delete_object("demo-assets", "test-upload.txt")

# 批量删除
delete_objects("demo-assets", [
    "images/posts/old-cover-1.jpg",
    "images/posts/old-cover-2.jpg",
    "temp/file.txt"
])
```

## 十、数据分析项目实战：批量上传脚本

下面是一个完整的 Python 脚本，用于批量上传数据文件到 MinIO：

```python
#!/usr/bin/env python3
"""
数据文件批量上传工具

功能：
1. 扫描本地图片目录
2. 按日期组织上传到 MinIO
3. 自动生成预签名 URL 列表
4. 生成 Markdown 格式的图片链接
"""

import os
import sys
from datetime import timedelta
from pathlib import Path
from minio import Minio
from minio.error import S3Error

# ===== 配置 =====
MINIO_ENDPOINT = "localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME = "demo-assets"
IMAGE_DIR = "/tmp/demo-images"          # 本地图片目录
OUTPUT_MD_FILE = "/tmp/uploaded-images.md"  # 输出的 Markdown 文件
URL_EXPIRES = timedelta(days=365)       # URL 有效期（1年）

# ===== 初始化客户端 =====
client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def ensure_bucket():
    """确保 Bucket 存在"""
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"[OK] 创建 Bucket: {BUCKET_NAME}")
    else:
        print(f"[OK] Bucket 已存在: {BUCKET_NAME}")


def upload_image(local_path, object_name):
    """上传单个图片"""
    # 根据文件扩展名设置 Content-Type
    ext = Path(local_path).suffix.lower()
    content_types = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".svg": "image/svg+xml",
    }
    content_type = content_types.get(ext, "application/octet-stream")

    try:
        result = client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            file_path=local_path,
            content_type=content_type
        )
        print(f"[OK] 上传: {object_name} ({result.etag})")
        return True
    except S3Error as e:
        print(f"[ERR] 上传失败 {object_name}: {e}")
        return False


def generate_url(object_name):
    """生成预签名 URL"""
    try:
        url = client.presigned_get_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            expires=URL_EXPIRES
        )
        return url
    except S3Error as e:
        print(f"[ERR] 生成 URL 失败 {object_name}: {e}")
        return None


def main():
    """主函数"""
    print("=" * 60)
    print("数据文件批量上传工具")
    print("=" * 60)

    # 1. 确保 Bucket 存在
    ensure_bucket()

    # 2. 扫描本地图片
    image_dir = Path(IMAGE_DIR)
    if not image_dir.exists():
        print(f"[ERR] 目录不存在: {IMAGE_DIR}")
        sys.exit(1)

    image_files = []
    for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]:
        image_files.extend(image_dir.rglob(f"*{ext}"))

    if not image_files:
        print(f"[WARN] 未找到图片文件: {IMAGE_DIR}")
        sys.exit(0)

    print(f"\n找到 {len(image_files)} 个图片文件\n")

    # 3. 上传并生成 URL
    results = []
    for img_path in image_files:
        # 按相对路径组织 Key
        rel_path = img_path.relative_to(image_dir)
        object_name = f"images/{rel_path}"

        # 上传
        if upload_image(str(img_path), str(object_name)):
            # 生成 URL
            url = generate_url(str(object_name))
            if url:
                results.append({
                    "name": img_path.name,
                    "object_name": str(object_name),
                    "url": url
                })

    # 4. 生成 Markdown 文件
    print(f"\n{'=' * 60}")
    print(f"上传完成: {len(results)}/{len(image_files)}")
    print(f"{'=' * 60}\n")

    with open(OUTPUT_MD_FILE, "w", encoding="utf-8") as f:
        f.write("# 数据文件上传记录\n\n")
        f.write(f"上传时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write("| 文件名 | 对象键 | URL |\n")
        f.write("|--------|--------|-----|\n")
        for r in results:
            f.write(f"| {r['name']} | `{r['object_name']}` | [查看]({r['url']}) |\n")

    print(f"Markdown 记录已保存: {OUTPUT_MD_FILE}")


if __name__ == "__main__":
    main()
```

### 运行脚本

```bash
# 准备测试图片
mkdir -p /tmp/demo-images/posts
echo "fake image 1" > /tmp/demo-images/posts/spark-arch.jpg
echo "fake image 2" > /tmp/demo-images/posts/flink-pipeline.png

# 运行上传脚本
python3 upload_demo_images.py
```

## 十一、boto3 方式操作 MinIO

如果你更熟悉 boto3（AWS SDK），也可以用它来操作 MinIO：

```python
from boto3 import client
from botocore.client import Config

# 创建 S3 客户端（连接 MinIO）
s3 = client(
    's3',
    endpoint_url='http://localhost:9002',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# 创建 Bucket
s3.create_bucket(Bucket='test-boto3')

# 上传文件
s3.upload_file('/tmp/test.txt', 'test-boto3', 'test.txt')

# 下载文件
s3.download_file('test-boto3', 'test.txt', '/tmp/downloaded.txt')

# 列出对象
response = s3.list_objects_v2(Bucket='test-boto3')
for obj in response.get('Contents', []):
    print(f"  {obj['Key']} ({obj['Size']} bytes)")

# 生成预签名 URL
url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'test-boto3', 'Key': 'test.txt'},
    ExpiresIn=3600
)
print(f"预签名 URL: {url}")

# 删除对象
s3.delete_object(Bucket='test-boto3', Key='test.txt')
```

**minio-py 和 boto3 的选择建议：**

```
┌─────────────────────────────────────────────────────────────────┐
│                    SDK 选择建议                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  选 minio-py：                                                  │
│  - 你的项目只用 MinIO，不打算迁移到 AWS                          │
│  - 需要 minio-py 的特有功能（如 admin 管理接口）                  │
│  - 喜欢简洁的 API 风格                                          │
│                                                                 │
│  选 boto3：                                                     │
│  - 可能需要迁移到 AWS S3 或其他云厂商                            │
│  - 团队已经熟悉 boto3                                           │
│  - Spark/Flink 等 Java 生态也需要 S3 兼容接口                   │
│                                                                 │
│  我们数据分析项目选择 minio-py（简洁、够用）                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 十二、面试题

### 题目：预签名 URL 的原理是什么？如何保证安全性？

**参考答案：**

预签名 URL 是将访问凭证（签名）嵌入到 URL 中，接收方无需拥有 MinIO 账号就能访问资源。

**工作原理：**
1. 服务端使用 Access Key/Secret Key 对请求信息（Bucket、Key、过期时间等）进行 HMAC-SHA256 签名
2. 签名结果作为查询参数附加到 URL 上
3. 客户端拿着这个 URL 访问 MinIO，MinIO 用相同的算法验证签名
4. 验证通过且未过期，则允许访问

**安全保证：**
- URL 有过期时间，过期后自动失效
- 签名使用了 Secret Key，无法伪造
- 可以限制为只读（GET）或只写（PUT），防止越权操作
- 可以设置较短的过期时间来降低风险

### 题目：大文件上传时，分片上传有什么优势？分片大小应该如何设置？

**参考答案：**

**分片上传的优势：**
1. **断点续传**：某个分片上传失败只需重传该分片，不需要重传整个文件
2. **并行上传**：多个分片可以同时上传，提高速度
3. **内存友好**：不需要将整个文件加载到内存
4. **网络容错**：网络中断后可以从上次断点继续

**分片大小建议：**
- 默认 5MB，推荐 5MB ~ 100MB
- 太小：分片数量过多，管理开销大
- 太大：单次传输失败重传代价高
- 根据网络带宽和文件大小选择：
  - 100MB 文件 → 5-10MB 分片
  - 1GB 文件 → 10-50MB 分片
  - 10GB+ 文件 → 50-100MB 分片

---

> **上一篇：[03-mc命令行工具.md](03-mc命令行工具.md) — mc 命令行工具详解**
>
> **下一篇：[05-进阶特性.md](05-进阶特性.md) — 进阶特性**
