#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
博客数据 ETL 脚本
功能：读取 CSV 数据 -> 数据清洗 -> 写入 Hive 表
兼容：Python 3.9 + PySpark 3.5.8
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_date, when, lower, regexp_replace,
    split, size, length, lit, year, month
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    StringType, DateType, DoubleType
)


def create_spark_session(app_name="DemoETL"):
    """
    创建 SparkSession，启用 Hive 支持
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别为 WARN，减少输出噪音
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark 版本: {spark.version}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


def read_posts_csv(spark, csv_path):
    """
    读取博客文章 CSV 数据
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("category", StringType(), True),
        StructField("views", LongType(), True),
        StructField("likes", IntegerType(), True),
        StructField("comments", IntegerType(), True),
        StructField("publish_date", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("status", StringType(), True),
    ])

    df = spark.read.csv(
        csv_path,
        header=True,
        schema=schema,
        encoding="UTF-8",
        mode="PERMISSIVE",       # 遇到错误行保留为 null，不中断
        columnNameOfCorruptRecord="_corrupt_record"
    )

    print(f"[读取] 文章数据: {df.count()} 条, {len(df.columns)} 列")
    return df


def read_access_logs_csv(spark, csv_path):
    """
    读取访问日志 CSV 数据
    """
    schema = StructType([
        StructField("log_id", IntegerType(), False),
        StructField("user_id", StringType(), True),
        StructField("post_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("timestamp", StringType(), True),
    ])

    df = spark.read.csv(
        csv_path,
        header=True,
        schema=schema,
        encoding="UTF-8",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        mode="PERMISSIVE",
        columnNameOfCorruptRecord="_corrupt_record"
    )

    print(f"[读取] 日志数据: {df.count()} 条, {len(df.columns)} 列")
    return df


def clean_posts(df):
    """
    数据清洗：博客文章数据
    - 去除空值（关键字段）
    - 去除前后空格
    - 格式转换（日期、数值）
    - 处理异常值
    - 新增衍生列
    """
    print("[清洗] 开始清洗文章数据...")

    # Step 1：去除损坏的记录
    if "_corrupt_record" in df.columns:
        df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Step 2：去除空标题和空作者的行
    df = df.filter(
        col("title").isNotNull() & (trim(col("title")) != "") &
        col("author").isNotNull() & (trim(col("author")) != "")
    )

    # Step 3：去除字符串列的前后空格
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))

    # Step 4：填充缺失的数值为 0
    df = df.fillna({
        "views": 0,
        "likes": 0,
        "comments": 0
    })

    # Step 5：处理异常值（负数浏览量设为 0）
    df = df.withColumn("views",
        when(col("views") < 0, 0).otherwise(col("views"))
    )
    df = df.withColumn("likes",
        when(col("likes") < 0, 0).otherwise(col("likes"))
    )

    # Step 6：转换日期类型
    df = df.withColumn("publish_date",
        to_date(col("publish_date"), "yyyy-MM-dd")
    )

    # Step 7：过滤无效日期
    df = df.filter(col("publish_date").isNotNull())

    # Step 8：新增衍生列
    # 年月（用于分区）
    df = df.withColumn("year", year(col("publish_date")))
    df = df.withColumn("month", month(col("publish_date")))

    # 标签数量
    df = df.withColumn("tag_count",
        when(col("tags").isNull() | (col("tags") == ""), 0)
        .otherwise(size(split(col("tags"), ",")))
    )

    # 互动量（浏览 + 点赞 + 评论）
    df = df.withColumn("engagement",
        col("views") + col("likes") * 5 + col("comments") * 10
    )

    # 热度等级
    df = df.withColumn("popularity",
        when(col("views") >= 3000, "热门")
        .when(col("views") >= 1500, "普通")
        .otherwise("冷门")
    )

    # 标签清理：去除空格，转小写
    df = df.withColumn("tags",
        when(col("tags").isNull() | (col("tags") == ""), lit(""))
        .otherwise(lower(regexp_replace(col("tags"), r"\s+", "")))
    )

    print(f"[清洗] 清洗后文章数据: {df.count()} 条")
    return df


def clean_access_logs(df):
    """
    数据清洗：访问日志数据
    - 去除空值
    - 格式转换
    - 过滤无效记录
    """
    print("[清洗] 开始清洗日志数据...")

    # 去除损坏的记录
    if "_corrupt_record" in df.columns:
        df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # 去除关键字段为空的记录
    df = df.filter(
        col("user_id").isNotNull() &
        col("post_id").isNotNull() &
        col("action").isNotNull()
    )

    # 转换时间戳
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # 过滤无效时间
    df = df.filter(col("event_time").isNotNull())

    # 新增日期列（用于分析）
    df = df.withColumn("event_date", to_date(col("event_time")))
    df = df.withColumn("event_hour",
        (df["event_time"].substr(12, 2)).cast(IntegerType())
    )

    # 去除前后空格
    for c in ["user_id", "action", "device", "browser", "os"]:
        df = df.withColumn(c, trim(col(c)))

    # action 标准化为小写
    df = df.withColumn("action", lower(col("action")))

    print(f"[清洗] 清洗后日志数据: {df.count()} 条")
    return df


def write_to_hive(spark, df_posts, df_logs, database="blog_db"):
    """
    将清洗后的数据写入 Hive 表
    """
    print(f"[写入] 开始写入 Hive 数据库: {database}")

    # 创建数据库
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    # 启用动态分区
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # ========== 写入文章表 ==========
    # 先删除旧表（如果存在）
    spark.sql(f"DROP TABLE IF EXISTS {database}.posts")

    # 创建分区表
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database}.posts (
            id          INT             COMMENT '文章ID',
            title       STRING          COMMENT '标题',
            author      STRING          COMMENT '作者',
            category    STRING          COMMENT '分类',
            views       BIGINT          COMMENT '浏览量',
            likes       INT             COMMENT '点赞数',
            comments    INT             COMMENT '评论数',
            publish_date DATE          COMMENT '发布日期',
            tags        STRING          COMMENT '标签（逗号分隔）',
            status      STRING          COMMENT '状态',
            tag_count   INT             COMMENT '标签数量',
            engagement  BIGINT          COMMENT '互动量',
            popularity  STRING          COMMENT '热度等级'
        )
        COMMENT '博客文章表（清洗后）'
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
    """)

    # 写入数据
    df_posts.select(
        "id", "title", "author", "category", "views", "likes", "comments",
        "publish_date", "tags", "status", "tag_count", "engagement", "popularity",
        "year", "month"
    ).write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .insertInto(f"{database}.posts")

    print(f"[写入] 文章表写入完成: {database}.posts")

    # ========== 写入访问日志表 ==========
    spark.sql(f"DROP TABLE IF EXISTS {database}.access_logs")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database}.access_logs (
            log_id     INT             COMMENT '日志ID',
            user_id    STRING          COMMENT '用户ID',
            post_id    INT             COMMENT '文章ID',
            action     STRING          COMMENT '行为类型',
            ip_address STRING          COMMENT 'IP地址',
            device     STRING          COMMENT '设备类型',
            browser    STRING          COMMENT '浏览器',
            os         STRING          COMMENT '操作系统',
            duration   INT             COMMENT '停留时长（秒）',
            event_time TIMESTAMP       COMMENT '事件时间',
            event_date DATE           COMMENT '事件日期',
            event_hour INT             COMMENT '事件小时'
        )
        COMMENT '访问日志表（清洗后）'
        STORED AS PARQUET
    """)

    df_logs.select(
        "log_id", "user_id", "post_id", "action", "ip_address",
        "device", "browser", "os", "duration", "event_time",
        "event_date", "event_hour"
    ).write \
        .mode("overwrite") \
        .insertInto(f"{database}.access_logs")

    print(f"[写入] 日志表写入完成: {database}.access_logs")

    # 验证写入结果
    print(f"\n[验证] 文章表行数: {spark.table(f'{database}.posts').count()}")
    print(f"[验证] 日志表行数: {spark.table(f'{database}.access_logs').count()}")

    # 展示文章表分区信息
    print("\n[验证] 文章表分区:")
    spark.sql(f"SHOW PARTITIONS {database}.posts").show(10)

    # 展示样例数据
    print("\n[验证] 文章表样例:")
    spark.sql(f"SELECT * FROM {database}.posts LIMIT 5").show(truncate=False)

    print("\n[验证] 日志表样例:")
    spark.sql(f"SELECT * FROM {database}.access_logs LIMIT 5").show(truncate=False)


def main():
    """主函数：ETL 流程入口"""
    # 获取数据目录（基于脚本所在位置）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    posts_csv = os.path.join(data_dir, "posts.csv")
    access_logs_csv = os.path.join(data_dir, "access_logs.csv")

    # 检查博客图片是否存在
    for f in [posts_csv, access_logs_csv]:
        if not os.path.exists(f):
            print(f"[错误] 博客图片不存在: {f}")
            sys.exit(1)

    # 创建 SparkSession
    spark = create_spark_session()

    try:
        # ====== 读取阶段 ======
        print("\n" + "=" * 60)
        print("阶段 1: 读取数据")
        print("=" * 60)

        df_posts = read_posts_csv(spark, posts_csv)
        df_logs = read_access_logs_csv(spark, access_logs_csv)

        # ====== 清洗阶段 ======
        print("\n" + "=" * 60)
        print("阶段 2: 数据清洗")
        print("=" * 60)

        df_posts_clean = clean_posts(df_posts)
        df_logs_clean = clean_access_logs(df_logs)

        # ====== 写入阶段 ======
        print("\n" + "=" * 60)
        print("阶段 3: 写入 Hive 表")
        print("=" * 60)

        write_to_hive(spark, df_posts_clean, df_logs_clean)

        print("\n" + "=" * 60)
        print("ETL 流程执行完成!")
        print("=" * 60)

    finally:
        spark.stop()
        print("[关闭] SparkSession 已停止")


if __name__ == "__main__":
    main()
