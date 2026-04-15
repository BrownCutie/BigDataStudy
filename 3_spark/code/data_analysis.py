#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
博客数据分析脚本
功能：日活统计、热门文章排行、用户行为分析
结果输出到控制台和 CSV
兼容：Python 3.9 + PySpark 3.5.8
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_date, to_timestamp, when, lower,
    count as _count, sum as _sum, avg as _avg,
    max as _max, min as _min, countDistinct, round as _round,
    desc, asc, lit, split, size, regexp_replace,
    collect_list, first as _first, array_distinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    StringType, DateType, DoubleType
)


def create_spark_session(app_name="DemoAnalysis"):
    """
    创建 SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark 版本: {spark.version}")
    return spark


def load_data(spark, data_dir):
    """
    加载 CSV 数据
    """
    posts_csv = os.path.join(data_dir, "posts.csv")
    access_logs_csv = os.path.join(data_dir, "access_logs.csv")

    # 文章数据
    posts_schema = StructType([
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

    posts_df = spark.read.csv(
        posts_csv, header=True, schema=posts_schema, encoding="UTF-8"
    )
    posts_df = posts_df.withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd"))

    # 访问日志数据
    logs_schema = StructType([
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

    logs_df = spark.read.csv(
        access_logs_csv, header=True, schema=logs_schema, encoding="UTF-8"
    )
    logs_df = logs_df.withColumn("event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    logs_df = logs_df.withColumn("event_date", to_date(col("event_time")))
    logs_df = logs_df.withColumn("action", lower(trim(col("action"))))

    print(f"[加载] 文章数据: {posts_df.count()} 条")
    print(f"[加载] 日志数据: {logs_df.count()} 条")

    return posts_df, logs_df


def analyze_daily_active_users(logs_df, output_dir):
    """
    分析 1：日活统计（DAU）
    - 每日活跃用户数
    - 每日活跃用户明细
    """
    print("\n" + "=" * 60)
    print("分析 1: 日活统计（DAU）")
    print("=" * 60)

    # 注册临时视图
    logs_df.createOrReplaceTempView("access_logs")

    # 每日活跃用户数
    dau = spark.sql("""
        SELECT
            event_date              AS 日期,
            COUNT(DISTINCT user_id) AS 日活用户数,
            COUNT(*)                AS 总行为数,
            COUNT(DISTINCT post_id) AS 被访问文章数,
            ROUND(AVG(duration), 1) AS 平均停留时长_秒
        FROM access_logs
        WHERE action = 'view'
        GROUP BY event_date
        ORDER BY event_date
    """)

    dau.show(truncate=False)
    dau.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "dau_stats"), encoding="UTF-8")

    # 活跃用户行为分布
    user_actions = spark.sql("""
        SELECT
            user_id        AS 用户ID,
            COUNT(*)        AS 行为数,
            COUNT(DISTINCT post_id) AS 访问文章数,
            COUNT(DISTINCT event_date) AS 活跃天数,
            ROUND(AVG(duration), 1) AS 平均停留时长_秒
        FROM access_logs
        GROUP BY user_id
        ORDER BY 行为数 DESC
    """)

    user_actions.show(truncate=False)
    user_actions.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "user_actions"), encoding="UTF-8")

    return dau


def analyze_hot_posts(posts_df, logs_df, output_dir):
    """
    分析 2：热门文章排行
    - 浏览量 Top 10
    - 互动量 Top 10
    - 日均浏览量趋势
    """
    print("\n" + "=" * 60)
    print("分析 2: 热门文章排行")
    print("=" * 60)

    posts_df.createOrReplaceTempView("posts")
    logs_df.createOrReplaceTempView("access_logs")

    # 浏览量 Top 10
    top_views = spark.sql("""
        SELECT
            id    AS 文章ID,
            title AS 标题,
            author AS 作者,
            category AS 分类,
            views AS 浏览量,
            likes AS 点赞数,
            comments AS 评论数,
            publish_date AS 发布日期,
            popularity AS 热度
        FROM posts
        ORDER BY views DESC
        LIMIT 10
    """)

    top_views.show(truncate=False)
    top_views.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "top_views"), encoding="UTF-8")

    # 互动量 Top 10（浏览 + 点赞*5 + 评论*10）
    top_engagement = spark.sql("""
        SELECT
            id,
            title,
            author,
            category,
            views,
            likes,
            comments,
            (views + likes * 5 + comments * 10) AS 互动量,
            popularity
        FROM posts
        ORDER BY 互动量 DESC
        LIMIT 10
    """)

    top_engagement.show(truncate=False)
    top_engagement.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "top_engagement"), encoding="UTF-8")

    # 按日志统计实际访问热度 Top 10
    top_visited = spark.sql("""
        SELECT
            p.id    AS 文章ID,
            p.title AS 标题,
            p.author AS 作者,
            COUNT(*) AS 访问次数,
            COUNT(DISTINCT l.user_id) AS 独立访客数,
            ROUND(AVG(l.duration), 1) AS 平均停留时长_秒
        FROM access_logs l
        INNER JOIN posts p ON l.post_id = p.id
        WHERE l.action = 'view'
        GROUP BY p.id, p.title, p.author
        ORDER BY 访问次数 DESC
        LIMIT 10
    """)

    top_visited.show(truncate=False)
    top_visited.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "top_visited"), encoding="UTF-8")

    return top_views, top_engagement, top_visited


def analyze_user_behavior(posts_df, logs_df, output_dir):
    """
    分析 3：用户行为分析
    - 行为类型分布（view/like/comment/share/bookmark）
    - 设备分布
    - 浏览器分布
    - 操作系统分布
    - 用户活跃时段分布
    - 作者维度统计
    """
    print("\n" + "=" * 60)
    print("分析 3: 用户行为分析")
    print("=" * 60)

    posts_df.createOrReplaceTempView("posts")
    logs_df.createOrReplaceTempView("access_logs")

    # 行为类型分布
    action_dist = spark.sql("""
        SELECT
            action   AS 行为类型,
            COUNT(*) AS 次数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM access_logs), 2) AS 占比百分比
        FROM access_logs
        GROUP BY action
        ORDER BY 次数 DESC
    """)

    print("--- 行为类型分布 ---")
    action_dist.show(truncate=False)
    action_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "action_distribution"), encoding="UTF-8")

    # 设备分布
    device_dist = spark.sql("""
        SELECT
            device   AS 设备类型,
            COUNT(*) AS 次数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM access_logs), 2) AS 占比百分比
        FROM access_logs
        GROUP BY device
        ORDER BY 次数 DESC
    """)

    print("--- 设备分布 ---")
    device_dist.show(truncate=False)
    device_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "device_distribution"), encoding="UTF-8")

    # 浏览器分布
    browser_dist = spark.sql("""
        SELECT
            browser  AS 浏览器,
            COUNT(*) AS 次数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM access_logs), 2) AS 占比百分比
        FROM access_logs
        GROUP BY browser
        ORDER BY 次数 DESC
    """)

    print("--- 浏览器分布 ---")
    browser_dist.show(truncate=False)
    browser_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "browser_distribution"), encoding="UTF-8")

    # 操作系统分布
    os_dist = spark.sql("""
        SELECT
            os       AS 操作系统,
            COUNT(*) AS 次数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM access_logs), 2) AS 占比百分比
        FROM access_logs
        GROUP BY os
        ORDER BY 次数 DESC
    """)

    print("--- 操作系统分布 ---")
    os_dist.show(truncate=False)
    os_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "os_distribution"), encoding="UTF-8")

    # 用户活跃时段分布
    hour_dist = spark.sql("""
        SELECT
            HOUR(event_time)       AS 小时,
            COUNT(*)               AS 行为数,
            COUNT(DISTINCT user_id) AS 活跃用户数
        FROM access_logs
        WHERE action = 'view'
        GROUP BY HOUR(event_time)
        ORDER BY 小时
    """)

    print("--- 用户活跃时段分布 ---")
    hour_dist.show(24, truncate=False)
    hour_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "hourly_activity"), encoding="UTF-8")

    # 作者综合统计
    author_stats = spark.sql("""
        SELECT
            author              AS 作者,
            COUNT(*)            AS 文章数,
            SUM(views)          AS 总浏览量,
            ROUND(AVG(views), 2) AS 平均浏览量,
            MAX(views)          AS 最高浏览量,
            SUM(likes)          AS 总点赞数,
            SUM(comments)       AS 总评论数,
            COUNT(DISTINCT category) AS 涉及分类数,
            CASE
                WHEN SUM(views) >= 8000 THEN '头部作者'
                WHEN SUM(views) >= 3000 THEN '腰部作者'
                ELSE '新晋作者'
            END                 AS 作者等级
        FROM posts
        GROUP BY author
        ORDER BY 总浏览量 DESC
    """)

    print("--- 作者综合统计 ---")
    author_stats.show(truncate=False)
    author_stats.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "author_stats"), encoding="UTF-8")

    # 分类统计
    category_stats = spark.sql("""
        SELECT
            category            AS 分类,
            COUNT(*)            AS 文章数,
            SUM(views)          AS 总浏览量,
            ROUND(AVG(views), 2) AS 平均浏览量,
            SUM(likes)          AS 总点赞数
        FROM posts
        GROUP BY category
        ORDER BY 总浏览量 DESC
    """)

    print("--- 分类统计 ---")
    category_stats.show(truncate=False)
    category_stats.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "category_stats"), encoding="UTF-8")

    return action_dist, device_dist, author_stats


def analyze_user_conversion(logs_df, output_dir):
    """
    分析 4：用户转化漏斗
    - 浏览 -> 点赞 -> 评论 -> 分享 转化率
    """
    print("\n" + "=" * 60)
    print("分析 4: 用户转化漏斗")
    print("=" * 60)

    logs_df.createOrReplaceTempView("access_logs")

    funnel = spark.sql("""
        SELECT
            COUNT(DISTINCT CASE WHEN action = 'view' THEN user_id END)     AS 浏览用户数,
            COUNT(DISTINCT CASE WHEN action = 'like' THEN user_id END)     AS 点赞用户数,
            COUNT(DISTINCT CASE WHEN action = 'comment' THEN user_id END)  AS 评论用户数,
            COUNT(DISTINCT CASE WHEN action = 'share' THEN user_id END)    AS 分享用户数,
            COUNT(DISTINCT CASE WHEN action = 'bookmark' THEN user_id END) AS 收藏用户数,
            COUNT(DISTINCT user_id)                                        AS 总用户数
        FROM access_logs
    """)

    funnel.show(truncate=False)

    # 转化率计算（使用 Python 收集后计算，因为需要跨列引用）
    row = funnel.collect()[0]
    view_users = row["浏览用户数"] or 0
    like_users = row["点赞用户数"] or 0
    comment_users = row["评论用户数"] or 0
    share_users = row["分享用户数"] or 0
    bookmark_users = row["收藏用户数"] or 0
    total_users = row["总用户数"] or 1

    print(f"\n--- 转化漏斗 ---")
    print(f"  浏览用户数: {view_users}")
    print(f"  点赞用户数: {like_users} (转化率: {like_users/view_users*100:.1f}%)" if view_users > 0 else "  点赞用户数: 0")
    print(f"  评论用户数: {comment_users} (转化率: {comment_users/view_users*100:.1f}%)" if view_users > 0 else "  评论用户数: 0")
    print(f"  分享用户数: {share_users} (转化率: {share_users/view_users*100:.1f}%)" if view_users > 0 else "  分享用户数: 0")
    print(f"  收藏用户数: {bookmark_users} (转化率: {bookmark_users/view_users*100:.1f}%)" if view_users > 0 else "  收藏用户数: 0")

    # 写入 CSV
    funnel.write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "conversion_funnel"), encoding="UTF-8")


def analyze_tags(posts_df, output_dir):
    """
    分析 5：标签分析
    - 热门标签统计
    """
    print("\n" + "=" * 60)
    print("分析 5: 标签分析")
    print("=" * 60)

    # 展开标签
    from pyspark.sql.functions import explode

    tag_stats = posts_df \
        .filter(col("tags").isNotNull() & (col("tags") != "")) \
        .withColumn("tag", explode(split(lower(col("tags")), ","))) \
        .filter(col("tag") != "") \
        .groupBy("tag") \
        .agg(
            _count("*").alias("使用次数"),
            _sum("views").alias("关联浏览量"),
            _round(_avg("views"), 2).alias("平均浏览量"),
            collect_list("title").alias("关联文章")
        ) \
        .orderBy(desc("使用次数"))

    tag_stats.select("tag", "使用次数", "关联浏览量", "平均浏览量").show(truncate=False)

    tag_stats.select("tag", "使用次数", "关联浏览量", "平均浏览量") \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_dir, "tag_stats"), encoding="UTF-8")


def main():
    """主函数"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    output_dir = os.path.join(base_dir, "output", datetime.now().strftime("%Y%m%d_%H%M%S"))

    # 检查博客图片
    for f in ["posts.csv", "access_logs.csv"]:
        if not os.path.exists(os.path.join(data_dir, f)):
            print(f"[错误] 博客图片不存在: {os.path.join(data_dir, f)}")
            sys.exit(1)

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 创建 SparkSession
    spark = create_spark_session()

    try:
        # 加载数据
        print("\n" + "=" * 60)
        print("加载数据")
        print("=" * 60)
        posts_df, logs_df = load_data(spark, data_dir)

        # 执行各项分析
        analyze_daily_active_users(logs_df, output_dir)
        analyze_hot_posts(posts_df, logs_df, output_dir)
        analyze_user_behavior(posts_df, logs_df, output_dir)
        analyze_user_conversion(logs_df, output_dir)
        analyze_tags(posts_df, output_dir)

        print("\n" + "=" * 60)
        print(f"所有分析完成! 结果已保存到: {output_dir}")
        print("=" * 60)

    finally:
        spark.stop()
        print("[关闭] SparkSession 已停止")


if __name__ == "__main__":
    main()
