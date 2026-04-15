#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据平台 ClickHouse 数据分析脚本
功能：连接 ClickHouse、批量导入数据、执行分析查询、格式化输出结果
依赖：pip install clickhouse-driver
"""

import csv
import os
from datetime import datetime
from clickhouse_driver import Client


# ============================================================
# 配置信息
# ============================================================
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000  # Native TCP 端口（注意：可能与 Hadoop NameNode 冲突）
DATABASE = 'blog'
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def create_client():
    """创建 ClickHouse 客户端连接"""
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=DATABASE,
    )
    # 测试连接
    version = client.execute('SELECT version()')[0][0]
    print(f'[OK] 已连接 ClickHouse {version}')
    return client


def init_database(client):
    """初始化数据库（如果不存在则创建）"""
    client.execute('CREATE DATABASE IF NOT EXISTS demo')
    print('[OK] 数据库 demo 已就绪')


def import_access_logs(client, csv_path):
    """从 CSV 批量导入访问日志数据"""
    if not os.path.exists(csv_path):
        print(f'[跳过] 文件不存在: {csv_path}')
        return

    # 读取 CSV 数据
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            # 解析每一行数据，转换为 ClickHouse 支持的类型
            rows.append((
                int(row['log_id']),
                int(row['user_id']),
                int(row['post_id']),
                row['action'],
                row['ip'],
                row['city'],
                row['device'],
                datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S'),
            ))

    if not rows:
        print('[跳过] CSV 文件为空')
        return

    # 批量插入数据
    client.execute(
        'INSERT INTO demo.access_logs (log_id, user_id, post_id, action, ip, city, device, timestamp) VALUES',
        rows
    )
    print(f'[OK] 成功导入 {len(rows)} 条访问日志')


def import_demo_stats(client, csv_path):
    """从 CSV 导入数据平台每日汇总统计数据"""
    if not os.path.exists(csv_path):
        print(f'[跳过] 文件不存在: {csv_path}')
        return

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((
                datetime.strptime(row['date'], '%Y-%m-%d').date(),
                int(row['total_pv']),
                int(row['total_uv']),
                int(row['new_users']),
                int(row['new_posts']),
                int(row['total_comments']),
                int(row['total_likes']),
                float(row['bounce_rate']),
                int(row['avg_duration']),
            ))

    if not rows:
        print('[跳过] CSV 文件为空')
        return

    client.execute(
        'INSERT INTO demo.demo_daily_summary '
        '(date, total_pv, total_uv, new_users, new_posts, total_comments, total_likes, bounce_rate, avg_duration) '
        'VALUES',
        rows
    )
    print(f'[OK] 成功导入 {len(rows)} 条数据平台统计数据')


def print_separator(title):
    """打印分隔线"""
    print(f'\n{"=" * 60}')
    print(f'  {title}')
    print(f'{"=" * 60}')


def print_table(headers, rows):
    """以表格形式打印查询结果"""
    if not rows:
        print('  （无数据）')
        return

    # 计算每列最大宽度
    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    # 打印表头
    header_line = ' | '.join(str(h).ljust(col_widths[i]) for i, h in enumerate(headers))
    print(f'  {header_line}')
    print(f'  {"-+-".join("-" * w for w in col_widths)}')

    # 打印数据行
    for row in rows:
        row_line = ' | '.join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
        print(f'  {row_line}')


def run_daily_trend(client):
    """查询每日 PV/UV 趋势"""
    print_separator('每日 PV/UV 趋势')

    result = client.execute('''
        SELECT
            toDate(timestamp)                    AS date,
            count()                              AS pv,
            count(DISTINCT user_id)              AS uv,
            countIf(action = 'comment')          AS comments,
            countIf(action = 'like')             AS likes,
            countIf(action = 'share')            AS shares
        FROM demo.access_logs
        GROUP BY date
        ORDER BY date
    ''')

    print_table(['日期', 'PV', 'UV', '评论', '点赞', '分享'], result)


def run_top_posts(client):
    """查询热门文章排行"""
    print_separator('热门文章排行 TOP 10')

    result = client.execute('''
        SELECT
            post_id,
            countIf(action = 'view')             AS views,
            countIf(action = 'comment')          AS comments,
            countIf(action = 'like')             AS likes,
            countIf(action = 'share')            AS shares
        FROM demo.access_logs
        WHERE post_id > 0
        GROUP BY post_id
        ORDER BY views DESC
        LIMIT 10
    ''')

    print_table(['文章ID', '浏览量', '评论', '点赞', '分享'], result)


def run_city_distribution(client):
    """查询地域分布"""
    print_separator('地域分布（按 PV 排序）')

    result = client.execute('''
        SELECT
            city,
            count()                              AS pv,
            count(DISTINCT user_id)              AS uv,
            round(count() * 100.0 / sum(count()) OVER (), 2) AS pv_percent
        FROM demo.access_logs
        GROUP BY city
        ORDER BY pv DESC
    ''')

    print_table(['城市', 'PV', 'UV', 'PV占比(%)'], result)


def run_device_distribution(client):
    """查询设备类型分布"""
    print_separator('设备类型分布')

    result = client.execute('''
        SELECT
            device,
            count()                              AS pv,
            count(DISTINCT user_id)              AS uv,
            round(count() * 100.0 / sum(count()) OVER (), 2) AS pv_percent
        FROM demo.access_logs
        GROUP BY device
        ORDER BY pv DESC
    ''')

    print_table(['设备类型', 'PV', 'UV', 'PV占比(%)'], result)


def run_hourly_trend(client):
    """查询每小时活跃趋势"""
    print_separator('每小时活跃趋势')

    result = client.execute('''
        SELECT
            toHour(timestamp)                    AS hour,
            count()                              AS pv,
            count(DISTINCT user_id)              AS uv
        FROM demo.access_logs
        GROUP BY hour
        ORDER BY hour
    ''')

    print_table(['小时', 'PV', 'UV'], result)


def run_active_users(client):
    """查询活跃用户排行"""
    print_separator('活跃用户排行 TOP 10')

    result = client.execute('''
        SELECT
            user_id,
            count()                              AS total_actions,
            countIf(action = 'view')             AS views,
            countIf(action = 'comment')          AS comments,
            countIf(action = 'like')             AS likes,
            min(toDate(timestamp))               AS first_visit,
            max(toDate(timestamp))               AS last_visit
        FROM demo.access_logs
        WHERE user_id > 0
        GROUP BY user_id
        ORDER BY total_actions DESC
        LIMIT 10
    ''')

    print_table(['用户ID', '操作总数', '浏览', '评论', '点赞', '首次访问', '最近访问'], result)


def run_materialized_view_query(client):
    """从物化视图查询预聚合数据"""
    print_separator('物化视图：每日统计（预聚合结果）')

    result = client.execute('''
        SELECT
            date,
            sum(pv)                              AS pv,
            sum(uv)                              AS uv,
            sum(comment_cnt)                     AS comments,
            sum(like_cnt)                        AS likes,
            sum(share_cnt)                       AS shares
        FROM demo.blog_stats
        GROUP BY date
        ORDER BY date
    ''')

    print_table(['日期', 'PV', 'UV', '评论', '点赞', '分享'], result)


def main():
    """主函数：导入数据并执行分析查询"""
    print('=' * 60)
    print('  数据平台 ClickHouse 数据分析')
    print(f'  执行时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    # 1. 连接 ClickHouse
    client = create_client()

    # 2. 初始化数据库
    init_database(client)

    # 3. 导入数据
    print('\n--- 数据导入 ---')
    access_logs_path = os.path.join(DATA_DIR, 'access_logs.csv')
    demo_stats_path = os.path.join(DATA_DIR, 'demo_stats.csv')

    # 先检查表是否存在，不存在则先建表
    tables = client.execute('SHOW TABLES FROM demo')
    table_names = [t[0] for t in tables]

    if 'access_logs' not in table_names:
        print('[提示] access_logs 表不存在，请先执行 init_tables.sql 建表')
        print('  命令: clickhouse-client --multiquery < init_tables.sql')
        return

    import_access_logs(client, access_logs_path)
    import_demo_stats(client, demo_stats_path)

    # 4. 执行分析查询
    print('\n--- 数据分析 ---')
    run_daily_trend(client)
    run_top_posts(client)
    run_city_distribution(client)
    run_device_distribution(client)
    run_hourly_trend(client)
    run_active_users(client)
    run_materialized_view_query(client)

    # 5. 完成
    print(f'\n{"=" * 60}')
    print('  分析完成！')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    main()
