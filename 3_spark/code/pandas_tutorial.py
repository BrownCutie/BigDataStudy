"""
Pandas 数据分析 — 函数大全实战教程
==================================
配套文档：05.5-Pandas数据分析.md

使用方式：
  1. 直接运行：python3 pandas_tutorial.py
  2. 逐步运行：在 Jupyter/IPython 中按区块复制运行
  3. 按知识点查找：搜索 "===== 第X节 ====="

依赖安装：pip install pandas numpy
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 设置显示选项，让输出更美观
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 120)
pd.set_option('display.max_rows', 30)
pd.set_option('display.unicode.east_asian_width', True)

print("=" * 70)
print("Pandas 数据分析 — 函数大全实战教程")
print("=" * 70)


# ======================================================================
# 准备模拟数据（数据平台）
# ======================================================================
print("\n" + "=" * 70)
print("准备模拟数据")
print("=" * 70)

# 博客文章表
articles = pd.DataFrame({
    'article_id': range(1, 21),
    'title': [
        'Spark入门教程', 'Spark SQL详解', 'Spark性能调优', 'Hadoop核心原理', 'Hive数据仓库',
        'Flink流处理入门', 'Kafka消息队列', 'Python数据分析', 'Docker容器化部署',
        'K8s实战指南', 'Spark MLlib机器学习', 'HBase NoSQL数据库', 'Zookeeper分布式协调',
        'Sqoop数据迁移', 'Pig数据分析', 'Scala编程入门', 'Java大数据开发',
        '数据可视化实战', 'ELK日志分析', 'Redis缓存技术'
    ],
    'author': [
        '张三', '张三', '张三', '李四', '李四',
        '王五', '王五', '赵六', '赵六', '王五',
        '张三', '李四', '王五', '李四', '赵六',
        '张三', '李四', '赵六', '王五', '张三'
    ],
    'category': [
        'Spark', 'Spark', 'Spark', 'Hadoop', 'Hive',
        'Flink', 'Kafka', 'Python', 'DevOps', 'DevOps',
        'Spark', 'Hadoop', 'Hadoop', 'Hadoop', 'Hadoop',
        '语言', '语言', '数据可视化', '日志', '中间件'
    ],
    'views': [
        1500, 2800, 4500, 2300, 800,
        3200, 1800, 950, 1200, 2600,
        3800, 1100, 700, 600, 450,
        2000, 1700, 3100, 2400, 2900
    ],
    'likes': [
        50, 120, 200, 80, 30,
        150, 60, 40, 45, 100,
        180, 35, 20, 15, 10,
        70, 55, 130, 90, 110
    ],
    'publish_date': pd.date_range('2024-01-01', periods=20, freq='15D'),
})

# 手动设置一些缺失值，模拟真实数据
articles.loc[7, 'title'] = None         # 第8篇标题缺失
articles.loc[4, 'views'] = np.nan       # 第5篇浏览量缺失
articles.loc[12, 'likes'] = np.nan      # 第13篇点赞数缺失

# 用户表
users = pd.DataFrame({
    'user_id': range(1, 11),
    'username': ['alice', 'bob', 'charlie', 'david', 'eve',
                 'frank', 'grace', 'henry', 'iris', 'jack'],
    'city': ['北京', '上海', '北京', '广州', '深圳',
             '上海', '杭州', '北京', '成都', '深圳'],
    'register_date': pd.date_range('2023-06-01', periods=10, freq='20D'),
})

# 访问日志表（模拟100条访问记录）
np.random.seed(42)
log_entries = 100
visit_logs = pd.DataFrame({
    'log_id': range(1, log_entries + 1),
    'user_id': np.random.randint(1, 11, size=log_entries),
    'article_id': np.random.randint(1, 21, size=log_entries),
    'action': np.random.choice(['view', 'like', 'comment', 'share'], size=log_entries, p=[0.6, 0.2, 0.15, 0.05]),
    'timestamp': pd.date_range('2024-06-01', periods=log_entries, freq='2h'),
})

print("\n--- 博客文章表（articles）---")
print(articles)
print(f"\n形状: {articles.shape}")

print("\n--- 用户表（users）---")
print(users)

print("\n--- 访问日志表（visit_logs）前20行 ---")
print(visit_logs.head(20))
print(f"\n总行数: {len(visit_logs)}")


# ======================================================================
# 第一节：Pandas 基础
# ======================================================================
print("\n" + "=" * 70)
print("第一节：Pandas 基础 — Series 和 DataFrame")
print("=" * 70)

# --- Series ---
print("\n【1.1 Series — 一维数组，带标签索引】")
s = pd.Series([1500, 2800, 4500], index=['Spark入门', 'Spark SQL', 'Spark调优'], name='views')
print(s)
print(f"类型: {type(s)}")
print(f"索引: {s.index.tolist()}")
print(f"值: {s.values.tolist()}")

# --- DataFrame ---
print("\n【1.2 DataFrame — 二维表格】")
mini_df = pd.DataFrame({
    'title': ['Spark入门', 'Hadoop原理'],
    'views': [1500, 2300],
    'author': ['张三', '李四']
})
print(mini_df)

# --- 创建 DataFrame 的多种方式 ---
print("\n【1.3 创建 DataFrame 的多种方式】")

# 方式1：字典创建
df_from_dict = pd.DataFrame({'name': ['张三', '李四'], 'score': [90, 85]})
print("字典创建:")
print(df_from_dict)

# 方式2：列表 of 字典
df_from_list = pd.DataFrame([
    {'name': '张三', 'score': 90},
    {'name': '李四', 'score': 85}
])
print("\n列表 of 字典:")
print(df_from_list)

# 方式3：NumPy 数组创建
df_from_numpy = pd.DataFrame(
    np.random.randn(3, 3),
    columns=['A', 'B', 'C'],
    index=['x', 'y', 'z']
)
print("\nNumPy 数组创建:")
print(df_from_numpy)

# --- 查看 DataFrame 信息 ---
print("\n【1.4 查看 DataFrame 信息 — 这些函数每天都会用】")

print("\n--- head() 查看前几行 ---")
print(articles.head(5))

print("\n--- tail() 查看后几行 ---")
print(articles.tail(3))

print("\n--- shape 行列数 ---")
print(f"articles 的形状: {articles.shape}  (行数, 列数)")

print("\n--- columns 列名 ---")
print(f"列名: {articles.columns.tolist()}")

print("\n--- dtypes 数据类型 ---")
print(articles.dtypes)

print("\n--- index 行索引 ---")
print(f"行索引类型: {type(articles.index)}, 范围: {articles.index[0]} ~ {articles.index[-1]}")

print("\n--- info() 完整信息 ---")
articles.info()

print("\n--- describe() 统计摘要 ---")
print(articles.describe())

print("\n--- memory_usage() 内存占用 ---")
print(articles.memory_usage(deep=True))


# ======================================================================
# 第二节：数据读取与写入
# ======================================================================
print("\n" + "=" * 70)
print("第二节：数据读取与写入")
print("=" * 70)

# --- 写入 CSV ---
print("\n【2.1 写入 CSV】")
csv_path = '/tmp/pandas_tutorial_articles.csv'
articles.to_csv(csv_path, index=False, encoding='utf-8')
print(f"已写入 CSV: {csv_path}")

# --- 读取 CSV ---
print("\n【2.2 读取 CSV — 各种参数演示】")

# 基础读取
df_basic = pd.read_csv(csv_path)
print("基础读取:")
print(df_basic.head(3))

# 指定列
df_cols = pd.read_csv(csv_path, usecols=['title', 'author', 'views'])
print("\n只读指定列 (usecols):")
print(df_cols.head(3))

# 指定类型
df_types = pd.read_csv(csv_path, dtype={'article_id': 'int32', 'views': 'float32'})
print("\n指定数据类型 (dtype):")
print(df_types.dtypes)

# 只读前几行
df_nrows = pd.read_csv(csv_path, nrows=5)
print(f"\n只读前5行 (nrows=5): 形状 = {df_nrows.shape}")

# --- 写入和读取 JSON ---
print("\n【2.3 JSON 读写】")
json_path = '/tmp/pandas_tutorial_articles.json'
articles.to_json(json_path, orient='records', force_ascii=False, indent=2)
print(f"已写入 JSON: {json_path}")

df_json = pd.read_json(json_path)
print("从 JSON 读取:")
print(df_json.head(3))

# --- 写入和读取 Excel（如果安装了 openpyxl）---
print("\n【2.4 Excel 读写（需要 openpyxl）】")
try:
    excel_path = '/tmp/pandas_tutorial_articles.xlsx'
    articles.to_excel(excel_path, sheet_name='文章', index=False)
    print(f"已写入 Excel: {excel_path}")
    df_excel = pd.read_excel(excel_path)
    print("从 Excel 读取:")
    print(df_excel.head(3))
except ImportError:
    print("未安装 openpyxl，跳过 Excel 演示。安装命令: pip install openpyxl")

# --- 写入 Parquet ---
print("\n【2.5 Parquet 读写（Spark 友好格式）】")
try:
    parquet_path = '/tmp/pandas_tutorial_articles.parquet'
    articles.to_parquet(parquet_path, index=False)
    print(f"已写入 Parquet: {parquet_path}")
    df_parquet = pd.read_parquet(parquet_path)
    print("从 Parquet 读取:")
    print(df_parquet.head(3))
    print(f"数据类型保持完好: {df_parquet.dtypes.tolist()}")
except ImportError:
    print("未安装 pyarrow，跳过 Parquet 演示。安装命令: pip install pyarrow")


# ======================================================================
# 第三节：数据选择与过滤
# ======================================================================
print("\n" + "=" * 70)
print("第三节：数据选择与过滤")
print("=" * 70)

# --- 选择列 ---
print("\n【3.1 选择列】")

# 选择单列（返回 Series）
title_series = articles['title']
print("选择单列 df['title']:")
print(f"类型: {type(title_series)}")
print(title_series.head())

# 选择多列（返回 DataFrame）
subset = articles[['title', 'author', 'views']]
print("\n选择多列 df[['title', 'author', 'views']]:")
print(f"类型: {type(subset)}")
print(subset.head())

# --- loc 标签索引 ---
print("\n【3.2 loc[] — 按标签选择】")

# 选择单行
print("articles.loc[0] — 第0行:")
print(articles.loc[0])

# 选择多行
print("\narticles.loc[0:3] — 第0到第3行（包含两端）:")
print(articles.loc[0:3])

# 选择行列
print("\narticles.loc[0:3, ['title', 'views']] — 指定行和列:")
print(articles.loc[0:3, ['title', 'views']])

# --- iloc 位置索引 ---
print("\n【3.3 iloc[] — 按位置选择】")

print("articles.iloc[0] — 第0行:")
print(articles.iloc[0])

print("\narticles.iloc[0:3] — 前3行（不包含3）:")
print(articles.iloc[0:3])

print("\narticles.iloc[0:3, 0:3] — 前3行的前3列:")
print(articles.iloc[0:3, 0:3])

print("\narticles.iloc[-1] — 最后一行:")
print(articles.iloc[-1])

# --- 条件过滤 ---
print("\n【3.4 条件过滤】")

# 单条件
print("浏览量 > 3000 的文章:")
print(articles[articles['views'] > 3000][['title', 'views']])

# 多条件（用 & | ~ 连接，每个条件必须用括号）
print("\n浏览量 > 2000 且 点赞 > 80:")
result = articles[(articles['views'] > 2000) & (articles['likes'] > 80)]
print(result[['title', 'views', 'likes']])

# 取反
print("\n分类不是 Spark 的文章:")
print(articles[~(articles['category'] == 'Spark')][['title', 'category']].head())

# --- query() ---
print("\n【3.5 query() — 字符串表达式过滤】")
print("query('views > 3000'):")
print(articles.query('views > 3000')[['title', 'views']])

print("\nquery('category in [\"Spark\", \"Flink\", \"Kafka\"]'):")
print(articles.query('category in ["Spark", "Flink", "Kafka"]')[['title', 'category']])

# --- isin() ---
print("\n【3.6 isin() — 值在列表中】")
print("作者是张三或王五的文章:")
print(articles[articles['author'].isin(['张三', '王五'])][['title', 'author']].head())

# --- between() ---
print("\n【3.7 between() — 值在范围内】")
print("浏览量在 1000 到 3000 之间的文章:")
print(articles[articles['views'].between(1000, 3000)][['title', 'views']])

# --- str.contains() ---
print("\n【3.8 str.contains() — 字符串包含匹配】")
print("标题包含 'Spark' 或 'Hadoop' 的文章:")
print(articles[articles['title'].str.contains('Spark|Hadoop', na=False)][['title']])

# --- sample() 随机采样 ---
print("\n【3.9 sample() — 随机采样】")
print("随机抽3行:")
print(articles.sample(n=3, random_state=42)[['title', 'author']])

print("\n随机抽30%的行:")
print(articles.sample(frac=0.3, random_state=42)[['title', 'author']])


# ======================================================================
# 第四节：数据清洗
# ======================================================================
print("\n" + "=" * 70)
print("第四节：数据清洗")
print("=" * 70)

# --- 缺失值检测 ---
print("\n【4.1 缺失值检测】")
print("isna() — 每列缺失值数量:")
print(articles.isna().sum())

print("\nisna().any() — 哪些列有缺失值:")
print(articles.isna().any())

print("\n完整检测（标记缺失值的位置）:")
print(articles.isna())

# --- fillna() 填充缺失值 ---
print("\n【4.2 fillna() — 填充缺失值】")

# 用固定值填充
articles_filled = articles.copy()
articles_filled['views'] = articles_filled['views'].fillna(0)
print("views 缺失值用 0 填充:")
print(articles_filled.loc[articles['views'].isna(), ['title', 'views']])

# 用均值填充
articles_filled2 = articles.copy()
mean_views = articles_filled2['views'].mean()
articles_filled2['views'] = articles_filled2['views'].fillna(mean_views)
print(f"\nviews 缺失值用均值 {mean_views:.1f} 填充:")
print(articles_filled2.loc[articles['views'].isna(), ['title', 'views']])

# 用前一个值填充 (ffill)
articles_filled3 = articles.copy()
articles_filled3['views'] = articles_filled3['views'].fillna(method='ffill')
print("\nviews 缺失值用前一个值填充 (ffill):")
print(articles_filled3.loc[articles['views'].isna(), ['title', 'views']])

# 用字符串填充
articles_filled4 = articles.copy()
articles_filled4['title'] = articles_filled4['title'].fillna('（无标题）')
print("\ntitle 缺失值用字符串填充:")
print(articles_filled4.loc[articles['title'].isna() | (articles.index == 7), ['title']])

# --- dropna() 删除缺失值 ---
print("\n【4.3 dropna() — 删除缺失值】")

# 删除任何含缺失值的行
print("dropna() — 删除任何含缺失值的行:")
print(f"原始行数: {len(articles)}, 删除后: {len(articles.dropna())}")

# 只检查指定列
print("\ndropna(subset=['title']) — 只检查 title 列:")
print(f"删除后行数: {len(articles.dropna(subset=['title']))}")

# 只删除全为空的行
print(f"\ndropna(how='all') — 只删除全为空的行: {len(articles.dropna(how='all'))} 行")

# 保留至少有 n 个非空值的行
print(f"dropna(thresh=5) — 保留至少5个非空值的行: {len(articles.dropna(thresh=5))} 行")

# --- interpolate() 插值 ---
print("\n【4.4 interpolate() — 插值填充】")
s_with_nan = pd.Series([1, np.nan, np.nan, 4, 5, np.nan, 7])
print("原始数据:")
print(s_with_nan)
print("\n线性插值后:")
print(s_with_nan.interpolate())

# --- duplicated() / drop_duplicates() 重复值处理 ---
print("\n【4.5 重复值处理】")

# 创建有重复的数据
df_dup = pd.DataFrame({
    'title': ['Spark入门', 'Spark入门', 'Hadoop原理', 'Flink流处理', 'Flink流处理', 'Flink流处理'],
    'author': ['张三', '张三', '李四', '王五', '王五', '王五'],
    'views': [1500, 1500, 2300, 3200, 3200, 3200]
})

print("原始数据（含重复）:")
print(df_dup)

print("\nduplicated() — 标记重复行:")
print(df_dup.duplicated())

print("\nduplicated(keep='first') — 保留第一个（默认）:")
print(df_dup.duplicated(keep='first'))

print("\n根据 title 列判断重复:")
print(df_dup.duplicated(subset=['title']))

print("\ndrop_duplicates() — 删除重复行:")
print(df_dup.drop_duplicates())

print("\ndrop_duplicates(subset=['title'], keep='last') — 基于 title 列，保留最后一个:")
print(df_dup.drop_duplicates(subset=['title'], keep='last'))

# --- astype() 数据类型转换 ---
print("\n【4.6 数据类型转换 — astype()】")
print("转换前 views 的类型:", articles['views'].dtype)

articles_int32 = articles.copy()
articles_int32['views'] = articles_int32['views'].astype('float')
print("转为 float 后:", articles_int32['views'].dtype)

# pd.to_numeric
print("\npd.to_numeric() — 智能转换:")
s_mixed = pd.Series(['10', '20', 'abc', '40'])
print(f"原始: {s_mixed.tolist()}")
print(f"errors='coerce': {pd.to_numeric(s_mixed, errors='coerce').tolist()}")

# pd.to_datetime
print("\npd.to_datetime() — 转日期:")
date_str = pd.Series(['2024-01-15', '2024-02-28', '2024-03-10'])
print(f"原始: {date_str.tolist()}")
print(f"转换后: {pd.to_datetime(date_str).tolist()}")

# --- 字符串处理 str 访问器 ---
print("\n【4.7 字符串处理 — str 访问器】")
titles = articles['title'].dropna()  # 先去掉缺失值

print(f"原始标题: {titles.tolist()[:5]}")
print(f"str.lower(): {titles.str.lower().tolist()[:5]}")
print(f"str.upper(): {titles.str.upper().tolist()[:3]}")
print(f"str.len() (标题长度): {titles.str.len().tolist()[:5]}")
print(f"str.contains('Spark'): {titles.str.contains('Spark').tolist()[:5]}")
print(f"str.startswith('Spark'): {titles.str.startswith('Spark').tolist()[:5]}")
print(f"str.split('入') (分割): {titles.str.split('入').tolist()[:5]}")
print(f"str.replace('入门', '基础'): {titles.str.replace('入门', '基础').tolist()[:5]}")
print(f"str[:3] (切片，前3字): {titles.str[:3].tolist()[:5]}")
print(f"str.cat(sep=' | ') (前5个拼接): {titles.str.cat(sep=' | ')[:80]}...")

# str.extract 正则提取
print("\nstr.extract() — 正则提取:")
print(titles.str.extract(r'(Spark|Hadoop|Flink|Kafka)').dropna().head())

# --- rename() 重命名 ---
print("\n【4.8 重命名 — rename()】")
print("原始列名:", articles.columns.tolist())

renamed = articles.rename(columns={
    'article_id': 'id',
    'publish_date': 'date'
})
print("重命名后:", renamed.columns.tolist())

# lambda 重命名
print("列名加前缀 add_prefix('col_'):")
print(articles.add_prefix('col_').columns.tolist())

# 直接赋值
cols = articles.columns.tolist()
cols_upper = [c.upper() for c in cols]
print(f"\n手动全大写: {cols_upper}")


# ======================================================================
# 第五节：数据排序与排名
# ======================================================================
print("\n" + "=" * 70)
print("第五节：数据排序与排名")
print("=" * 70)

# --- sort_values() ---
print("\n【5.1 sort_values() — 按值排序】")
print("按浏览量升序:")
print(articles[['title', 'views']].sort_values('views').head(5))

print("\n按浏览量降序:")
print(articles[['title', 'views']].sort_values('views', ascending=False).head(5))

print("\n先按分类升序，再按浏览量降序:")
print(articles[['title', 'category', 'views']].sort_values(
    ['category', 'views'], ascending=[True, False]
).head(10))

# --- sort_index() ---
print("\n【5.2 sort_index() — 按索引排序】")
shuffled = articles.sample(5, random_state=42)
print("乱序:")
print(shuffled[['title', 'views']])
print("\n按索引排序后:")
print(shuffled.sort_index()[['title', 'views']])

# --- rank() 排名 ---
print("\n【5.3 rank() — 排名】")
rank_df = articles[['title', 'views']].copy()
rank_df['rank_default'] = rank_df['views'].rank()
rank_df['rank_desc'] = rank_df['views'].rank(ascending=False)
rank_df['rank_min'] = rank_df['views'].rank(method='min')
print(rank_df.sort_values('rank_desc').head(10))

# --- nlargest / nsmallest ---
print("\n【5.4 nlargest() / nsmallest() — 取 Top N】")
print("浏览量前5:")
print(articles.nlargest(5, 'views')[['title', 'views']])

print("\n浏览量后3:")
print(articles.nsmallest(3, 'views')[['title', 'views']])

print("\n点赞数前5:")
print(articles.nlargest(5, 'likes')[['title', 'likes']])


# ======================================================================
# 第六节：数据聚合与分组
# ======================================================================
print("\n" + "=" * 70)
print("第六节：数据聚合与分组")
print("=" * 70)

# --- 基础聚合函数 ---
print("\n【6.1 基础聚合函数】")
print(f"浏览量总和: {articles['views'].sum()}")
print(f"浏览量均值: {articles['views'].mean():.1f}")
print(f"浏览量中位数: {articles['views'].median():.1f}")
print(f"浏览量最大值: {articles['views'].max()}")
print(f"浏览量最小值: {articles['views'].min()}")
print(f"浏览量标准差: {articles['views'].std():.1f}")
print(f"浏览量最大值所在行: {articles['views'].idxmax()}")
print(f"浏览量最小值所在行: {articles['views'].idxmin()}")

print("\nviews 和 likes 的相关系数矩阵:")
print(articles[['views', 'likes']].corr())

# --- groupby 分组聚合 ---
print("\n【6.2 groupby() — 分组聚合】")
print("按分类统计浏览量总和:")
print(articles.groupby('category')['views'].sum())

print("\n按分类统计浏览量的多种指标:")
print(articles.groupby('category')['views'].agg(['sum', 'mean', 'count', 'max', 'min']))

print("\n按作者统计:")
print(articles.groupby('author').agg({
    'views': 'sum',
    'likes': 'mean',
    'title': 'count'
}).rename(columns={'title': '文章数', 'views': '总浏览量', 'likes': '平均点赞'}))

# --- 命名聚合 ---
print("\n【6.3 命名聚合 — agg() with NamedAgg】")
result = articles.groupby('category').agg(
    总浏览量=pd.NamedAgg(column='views', aggfunc='sum'),
    平均点赞=pd.NamedAgg(column='likes', aggfunc='mean'),
    文章数=pd.NamedAgg(column='title', aggfunc='count')
)
print(result)

# --- transform 变换 ---
print("\n【6.4 transform() — 分组变换（保持原 DataFrame 形状）】")
articles['category_avg_views'] = articles.groupby('category')['views'].transform('mean')
articles['views_vs_avg'] = articles['views'] - articles['category_avg_views']
print("每个分类的平均浏览量:")
print(articles[['title', 'category', 'views', 'category_avg_views', 'views_vs_avg']].head(10))
# 清理临时列
articles.drop(columns=['category_avg_views', 'views_vs_avg'], inplace=True)

# --- apply 自定义函数 ---
print("\n【6.5 apply() — 自定义函数】")
print("每篇文章的互动率 (likes / views):")
articles['engagement'] = articles.apply(
    lambda row: row['likes'] / row['views'] if pd.notna(row['views']) and row['views'] > 0 else 0,
    axis=1
)
print(articles[['title', 'likes', 'views', 'engagement']].head(10))

# groupby + apply: 每个分类浏览量前2的文章
print("\n每个分类浏览量前2的文章:")
top2_per_category = articles.dropna(subset=['views']).groupby('category').apply(
    lambda g: g.nlargest(2, 'views')[['title', 'views']],
    include_groups=False
)
print(top2_per_category)

# 清理临时列
articles.drop(columns=['engagement'], inplace=True)

# --- pivot_table 透视表 ---
print("\n【6.6 pivot_table() — 透视表】")
pivot = articles.pivot_table(
    values='views',
    index='category',
    columns='author',
    aggfunc='sum',
    fill_value=0
)
print("分类 x 作者 的浏览量交叉表:")
print(pivot)

# --- crosstab 交叉表 ---
print("\n【6.7 crosstab() — 交叉表（计数）】")
print("分类 x 作者 的文章数量:")
print(pd.crosstab(articles['category'], articles['author'], margins=True))

# --- value_counts ---
print("\n【6.8 value_counts() — 计数统计】")
print("各分类的文章数量:")
print(articles['category'].value_counts())

print("\n各作者的文章数量:")
print(articles['author'].value_counts())

print("\n各分类的文章占比:")
print(articles['category'].value_counts(normalize=True))

# --- unique / nunique ---
print("\n【6.9 unique() / nunique()】")
print(f"所有分类: {articles['category'].unique().tolist()}")
print(f"分类数量: {articles['category'].nunique()}")
print(f"作者数量: {articles['author'].nunique()}")


# ======================================================================
# 第七节：数据合并与连接
# ======================================================================
print("\n" + "=" * 70)
print("第七节：数据合并与连接")
print("=" * 70)

# 准备用于合并的数据
articles_clean = articles.dropna(subset=['title'])  # 去掉标题为空的文章
author_stats = pd.DataFrame({
    'author': ['张三', '李四', '王五', '赵六'],
    'total_articles': [5, 5, 5, 5],
    'specialty': ['Spark', 'Hadoop', '流计算', 'Python']
})

# --- pd.concat 拼接 ---
print("\n【7.1 pd.concat() — 拼接】")
df1 = articles_clean[['article_id', 'title']].head(5)
df2 = articles_clean[['article_id', 'title']].iloc[5:10]
print("df1:")
print(df1)
print("\ndf2:")
print(df2)

print("\npd.concat([df1, df2]) — 纵向拼接:")
print(pd.concat([df1, df2], ignore_index=True))

print("\npd.concat([df1, df2], axis=1) — 横向拼接:")
print(pd.concat([df1.reset_index(drop=True), df2.reset_index(drop=True)], axis=1))

# --- pd.merge 连接 ---
print("\n【7.2 pd.merge() — 连接（SQL JOIN）】")

# 内连接
print("内连接 (inner join) — 文章 + 作者统计:")
merged_inner = pd.merge(
    articles_clean[['title', 'author', 'views']],
    author_stats,
    on='author',
    how='inner'
)
print(merged_inner.head(8))

# 左连接
print("\n左连接 (left join):")
# 创建一个不匹配的数据演示
author_extra = pd.DataFrame({
    'author': ['张三', '李四', '孙七'],  # 孙七不在文章表中
    'level': ['高级', '中级', '初级']
})
merged_left = pd.merge(
    articles_clean[['title', 'author']],
    author_extra,
    on='author',
    how='left'
)
print(merged_left[merged_left['author'] == '张三'].head(3))
print("注意: 左连接保留了所有文章，孙七不会出现在结果中")

# 外连接
print("\n外连接 (outer join) — 保留所有:")
merged_outer = pd.merge(
    articles_clean[['title', 'author']].head(5),
    author_extra,
    on='author',
    how='outer'
)
print(merged_outer)

# 不同列名连接
print("\n不同列名连接 (left_on / right_on):")
user_activity = pd.DataFrame({
    'user_name': ['张三', '李四', '王五'],
    'login_count': [120, 85, 200]
})
merged_diff = pd.merge(
    author_stats,
    user_activity,
    left_on='author',
    right_on='user_name',
    how='left'
)
print(merged_diff)

# --- df.join ---
print("\n【7.3 df.join() — 基于索引连接】")
df_left = pd.DataFrame({'A': [1, 2, 3]}, index=['a', 'b', 'c'])
df_right = pd.DataFrame({'B': [4, 5, 6]}, index=['a', 'b', 'd'])
print("左表:")
print(df_left)
print("\n右表:")
print(df_right)
print("\njoin (how='outer'):")
print(df_left.join(df_right, how='outer'))

# --- combine_first ---
print("\n【7.4 combine_first() — 用另一个 DataFrame 补充缺失值】")
df_original = pd.DataFrame({'A': [1, np.nan, 3], 'B': [np.nan, 5, 6]})
df_backup = pd.DataFrame({'A': [10, 20, 30], 'B': [40, 50, 60]})
print("原始数据:")
print(df_original)
print("\n备份数据:")
print(df_backup)
print("\ncombine_first 后（用备份填充缺失）:")
print(df_original.combine_first(df_backup))


# ======================================================================
# 第八节：时间序列
# ======================================================================
print("\n" + "=" * 70)
print("第八节：时间序列")
print("=" * 70)

# --- pd.to_datetime ---
print("\n【8.1 pd.to_datetime() — 转换为日期】")
date_strings = pd.Series(['2024-01-15', '2024-02-28', '2024-03-10', '2024-06-22'])
dates = pd.to_datetime(date_strings)
print(f"字符串: {date_strings.tolist()}")
print(f"转换后: {dates.tolist()}")
print(f"类型: {dates.dtype}")

# 指定格式
custom_date = pd.to_datetime('15/01/2024', format='%d/%m/%Y')
print(f"自定义格式解析: {custom_date}")

# --- pd.date_range ---
print("\n【8.2 pd.date_range() — 生成日期范围】")
print("2024年每天:")
print(pd.date_range('2024-01-01', '2024-01-10', freq='D'))

print("\n2024年每月初:")
print(pd.date_range('2024-01-01', periods=12, freq='MS'))

print("\n每2小时:")
print(pd.date_range('2024-01-01', periods=6, freq='2h'))

# --- dt 访问器 ---
print("\n【8.3 dt 访问器 — 提取时间分量】")
dates_df = articles[['title', 'publish_date']].copy()
print(f"原始日期列: {dates_df['publish_date'].head().tolist()}")

dates_df['year'] = dates_df['publish_date'].dt.year
dates_df['month'] = dates_df['publish_date'].dt.month
dates_df['day'] = dates_df['publish_date'].dt.day
dates_df['dayofweek'] = dates_df['publish_date'].dt.dayofweek
dates_df['quarter'] = dates_df['publish_date'].dt.quarter
dates_df['is_month_end'] = dates_df['publish_date'].dt.is_month_end

print("\n时间分量提取:")
print(dates_df.head(10))

# day_name
print("\n星期名称:")
print(dates_df['publish_date'].dt.day_name().head(10))

# --- 构造时间序列数据用于演示 resample 和 rolling ---
print("\n【8.4 准备时间序列数据（按天的浏览量）】")
# 用30天的模拟数据演示
ts_data = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=30, freq='D'),
    'views': np.random.randint(200, 1000, size=30).astype(float)
})
ts_data.set_index('date', inplace=True)
print(ts_data.head(10))
print(f"...")
print(ts_data.tail(5))

# --- resample 重采样 ---
print("\n【8.5 resample() — 重采样】")
print("按周汇总浏览量:")
weekly = ts_data.resample('W')['views'].sum()
print(weekly)

print("\n按周统计浏览量均值:")
weekly_mean = ts_data.resample('W')['views'].mean()
print(weekly_mean)

print("\n按周统计多种指标:")
weekly_agg = ts_data.resample('W')['views'].agg(['sum', 'mean', 'count', 'max', 'min'])
print(weekly_agg)

# --- rolling 滚动窗口 ---
print("\n【8.6 rolling() — 滚动窗口】")
ts_data['views_3d_avg'] = ts_data['views'].rolling(window=3).mean()
ts_data['views_7d_avg'] = ts_data['views'].rolling(window=7, min_periods=1).mean()
ts_data['views_3d_sum'] = ts_data['views'].rolling(window=3).sum()

print("3天和7天滚动平均:")
print(ts_data[['views', 'views_3d_avg', 'views_7d_avg']].head(15))

# --- Timedelta ---
print("\n【8.7 Timedelta — 时间差】")
print(pd.Timedelta(days=1))
print(pd.Timedelta(hours=3, minutes=30))
print(pd.Timedelta(weeks=2))

today = pd.Timestamp('2024-06-15')
print(f"\ntoday = {today}")
print(f"tomorrow = {today + pd.Timedelta(days=1)}")
print(f"last_week = {today - pd.Timedelta(weeks=1)}")

diff = pd.Timestamp('2024-06-20') - pd.Timestamp('2024-06-15')
print(f"\n2024-06-20 - 2024-06-15 = {diff}  ({diff.days}天)")

# 清理临时列
ts_data.drop(columns=['views_3d_avg', 'views_7d_avg', 'views_3d_sum'], inplace=True)


# ======================================================================
# 第九节：高级操作
# ======================================================================
print("\n" + "=" * 70)
print("第九节：高级操作")
print("=" * 70)

# --- apply / map ---
print("\n【9.1 apply() / map() — 自定义函数】")

# 对 Series 用 apply
print("views 翻倍 (apply):")
print(articles['views'].apply(lambda x: x * 2 if pd.notna(x) else 0).head())

# 对 Series 用 map — 值映射
print("\ncategory 映射 (map):")
cat_map = {'Spark': '计算引擎', 'Hadoop': '大数据', 'Flink': '流计算',
           'Kafka': '消息队列', 'Hive': '数据仓库', 'Python': '语言',
           'DevOps': '运维', '语言': '编程', '数据可视化': '可视化',
           '日志': '日志', '中间件': '中间件'}
print(articles['category'].map(cat_map).head(10))

# 对 DataFrame 用 applymap（Pandas 2.1+ 用 map）
print("\napplymap/map — 对每个元素操作:")
numeric_df = articles[['views', 'likes']].head(5)
print("原始:")
print(numeric_df)
print("\n每个元素 + 10:")
print(numeric_df.map(lambda x: x + 10 if pd.notna(x) else 0))

# --- pipe 管道 ---
print("\n【9.2 pipe() — 管道操作】")

def step1_clean(df):
    """步骤1：去掉标题为空的文章"""
    return df.dropna(subset=['title'])

def step2_add_metric(df):
    """步骤2：添加互动率"""
    df = df.copy()
    df['engagement_rate'] = df['likes'] / df['views']
    return df

def step3_filter(df):
    """步骤3：过滤浏览量 > 1000"""
    return df[df['views'] > 1000]

# 管道式调用
result_pipe = (articles
    .pipe(step1_clean)
    .pipe(step2_add_metric)
    .pipe(step3_filter)
)
print("管道处理结果（清洗 -> 添加指标 -> 过滤）:")
print(result_pipe[['title', 'views', 'likes', 'engagement_rate']].head(10))

# --- 窗口函数 ---
print("\n【9.3 窗口函数 — shift / diff / pct_change / cumsum】")

window_df = articles[['title', 'views']].dropna().head(10).copy()

# shift — 偏移
window_df['prev_views'] = window_df['views'].shift(1)
window_df['next_views'] = window_df['views'].shift(-1)
print("shift — 前一行/后一行:")
print(window_df[['title', 'views', 'prev_views', 'next_views']])

# diff — 差分
window_df['views_diff'] = window_df['views'].diff()
print("\ndiff — 与前一行差值:")
print(window_df[['title', 'views', 'views_diff']])

# pct_change — 百分比变化
window_df['views_pct'] = window_df['views'].pct_change()
print("\npct_change — 环比增长率:")
print(window_df[['title', 'views', 'views_pct']])

# cumsum — 累计
window_df['views_cumsum'] = window_df['views'].cumsum()
window_df['views_cummax'] = window_df['views'].cummax()
print("\ncumsum / cummax — 累计和 / 累计最大:")
print(window_df[['title', 'views', 'views_cumsum', 'views_cummax']])

# --- where / mask / clip ---
print("\n【9.4 where() / mask() / clip()】")
views = articles['views'].dropna()

print("where — 条件为 True 保留，False 替换:")
print(views.where(views > 2000, other=2000).head(10))

print("\nmask — 与 where 相反，条件为 True 替换:")
print(views.mask(views > 3000, other=3000).head(10))

print("\nclip — 截断到指定范围 [500, 4000]:")
print(views.clip(lower=500, upper=4000).head(10))

# --- cut / qcut 分箱 ---
print("\n【9.5 cut() / qcut() — 分箱】")

# cut 等宽分箱
views_clean = articles['views'].dropna()
bins_result = pd.cut(
    views_clean,
    bins=[0, 1000, 2000, 3000, 5000],
    labels=['低(<1k)', '中(1-2k)', '高(2-3k)', '热门(>3k)']
)
print("cut 等宽分箱:")
print(pd.DataFrame({'views': views_clean, 'level': bins_result}).head(15))

# qcut 等频分箱
qcut_result = pd.qcut(views_clean, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
print("\nqcut 等频分箱（每箱数量大致相同）:")
print(pd.DataFrame({'views': views_clean, 'quartile': qcut_result}).head(15))

# --- get_dummies 独热编码 ---
print("\n【9.6 get_dummies() — 独热编码】")
categories = articles['category'].head(10)
dummies = pd.get_dummies(categories, prefix='cat')
print("分类:")
print(categories.tolist())
print("\n独热编码:")
print(dummies)

# --- melt / pivot 宽长表转换 ---
print("\n【9.7 melt() / pivot() — 宽长表转换】")

# 宽表
wide_df = articles[['title', 'views', 'likes']].dropna().head(5)
print("宽表:")
print(wide_df)

# melt — 宽转长
long_df = pd.melt(
    wide_df,
    id_vars=['title'],
    value_vars=['views', 'likes'],
    var_name='metric',
    value_name='value'
)
print("\nmelt 宽转长:")
print(long_df)

# pivot — 长转宽
back_to_wide = long_df.pivot(
    index='title',
    columns='metric',
    values='value'
)
print("\npivot 长转宽:")
print(back_to_wide)


# ======================================================================
# 第十节：性能优化
# ======================================================================
print("\n" + "=" * 70)
print("第十节：性能优化")
print("=" * 70)

# --- 向量化 vs 循环 ---
print("\n【10.1 向量化操作 vs 循环】")

test_df = pd.DataFrame({'value': np.random.randn(100000)})

# 向量化操作
import time
start = time.time()
result_vectorized = test_df['value'] * 2 + 1
vectorized_time = time.time() - start
print(f"向量化操作: {vectorized_time:.6f} 秒")

# 循环
start = time.time()
result_loop = []
for i in range(len(test_df)):
    result_loop.append(test_df.loc[i, 'value'] * 2 + 1)
loop_time = time.time() - start
print(f"循环操作: {loop_time:.6f} 秒")
print(f"向量化比循环快 {loop_time / vectorized_time:.0f} 倍")

# numpy where
start = time.time()
result_numpy = np.where(test_df['value'] > 0, test_df['value'] * 2, test_df['value'])
numpy_time = time.time() - start
print(f"numpy where: {numpy_time:.6f} 秒")

# --- eval 表达式求值 ---
print("\n【10.2 eval() — 表达式求值】")
eval_df = articles[['views', 'likes']].dropna().head(5).copy()
print("原始:")
print(eval_df)

result_eval = eval_df.eval('engagement = likes / views')
print("\neval('engagement = likes / views'):")
print(result_eval)

# --- 数据类型优化 ---
print("\n【10.3 数据类型优化】")
print("优化前内存占用:")
print(articles.memory_usage(deep=True))
print(f"总内存: {articles.memory_usage(deep=True).sum() / 1024:.1f} KB")

# 优化
articles_opt = articles.copy()
for col in articles_opt.select_dtypes(include=['int64']).columns:
    articles_opt[col] = pd.to_numeric(articles_opt[col], downcast='integer')
for col in articles_opt.select_dtypes(include=['float64']).columns:
    articles_opt[col] = pd.to_numeric(articles_opt[col], downcast='float')
for col in articles_opt.select_dtypes(include=['object']).columns:
    if articles_opt[col].nunique() / len(articles_opt[col]) < 0.5:
        articles_opt[col] = articles_opt[col].astype('category')

print("\n优化后内存占用:")
print(articles_opt.memory_usage(deep=True))
print(f"总内存: {articles_opt.memory_usage(deep=True).sum() / 1024:.1f} KB")

# category 类型的优势
print("\ncategory 类型对字符串列的优化:")
s_normal = pd.Series(['Spark'] * 10000 + ['Flink'] * 10000 + ['Kafka'] * 10000)
s_category = s_normal.astype('category')
print(f"object 类型: {s_normal.memory_usage(deep=True) / 1024:.1f} KB")
print(f"category 类型: {s_category.memory_usage(deep=True) / 1024:.1f} KB")
print(f"节省: {(1 - s_category.memory_usage(deep=True) / s_normal.memory_usage(deep=True)) * 100:.1f}%")

# --- 分块读取大文件 ---
print("\n【10.4 分块读取大文件】")

# 先创建一个大文件用于演示
big_csv_path = '/tmp/pandas_big_file.csv'
big_df = pd.DataFrame({
    'id': range(100000),
    'value': np.random.randn(100000),
    'category': np.random.choice(['A', 'B', 'C', 'D'], size=100000)
})
big_df.to_csv(big_csv_path, index=False)
print(f"创建了 {len(big_df)} 行的 CSV 文件: {big_csv_path}")

# 方式1：只读前几行（预览）
preview = pd.read_csv(big_csv_path, nrows=5)
print(f"\nnrows=5 预览: {preview.shape}")

# 方式2：chunksize 分块处理
print("\nchunksize 分块处理:")
chunk_results = []
for chunk in pd.read_csv(big_csv_path, chunksize=20000):
    # 每个 chunk 做简单聚合
    chunk_results.append({
        'chunk_size': len(chunk),
        'mean_value': chunk['value'].mean(),
        'categories': chunk['category'].nunique()
    })
    print(f"  处理了一个 chunk: {len(chunk)} 行, 均值={chunk['value'].mean():.4f}")

print(f"\n共处理 {len(chunk_results)} 个 chunk")

# 方式3：只读需要的列
cols_only = pd.read_csv(big_csv_path, usecols=['id', 'value'])
print(f"\n只读指定列: {cols_only.shape}, 列名: {cols_only.columns.tolist()}")


# ======================================================================
# 第十一节：Pandas 与 Spark 对照
# ======================================================================
print("\n" + "=" * 70)
print("第十一节：Pandas 与 Spark 对照")
print("=" * 70)

print("""
+------------------------+-----------------------------+----------------------------------+
| 操作                   | Pandas                      | Spark DataFrame                  |
+------------------------+-----------------------------+----------------------------------+
| 创建                   | pd.DataFrame(data)          | spark.createDataFrame(data)      |
| 查看数据               | df.head()                   | df.show()                        |
| 查看结构               | df.info()                   | df.printSchema()                 |
| 选择列                 | df[['a', 'b']]              | df.select('a', 'b')              |
| 过滤                   | df[df['a'] > 0]             | df.filter(col('a') > 0)          |
| 排序                   | df.sort_values('a')         | df.orderBy('a')                  |
| 重命名                 | df.rename(columns={...})    | df.withColumnRenamed('old','new')|
| 新增列                 | df['new'] = df['a'] * 2     | df.withColumn('new', col('a')*2) |
| 删除列                 | df.drop('a', axis=1)        | df.drop('a')                     |
| 缺失值填充             | df.fillna(0)                | df.fillna(0)                     |
| 删除缺失值             | df.dropna()                 | df.dropna()                      |
| 去重                   | df.drop_duplicates()        | df.dropDuplicates()              |
| 分组聚合               | df.groupby('a').agg(...)    | df.groupBy('a').agg(...)         |
| 连接                   | pd.merge(df1, df2, on='id') | df1.join(df2, 'id')              |
| 拼接                   | pd.concat([df1, df2])       | df1.union(df2)                   |
| 类型转换               | df['a'].astype('int')       | col('a').cast('int')             |
| 字符串操作             | df['a'].str.lower()         | lower(col('a'))                  |
| 统计描述               | df.describe()               | df.describe().show()             |
| 计数                   | df['a'].value_counts()      | df.groupBy('a').count()          |
| 写入                   | df.to_parquet('f')          | df.write.parquet('f')            |
+------------------------+-----------------------------+----------------------------------+

核心差异:
  - Pandas: 立即执行（eager）, 单机, 有索引, 可原地修改
  - Spark:  惰性求值（lazy）, 分布式, 无索引, 不可变

掌握了 Pandas, Spark DataFrame 就只是换个写法而已!
""")

print("=" * 70)
print("全部知识点演示完毕!")
print("=" * 70)
