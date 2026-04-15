#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据分析项目 Redis 缓存演示
======================

功能：
  1. 连接 Redis
  2. 加载博客文章数据并缓存
  3. 浏览量计数（INCR）
  4. 热门文章排行榜（ZSet）
  5. 实时查询演示
  6. 缓存命中统计

依赖：
  pip install redis

使用方法：
  python blog_blog_cache_demo.py
"""

import json
import os
import random
import time
import uuid

import redis

# ============================================================
# 配置
# ============================================================

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None  # 本地开发环境无密码

# 博客图片路径
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DEMO_POSTS_FILE = os.path.join(DATA_DIR, "posts.json")
HOT_ARTICLES_FILE = os.path.join(DATA_DIR, "hot_articles.json")

# 缓存配置
ARTICLE_CACHE_TTL = 3600       # 文章缓存过期时间（秒）
SESSION_TTL = 7200             # Session 过期时间（秒）
HOT_ARTICLES_KEY = "demo:hot_articles"


# ============================================================
# Redis 连接
# ============================================================

def create_redis_client():
    """创建 Redis 客户端连接"""
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )
    return client


# ============================================================
# 数据加载
# ============================================================

def load_posts(filepath):
    """加载博客文章 JSON 数据"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("articles", [])


def load_hot_articles(filepath):
    """加载热门文章 JSON 数据"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("hot_articles", [])


# ============================================================
# 功能一：文章详情缓存（Cache Aside 模式）
# ============================================================

# 内存中的文章数据库（模拟 MySQL）
_articles_db = {}


def init_articles_db(articles):
    """初始化文章数据库"""
    global _articles_db
    for article in articles:
        _articles_db[article["id"]] = article
    print(f"[初始化] 已加载 {len(_articles_db)} 篇文章到数据库")


def get_article_from_db(article_id):
    """从数据库获取文章（模拟慢查询）"""
    time.sleep(0.02)  # 模拟 20ms 数据库查询延迟
    article = _articles_db.get(article_id)
    if article:
        return json.dumps(article, ensure_ascii=False)
    return None


def get_article(r, article_id):
    """
    获取文章详情（带缓存，Cache Aside 模式）
    读流程：先查 Redis → 未命中查数据库 → 回写缓存
    """
    cache_key = f"demo:article:{article_id}"

    # 第一步：查缓存
    cached = r.get(cache_key)
    if cached is not None:
        if cached == "NULL":
            return None
        print(f"  [缓存命中] 文章 {article_id} 从 Redis 获取")
        return json.loads(cached)

    # 第二步：缓存未命中，查数据库
    print(f"  [缓存未命中] 文章 {article_id} 从数据库获取")
    article_json = get_article_from_db(article_id)
    if article_json is None:
        # 缓存空值，防止缓存穿透（5 分钟过期）
        r.setex(cache_key, 300, "NULL")
        return None

    # 第三步：回写缓存（带随机 TTL 防止雪崩）
    ttl = ARTICLE_CACHE_TTL + random.randint(0, 600)
    r.setex(cache_key, ttl, article_json)
    print(f"  [回写缓存] 文章 {article_id} 已缓存（TTL={ttl}秒）")

    return json.loads(article_json)


def update_article(r, article_id, data):
    """
    更新文章详情
    写流程：先更新数据库 → 再删除缓存
    """
    if article_id not in _articles_db:
        print(f"  [错误] 文章 {article_id} 不存在")
        return False

    # 第一步：更新数据库
    _articles_db[article_id].update(data)
    _articles_db[article_id]["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"  [更新数据库] 文章 {article_id} 更新成功")

    # 第二步：删除缓存（下次读取时自动回填）
    cache_key = f"demo:article:{article_id}"
    r.delete(cache_key)
    print(f"  [删除缓存] 文章 {article_id} 的缓存已删除")

    return True


# ============================================================
# 功能二：实时浏览量计数
# ============================================================

def record_view(r, article_id):
    """记录文章浏览量（原子自增）"""
    key = f"demo:views:{article_id}"
    views = r.incr(key)
    return views


def get_views(r, article_id):
    """获取文章浏览量"""
    key = f"demo:views:{article_id}"
    views = r.get(key)
    return int(views) if views else 0


def batch_record_views(r, article_ids, count=100):
    """批量模拟浏览"""
    for _ in range(count):
        article_id = random.choice(article_ids)
        record_view(r, article_id)


# ============================================================
# 功能三：文章热度排行榜
# ============================================================

# 热度权重
SCORE_WEIGHTS = {
    "view": 1,
    "like": 5,
    "comment": 10,
    "collect": 8,
}


def init_hot_articles(r, hot_articles_data):
    """初始化热门文章排行榜"""
    # 先清空旧数据
    r.delete(HOT_ARTICLES_KEY)

    # 加载初始热度数据
    for item in hot_articles_data:
        r.zadd(HOT_ARTICLES_KEY, {item["article_id"]: item["heat_score"]})

    count = r.zcard(HOT_ARTICLES_KEY)
    print(f"[初始化] 热门文章排行榜已加载 {count} 篇文章")


def increase_heat(r, article_id, action="view"):
    """
    增加文章热度
    :param action: view/like/comment/collect
    """
    score = SCORE_WEIGHTS.get(action, 1)
    r.zincrby(HOT_ARTICLES_KEY, score, article_id)


def get_hot_articles(r, top_n=10):
    """获取热门文章 Top N（从高到低）"""
    results = r.zrevrange(HOT_ARTICLES_KEY, 0, top_n - 1, withscores=True)
    return [(member, int(score)) for member, score in results]


def get_article_rank(r, article_id):
    """获取文章的排名（从 1 开始）"""
    rank = r.zrevrank(HOT_ARTICLES_KEY, article_id)
    return rank + 1 if rank is not None else None


def get_article_heat(r, article_id):
    """获取文章的热度分数"""
    score = r.zscore(HOT_ARTICLES_KEY, article_id)
    return int(score) if score is not None else 0


# ============================================================
# 功能四：用户 Session 管理
# ============================================================

def create_session(r, user_id, user_info):
    """创建用户 Session"""
    session_token = str(uuid.uuid4())
    session_key = f"demo:session:{session_token}"

    user_info["user_id"] = user_id
    user_info["login_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

    r.hset(session_key, mapping=user_info)
    r.expire(session_key, SESSION_TTL)

    return session_token


def get_session(r, session_token):
    """获取 Session 信息"""
    session_key = f"demo:session:{session_token}"
    user_info = r.hgetall(session_key)

    if not user_info:
        return None

    # 滑动过期：每次访问刷新过期时间
    r.expire(session_key, SESSION_TTL)
    return user_info


def destroy_session(r, session_token):
    """销毁 Session（登出）"""
    session_key = f"demo:session:{session_token}"
    r.delete(session_key)


# ============================================================
# 功能五：分布式锁
# ============================================================

def acquire_lock(r, lock_name, expire_seconds=30):
    """获取分布式锁"""
    lock_key = f"demo:lock:{lock_name}"
    lock_token = str(uuid.uuid4())

    acquired = r.set(lock_key, lock_token, nx=True, ex=expire_seconds)
    if acquired:
        return lock_token
    return None


def release_lock(r, lock_name, lock_token):
    """释放分布式锁（Lua 脚本保证原子性）"""
    lua_script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    else
        return 0
    end
    """
    lock_key = f"demo:lock:{lock_name}"
    result = r.eval(lua_script, 1, lock_key, lock_token)
    return bool(result)


# ============================================================
# 功能六：缓存命中统计
# ============================================================

class CacheStats:
    """缓存命中统计器"""

    def __init__(self):
        self.hits = 0
        self.misses = 0

    def record_hit(self):
        self.hits += 1

    def record_miss(self):
        self.misses += 1

    @property
    def total(self):
        return self.hits + self.misses

    @property
    def hit_rate(self):
        if self.total == 0:
            return 0
        return self.hits / self.total * 100

    def report(self):
        print(f"\n  缓存统计：命中 {self.hits} 次，未命中 {self.misses} 次，"
              f"命中率 {self.hit_rate:.1f}%")


# ============================================================
# 主函数
# ============================================================

def main():
    """主函数：演示所有功能"""

    print("=" * 60)
    print("  数据分析项目 Redis 缓存演示")
    print("=" * 60)

    # 连接 Redis
    print("\n[1] 连接 Redis...")
    r = create_redis_client()
    try:
        r.ping()
        print(f"  连接成功: {REDIS_HOST}:{REDIS_PORT}")
    except redis.ConnectionError:
        print(f"  连接失败！请确保 Redis 服务已启动（redis-server）")
        return

    # 加载数据
    print("\n[2] 加载博客文章数据...")
    articles = load_posts(DEMO_POSTS_FILE)
    hot_articles_data = load_hot_articles(HOT_ARTICLES_FILE)
    init_articles_db(articles)

    # ---- 功能一：文章缓存演示 ----
    print("\n" + "=" * 60)
    print("  功能一：文章详情缓存（Cache Aside 模式）")
    print("=" * 60)

    # 第一次读取（缓存未命中）
    print("\n--- 第一次读取文章 1001（缓存未命中）---")
    article = get_article(r, "1001")
    if article:
        print(f"  标题: {article['title']}")

    # 第二次读取（缓存命中）
    print("\n--- 第二次读取文章 1001（缓存命中）---")
    article = get_article(r, "1001")
    if article:
        print(f"  标题: {article['title']}")

    # 缓存命中统计
    print("\n--- 缓存命中率测试（读取 100 次）---")
    stats = CacheStats()
    article_ids = [a["id"] for a in articles]
    for _ in range(100):
        aid = random.choice(article_ids)
        # 先检查缓存中是否有
        cache_key = f"demo:article:{aid}"
        if r.exists(cache_key):
            stats.record_hit()
            get_article(r, aid)
        else:
            stats.record_miss()
            get_article(r, aid)
    stats.report()

    # 更新文章
    print("\n--- 更新文章 1001 ---")
    update_article(r, "1001", {"title": "Redis 入门教程（更新版）"})

    # 更新后再读取（缓存被清除，需要重新从数据库获取）
    print("\n--- 更新后读取文章 1001（缓存重建）---")
    article = get_article(r, "1001")
    if article:
        print(f"  标题: {article['title']}")

    # ---- 功能二：浏览量计数演示 ----
    print("\n" + "=" * 60)
    print("  功能二：实时浏览量计数")
    print("=" * 60)

    print("\n--- 批量模拟浏览 ---")
    published_ids = [a["id"] for a in articles if a["status"] == "published"]
    batch_record_views(r, published_ids, count=200)

    print("\n--- 各文章浏览量 ---")
    for aid in published_ids:
        views = get_views(r, aid)
        title = _articles_db[aid]["title"]
        print(f"  文章 {aid} [{title[:20]}...]: 浏览量 {views}")

    # ---- 功能三：热度排行榜演示 ----
    print("\n" + "=" * 60)
    print("  功能三：文章热度排行榜（ZSet）")
    print("=" * 60)

    print("\n--- 初始化排行榜 ---")
    init_hot_articles(r, hot_articles_data)

    print("\n--- 模拟用户行为（增加热度）---")
    for _ in range(500):
        aid = random.choice(published_ids)
        action = random.choices(
            ["view", "like", "comment", "collect"],
            weights=[70, 15, 10, 5],
            k=1,
        )[0]
        increase_heat(r, aid, action)

    print("\n--- 热门文章 Top 5 ---")
    top5 = get_hot_articles(r, 5)
    for rank, (aid, score) in enumerate(top5, 1):
        title = _articles_db[aid]["title"]
        views = get_views(r, aid)
        print(f"  第 {rank} 名: [{aid}] {title[:25]}... "
              f"(热度: {score}, 浏览量: {views})")

    # ---- 功能四：Session 管理演示 ----
    print("\n" + "=" * 60)
    print("  功能四：用户 Session 管理")
    print("=" * 60)

    print("\n--- 用户登录 ---")
    token = create_session(r, "1001", {
        "username": "browncutie",
        "nickname": "大数据学习者",
    })
    print(f"  Session Token: {token[:16]}...")

    print("\n--- 验证 Session ---")
    session = get_session(r, token)
    print(f"  用户名: {session['username']}")
    print(f"  昵称: {session['nickname']}")
    print(f"  登录时间: {session['login_time']}")

    ttl = r.ttl(f"demo:session:{token}")
    print(f"  Session 剩余时间: {ttl} 秒")

    print("\n--- 用户登出 ---")
    destroy_session(r, token)
    session = get_session(r, token)
    print(f"  登出后 Session: {session}")

    # ---- 功能五：分布式锁演示 ----
    print("\n" + "=" * 60)
    print("  功能五：分布式锁")
    print("=" * 60)

    print("\n--- 正常获取和释放锁 ---")
    lock_name = "publish:1001"
    lock_token = acquire_lock(r, lock_name, expire_seconds=10)
    if lock_token:
        print(f"  获取锁成功: {lock_name}")
        print(f"  Lock Token: {lock_token[:16]}...")
        print("  执行业务操作...")
        time.sleep(0.5)
        released = release_lock(r, lock_name, lock_token)
        print(f"  释放锁: {'成功' if released else '失败'}")

    print("\n--- 尝试获取已被占用的锁 ---")
    lock_token_1 = acquire_lock(r, "publish:1002", expire_seconds=10)
    if lock_token_1:
        print(f"  第一次获取锁成功")
        lock_token_2 = acquire_lock(r, "publish:1002", expire_seconds=10)
        if lock_token_2:
            print(f"  第二次获取锁成功（不应该发生）")
        else:
            print(f"  第二次获取锁失败（锁已被占用）")
        release_lock(r, "publish:1002", lock_token_1)
        print(f"  释放第一次获取的锁")

    # ---- 清理 ----
    print("\n" + "=" * 60)
    print("  演示结束")
    print("=" * 60)

    # 显示 Redis 内存使用情况
    info = r.info("memory")
    print(f"\n  Redis 内存使用: {info['used_memory_human']}")
    print(f"  Redis 键数量: {r.dbsize()}")


if __name__ == "__main__":
    main()
