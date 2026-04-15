#!/bin/bash
# ============================================================
# 数据分析项目 Redis 操作示例
# ============================================================
#
# 功能：
#   - 五大数据类型的基本操作
#   - 数据平台场景的实际命令
#
# 使用方法：
#   1. 确保 Redis 服务已启动：redis-server
#   2. 执行本脚本：bash redis_operations.sh
#
# ============================================================

# Redis 连接配置
REDIS_HOST="127.0.0.1"
REDIS_PORT="6379"
REDIS_CLI="redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT}"

# ============================================================
# 辅助函数
# ============================================================

# 执行 Redis 命令并打印
cmd() {
    echo "  > $1"
    result=$(${REDIS_CLI} $1 2>/dev/null)
    echo "  < $result"
    echo ""
}

# 打印分隔标题
title() {
    echo ""
    echo "============================================================"
    echo "  $1"
    echo "============================================================"
    echo ""
}

# ============================================================
# 前置检查
# ============================================================

echo "============================================================"
echo "  数据分析项目 Redis 操作示例"
echo "============================================================"
echo ""

# 检查 Redis 是否可用
echo "[检查] 测试 Redis 连接..."
if ! ${REDIS_CLI} PING > /dev/null 2>&1; then
    echo "[错误] 无法连接到 Redis！"
    echo "  请先启动 Redis 服务："
    echo "    redis-server --daemonize yes"
    echo "  或使用 Homebrew："
    echo "    brew services start redis"
    exit 1
fi

echo "[成功] Redis 连接正常"
PONG=$(${REDIS_CLI} PING)
echo "  PING → ${PONG}"

# ============================================================
# 一、String（字符串）操作
# ============================================================

title "一、String（字符串）操作"

echo "--- 1.1 文章缓存 ---"
cmd "SET demo:article:1001 '{\"title\":\"Redis入门教程\",\"author\":\"browncutie\",\"views\":0}'"
cmd "GET demo:article:1001"
cmd "EXPIRE demo:article:1001 3600"
cmd "TTL demo:article:1001"

echo "--- 1.2 浏览量计数 ---"
cmd "SET demo:views:1001 0"
cmd "INCR demo:views:1001"
cmd "INCR demo:views:1001"
cmd "INCR demo:views:1001"
cmd "INCRBY demo:views:1001 10"
cmd "GET demo:views:1001"

echo "--- 1.3 点赞计数 ---"
cmd "SET demo:likes:1001 42"
cmd "INCR demo:likes:1001"
cmd "DECR demo:likes:1001"
cmd "GET demo:likes:1001"

echo "--- 1.4 分布式锁（SETNX）---"
cmd "SETNX demo:lock:publish:1001 'token_abc123'"
cmd "SETNX demo:lock:publish:1001 'token_xyz789'"
cmd "DEL demo:lock:publish:1001"

echo "--- 1.5 Session 管理 ---"
cmd "SET 'demo:session:abc123' '{\"user_id\":1001,\"username\":\"browncutie\"}' EX 7200"
cmd "GET 'demo:session:abc123'"

# ============================================================
# 二、Hash（哈希）操作
# ============================================================

title "二、Hash（哈希）操作"

echo "--- 2.1 用户信息存储 ---"
cmd "HSET user:1001 username browncutie"
cmd "HSET user:1001 nickname 大数据学习者"
cmd "HSET user:1001 email b@example.com"
cmd "HSET user:1001 register_date 2024-01-01"
cmd "HSET user:1001 article_count 15"

echo "--- 2.2 读取用户信息 ---"
cmd "HGET user:1001 username"
cmd "HGETALL user:1001"
cmd "HKEYS user:1001"
cmd "HLEN user:1001"

echo "--- 2.3 修改用户信息 ---"
cmd "HSET user:1001 bio 热爱大数据和云计算"
cmd "HGET user:1001 bio"

echo "--- 2.4 文章详情（Hash 方式）---"
cmd "HSET article:1001 title Redis入门教程"
cmd "HSET article:1001 author browncutie"
cmd "HSET article:1001 category 大数据"
cmd "HSET article:1001 views 0"
cmd "HSET article:1001 likes 0"
cmd "HSET article:1001 status published"
cmd "HMGET article:1001 title author category"
cmd "HINCRBY article:1001 views 1"
cmd "HINCRBY article:1001 likes 1"

echo "--- 2.5 批量设置多个字段 ---"
cmd "HMSET user:1002 username alice nickname Alice粉丝 email alice@example.com"

# ============================================================
# 三、List（列表）操作
# ============================================================

title "三、List（列表）操作"

echo "--- 3.1 最新文章列表 ---"
cmd "LPUSH demo:latest_posts 1003:Kafka深度解析"
cmd "LPUSH demo:latest_posts 1002:Spark实战指南"
cmd "LPUSH demo:latest_posts 1001:Redis入门教程"
cmd "LLEN demo:latest_posts"
cmd "LRANGE demo:latest_posts 0 -1"
cmd "LRANGE demo:latest_posts 0 2"

echo "--- 3.2 保持最新 N 篇 ---"
cmd "LTRIM demo:latest_posts 0 4"
cmd "LLEN demo:latest_posts"

echo "--- 3.3 简单消息队列 ---"
cmd "LPUSH demo:email_queue '{\"to\":\"user@example.com\",\"subject\":\"欢迎注册\"}'"
cmd "LPUSH demo:email_queue '{\"to\":\"user@example.com\",\"subject\":\"文章有新评论\"}'"
cmd "RPOP demo:email_queue"
cmd "RPOP demo:email_queue"
cmd "LLEN demo:email_queue"

echo "--- 3.4 栈操作（LIFO）---"
cmd "LPUSH demo:undo_stack '操作1:删除文章'"
cmd "LPUSH demo:undo_stack '操作2:修改标题'"
cmd "LPOP demo:undo_stack"

# ============================================================
# 四、Set（集合）操作
# ============================================================

title "四、Set（集合）操作"

echo "--- 4.1 文章标签 ---"
cmd "SADD article:1001:tags Redis 缓存 内存数据库 NoSQL"
cmd "SADD article:1002:tags Spark 大数据 分布式计算"
cmd "SADD article:1003:tags Kafka 消息队列 大数据 NoSQL"
cmd "SMEMBERS article:1001:tags"
cmd "SCARD article:1001:tags"
cmd "SISMEMBER article:1001:tags Redis"
cmd "SISMEMBER article:1001:tags Spark"

echo "--- 4.2 共同标签（交集）---"
cmd "SINTER article:1001:tags article:1003:tags"

echo "--- 4.3 所有标签（并集）---"
cmd "SUNION article:1001:tags article:1002:tags article:1003:tags"

echo "--- 4.4 差集（文章1001有但文章1002没有的标签）---"
cmd "SDIFF article:1001:tags article:1002:tags"

echo "--- 4.5 用户关注 ---"
cmd "SADD user:1001:following 2001 2002 2003 2004"
cmd "SADD user:1002:following 2002 2003 2005 2006"
cmd "SINTER user:1001:following user:1002:following"
echo "  （共同关注：2002, 2003）"

echo "--- 4.6 文章点赞（去重）---"
cmd "SADD article:1001:likers user:1001 user:1002 user:1003 user:1001"
cmd "SCARD article:1001:likers"
cmd "SISMEMBER article:1001:likers user:1001"
cmd "SREM article:1001:likers user:1001"
cmd "SCARD article:1001:likers"

# ============================================================
# 五、ZSet（有序集合）操作
# ============================================================

title "五、ZSet（有序集合）操作"

echo "--- 5.1 初始化文章热度 ---"
cmd "ZADD demo:hot_articles 100 1001"
cmd "ZADD demo:hot_articles 200 1002"
cmd "ZADD demo:hot_articles 150 1003"
cmd "ZADD demo:hot_articles 80 1004"
cmd "ZADD demo:hot_articles 50 1005"

echo "--- 5.2 热度自增 ---"
cmd "ZINCRBY demo:hot_articles 1 1001"
cmd "ZINCRBY demo:hot_articles 1 1001"
cmd "ZINCRBY demo:hot_articles 5 1001"
cmd "ZINCRBY demo:hot_articles 10 1003"

echo "--- 5.3 热门文章排行榜（从高到低）---"
cmd "ZREVRANGE demo:hot_articles 0 -1 WITHSCORES"
cmd "ZREVRANGE demo:hot_articles 0 2 WITHSCORES"

echo "--- 5.4 查询排名和分数 ---"
cmd "ZREVRANK demo:hot_articles 1001"
cmd "ZSCORE demo:hot_articles 1001"
cmd "ZCARD demo:hot_articles"

echo "--- 5.5 按分数范围查询 ---"
cmd "ZREVRANGEBYSCORE demo:hot_articles 100 +inf"
cmd "ZCOUNT demo:hot_articles 100 200"

echo "--- 5.6 删除元素 ---"
cmd "ZREM demo:hot_articles 1005"
cmd "ZCARD demo:hot_articles"

# ============================================================
# 六、数据平台场景综合操作
# ============================================================

title "六、数据平台场景综合操作"

echo "--- 6.1 文章发布（事务操作）---"
${REDIS_CLI} MULTI > /dev/null 2>&1
${REDIS_CLI} HSET article:2001 title "新文章：Hadoop入门" > /dev/null 2>&1
${REDIS_CLI} HSET article:2001 author browncutie > /dev/null 2>&1
${REDIS_CLI} HSET article:2001 views 0 > /dev/null 2>&1
${REDIS_CLI} LPUSH demo:latest_posts "2001:新文章：Hadoop入门" > /dev/null 2>&1
${REDIS_CLI} LTRIM demo:latest_posts 0 49 > /dev/null 2>&1
echo "  > MULTI"
echo "  > HSET article:2001 title '新文章：Hadoop入门'"
echo "  > HSET article:2001 author browncutie"
echo "  > HSET article:2001 views 0"
echo "  > LPUSH demo:latest_posts '2001:新文章'"
echo "  > LTRIM demo:latest_posts 0 49"
echo "  > EXEC"
result=$(${REDIS_CLI} EXEC 2>/dev/null)
echo "  < ${result}"
echo ""

echo "--- 6.2 模拟用户浏览流程 ---"
echo "  用户浏览文章 1001..."
${REDIS_CLI} INCR demo:views:1001 > /dev/null 2>&1
${REDIS_CLI} ZINCRBY demo:hot_articles 1 1001 > /dev/null 2>&1
echo "  用户点赞文章 1001..."
${REDIS_CLI} HINCRBY article:1001 likes 1 > /dev/null 2>&1
${REDIS_CLI} ZINCRBY demo:hot_articles 5 1001 > /dev/null 2>&1
echo "  用户评论文章 1001..."
${REDIS_CLI} ZINCRBY demo:hot_articles 10 1001 > /dev/null 2>&1

echo "  当前文章 1001 状态："
views=$(${REDIS_CLI} GET demo:views:1001 2>/dev/null)
likes=$(${REDIS_CLI} HGET article:1001 likes 2>/dev/null)
heat=$(${REDIS_CLI} ZSCORE demo:hot_articles 1001 2>/dev/null)
rank=$(${REDIS_CLI} ZREVRANK demo:hot_articles 1001 2>/dev/null)
echo "    浏览量: ${views}"
echo "    点赞数: ${likes}"
echo "    热度: ${heat}"
echo "    排名: 第 $((rank + 1)) 名"
echo ""

# ============================================================
# 七、运维操作
# ============================================================

title "七、运维操作"

echo "--- 7.1 数据库信息 ---"
cmd "DBSIZE"
cmd "INFO server | grep redis_version"
cmd "INFO memory | grep used_memory_human"

echo "--- 7.2 查看所有 demo 相关的键 ---"
echo "  > KEYS demo:*"
${REDIS_CLI} KEYS "demo:*" 2>/dev/null | head -20 | while read key; do
    echo "    ${key}"
done
echo ""

echo "--- 7.3 缓存命中统计 ---"
cmd "INFO stats | grep keyspace_hits"
cmd "INFO stats | grep keyspace_misses"

echo "--- 7.4 清理演示数据（可选）---"
echo "  如需清理，执行："
echo "    ${REDIS_CLI} FLUSHDB"
echo ""
echo "  注意：FLUSHDB 会清空当前数据库的所有数据！"

# ============================================================
# 结束
# ============================================================

echo ""
echo "============================================================"
echo "  操作演示结束"
echo "============================================================"
echo ""
echo "  提示："
echo "  - 使用 redis-cli 进入交互模式"
echo "  - 使用 MONITOR 命令实时监控所有操作"
echo "  - 使用 SLOWLOG GET 10 查看慢查询"
echo ""
