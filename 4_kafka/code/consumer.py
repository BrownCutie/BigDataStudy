#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
博客事件消费者 —— Kafka 实操示例

功能：
- 从 Kafka 消费博客事件
- 按事件类型统计（浏览、评论、点赞、搜索）
- 简单的实时统计展示
- 使用 kafka-python 库

依赖安装：pip install kafka-python
"""

import json
import logging
from collections import defaultdict
from kafka import KafkaConsumer

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka 连接配置
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'blog-events'
GROUP_ID = 'demo-event-consumer'


class BlogEventConsumer:
    """博客事件消费者

    功能：
    - 消费 blog-events Topic 中的博客事件
    - 按事件类型统计数量
    - 统计每篇文章的浏览量
    - 支持优雅关闭
    """

    def __init__(self, bootstrap_servers=BOOTSTRAP_SERVERS,
                 topic=TOPIC, group_id=GROUP_ID):
        """初始化 Kafka 消费者

        Args:
            bootstrap_servers: Kafka 集群地址
            topic: 订阅的 Topic 名称
            group_id: 消费者组 ID
        """
        self.topic = topic
        # 事件类型统计计数器
        self.event_counts = defaultdict(int)
        # 文章浏览量统计
        self.post_views = defaultdict(int)
        # 文章评论数统计
        self.post_comments = defaultdict(int)
        # 文章点赞数统计
        self.post_likes = defaultdict(int)
        # 搜索关键词统计
        self.search_keywords = defaultdict(int)
        # 已处理的总事件数
        self.total_events = 0

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            # 从最早的消息开始消费（首次启动时）
            auto_offset_reset='earliest',
            # 关闭自动提交 offset，改为手动提交
            enable_auto_commit=False,
            # 每次最多拉取 100 条消息
            max_poll_records=100,
            # 使用增量 Rebalance 策略
            partition_assignment_strategy=[
                'org.apache.kafka.clients.consumer'
                '.CooperativeStickyAssignor'
            ],
            # JSON 反序列化
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info(f"消费者初始化完成，Topic: {topic}, Group: {group_id}")

    def start(self):
        """启动消费循环，持续消费消息"""
        logger.info("===== 博客事件消费者启动，等待消息... =====")
        logger.info("按 Ctrl+C 停止消费")

        try:
            for message in self.consumer:
                try:
                    # 处理消息
                    self._process_event(message.value)

                    # 处理成功，手动提交 offset
                    self.consumer.commit()

                    # 每处理 10 条消息输出一次统计
                    if self.total_events % 10 == 0:
                        self._print_stats()

                except json.JSONDecodeError as e:
                    logger.error(f"JSON 解析失败: {e}")
                except KeyError as e:
                    logger.error(f"事件格式错误，缺少字段: {e}")
                except Exception as e:
                    logger.error(f"处理消息失败: {e}")
                    # 不提交 offset，下次消费时还会拉到这条消息

        except KeyboardInterrupt:
            logger.info("收到中断信号，停止消费")
        finally:
            self.consumer.close()
            logger.info("消费者已关闭")
            # 输出最终统计
            self._print_stats()

    def _process_event(self, event):
        """处理单条事件

        Args:
            event: 事件字典，包含 eventType 和 data 字段
        """
        event_type = event.get('eventType', 'unknown')
        data = event.get('data', {})

        # 按事件类型统计
        self.event_counts[event_type] += 1
        self.total_events += 1

        # 根据事件类型做具体统计
        if event_type == 'view':
            # 浏览事件：统计每篇文章的浏览量
            post_id = data.get('postId')
            user_id = data.get('userId')
            if post_id:
                self.post_views[post_id] += 1
            logger.info(f"[浏览] 文章ID={post_id}, 用户={user_id}")

        elif event_type == 'comment':
            # 评论事件：统计每篇文章的评论数
            post_id = data.get('postId')
            user_id = data.get('userId')
            content = data.get('content', '')
            if post_id:
                self.post_comments[post_id] += 1
            logger.info(f"[评论] 文章ID={post_id}, 用户={user_id}, 内容={content[:20]}")

        elif event_type == 'like':
            # 点赞事件：统计每篇文章的点赞数
            post_id = data.get('postId')
            user_id = data.get('userId')
            if post_id:
                self.post_likes[post_id] += 1
            logger.info(f"[点赞] 文章ID={post_id}, 用户={user_id}")

        elif event_type == 'search':
            # 搜索事件：统计搜索关键词
            keyword = data.get('keyword', '')
            user_id = data.get('userId')
            if keyword:
                self.search_keywords[keyword] += 1
            logger.info(f"[搜索] 用户={user_id}, 关键词={keyword}")

        else:
            logger.warning(f"未知事件类型: {event_type}")

    def _print_stats(self):
        """输出当前统计信息"""
        print("\n" + "=" * 60)
        print(f"  博客事件实时统计（共处理 {self.total_events} 条事件）")
        print("=" * 60)

        # 事件类型统计
        print("\n【事件类型统计】")
        for event_type, count in sorted(self.event_counts.items()):
            print(f"  {event_type:12s} : {count} 条")

        # 文章浏览量 Top 5
        if self.post_views:
            print("\n【文章浏览量 Top 5】")
            sorted_views = sorted(self.post_views.items(), key=lambda x: x[1], reverse=True)[:5]
            for post_id, count in sorted_views:
                print(f"  文章 {post_id:4s} : {count} 次浏览")

        # 文章评论数
        if self.post_comments:
            print("\n【文章评论数】")
            for post_id, count in sorted(self.post_comments.items()):
                print(f"  文章 {post_id:4s} : {count} 条评论")

        # 文章点赞数
        if self.post_likes:
            print("\n【文章点赞数】")
            for post_id, count in sorted(self.post_likes.items()):
                print(f"  文章 {post_id:4s} : {count} 个点赞")

        # 搜索热词
        if self.search_keywords:
            print("\n【搜索热词】")
            sorted_keywords = sorted(self.search_keywords.items(), key=lambda x: x[1], reverse=True)[:5]
            for keyword, count in sorted_keywords:
                print(f"  {keyword:20s} : {count} 次")

        print("\n" + "=" * 60)


def main():
    """主函数：启动消费者"""
    consumer = BlogEventConsumer()
    consumer.start()


if __name__ == '__main__':
    main()
