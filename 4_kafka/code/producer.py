#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
博客事件生产者 —— Kafka 实操示例

功能：
- 向 Kafka 发送博客事件（浏览、评论、点赞、搜索）
- 使用 kafka-python 库
- Kafka 4.2.0 KRaft 模式，端口 9092

依赖安装：pip install kafka-python
"""

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka 连接配置
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'blog-events'


class BlogEventProducer:
    """博客事件生产者

    支持发送四种博客事件类型：
    - view（浏览）
    - comment（评论）
    - like（点赞）
    - search（搜索）
    """

    def __init__(self, bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC):
        """初始化 Kafka 生产者

        Args:
            bootstrap_servers: Kafka 集群地址
            topic: 目标 Topic 名称
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            # 可靠性配置：所有 ISR 副本确认
            acks='all',
            # 失败重试 3 次
            retries=3,
            # 重试间隔 100ms
            retry_backoff_ms=100,
            # 开启幂等性，防止重试导致重复消息
            enable_idempotence=True,
            # 性能配置
            batch_size=32768,          # 32KB 批量
            linger_ms=10,              # 最多等 10ms 凑批
            compression_type='lz4',    # LZ4 压缩
            buffer_memory=67108864,    # 64MB 缓冲区
            # 序列化配置
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        logger.info(f"生产者初始化完成，目标 Topic: {topic}")

    def _create_event(self, event_type, data, key=None):
        """创建统一格式的事件消息

        Args:
            event_type: 事件类型（view/comment/like/search）
            data: 事件数据字典
            key: 消息 Key（用于分区路由）

        Returns:
            事件字典
        """
        return {
            'eventId': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'eventType': event_type,
            'version': '1.0',
            'timestamp': datetime.now().isoformat(),
            'source': 'python-producer',
            'data': data
        }

    def send_view_event(self, post_id, user_id, ip=None):
        """发送浏览事件

        Args:
            post_id: 文章 ID
            user_id: 用户 ID
            ip: 用户 IP 地址（可选）
        """
        event = self._create_event(
            event_type='view',
            data={
                'postId': post_id,
                'userId': user_id,
                'ip': ip or f"192.168.1.{random.randint(1, 255)}"
            },
            key=str(post_id)  # 用 postId 作为 Key，保证同一文章事件有序
        )
        self._send(event)
        logger.info(f"发送浏览事件: postId={post_id}, userId={user_id}")

    def send_comment_event(self, post_id, user_id, content):
        """发送评论事件

        Args:
            post_id: 文章 ID
            user_id: 用户 ID
            content: 评论内容
        """
        event = self._create_event(
            event_type='comment',
            data={
                'postId': post_id,
                'userId': user_id,
                'content': content
            },
            key=str(post_id)  # 同一文章的评论事件有序
        )
        self._send(event)
        logger.info(f"发送评论事件: postId={post_id}, userId={user_id}")

    def send_like_event(self, post_id, user_id):
        """发送点赞事件

        Args:
            post_id: 文章 ID
            user_id: 用户 ID
        """
        event = self._create_event(
            event_type='like',
            data={
                'postId': post_id,
                'userId': user_id
            },
            key=str(post_id)
        )
        self._send(event)
        logger.info(f"发送点赞事件: postId={post_id}, userId={user_id}")

    def send_search_event(self, user_id, keyword):
        """发送搜索事件

        Args:
            user_id: 用户 ID
            keyword: 搜索关键词
        """
        event = self._create_event(
            event_type='search',
            data={
                'userId': user_id,
                'keyword': keyword
            },
            key=None  # 搜索事件不需要 Key，轮询分配即可
        )
        self._send(event)
        logger.info(f"发送搜索事件: userId={user_id}, keyword={keyword}")

    def _send(self, event):
        """发送事件到 Kafka（带回调）

        Args:
            event: 事件字典
        """
        future = self.producer.send(
            self.topic,
            key=event.get('_key'),
            value=event
        )
        # 添加回调
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)

    @staticmethod
    def _on_success(record_metadata):
        """发送成功回调"""
        logger.debug(
            f"消息发送成功: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}"
        )

    @staticmethod
    def _on_error(excp):
        """发送失败回调"""
        logger.error(f"消息发送失败: {excp}")

    def close(self):
        """关闭生产者，刷写缓冲区中的所有消息"""
        self.producer.flush()
        self.producer.close()
        logger.info("生产者已关闭")


def main():
    """主函数：发送测试数据"""
    producer = BlogEventProducer()

    try:
        logger.info("===== 开始发送博客事件 =====")

        # 模拟用户浏览行为
        # 用户 alice 浏览了文章 1
        producer.send_view_event(post_id=1, user_id='alice', ip='192.168.1.100')
        time.sleep(0.1)

        # 用户 bob 浏览了文章 2
        producer.send_view_event(post_id=2, user_id='bob', ip='192.168.1.101')
        time.sleep(0.1)

        # 用户 charlie 评论了文章 1
        producer.send_comment_event(post_id=1, user_id='charlie', content='写得太好了！')
        time.sleep(0.1)

        # 用户 alice 点赞了文章 1
        producer.send_like_event(post_id=1, user_id='alice')
        time.sleep(0.1)

        # 用户 david 浏览了文章 1
        producer.send_view_event(post_id=1, user_id='david', ip='192.168.1.102')
        time.sleep(0.1)

        # 用户 eve 搜索了关键词
        producer.send_search_event(user_id='eve', keyword='Kafka 入门')
        time.sleep(0.1)

        # 用户 bob 评论了文章 2
        producer.send_comment_event(post_id=2, user_id='bob', content='学到了很多')
        time.sleep(0.1)

        # 用户 frank 浏览了文章 3
        producer.send_view_event(post_id=3, user_id='frank', ip='192.168.1.103')
        time.sleep(0.1)

        # 用户 grace 搜索了关键词
        producer.send_search_event(user_id='grace', keyword='Flink 实时计算')
        time.sleep(0.1)

        # 用户 alice 点赞了文章 2
        producer.send_like_event(post_id=2, user_id='alice')

        logger.info("===== 所有事件发送完毕 =====")

    finally:
        producer.close()


if __name__ == '__main__':
    main()
