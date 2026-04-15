#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hadoop Streaming WordCount - Mapper

功能：从标准输入读取文本，将每行拆分为单词，输出 <单词, 1> 键值对
输出格式：每个单词占一行，单词和计数之间用制表符分隔

使用方式（配合 Hadoop Streaming）：
  hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -input /input/path \\
    -output /output/path \\
    -mapper mapper.py \\
    -reducer reducer.py \\
    -file mapper.py \\
    -file reducer.py

单独测试：
  cat input.txt | python3 mapper.py
"""

import sys

def main():
    """
    主函数：逐行读取标准输入，拆分单词并输出
    """
    # 从标准输入逐行读取
    for line in sys.stdin:
        # 去除行首行尾的空白字符
        line = line.strip()

        # 如果行为空则跳过
        if not line:
            continue

        # 按空白字符（空格、制表符等）拆分单词
        words = line.split()

        # 输出每个单词及其计数值 1
        # 格式：单词\t1（制表符分隔）
        for word in words:
            # 输出到标准输出（Hadoop Streaming 会自动收集并按 key 排序传递给 Reducer）
            print(f"{word}\t1")

if __name__ == "__main__":
    main()
