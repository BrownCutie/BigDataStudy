#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hadoop Streaming WordCount - Reducer

功能：从标准输入读取 Mapper 的输出（已按 key 排序），对相同单词的计数进行累加
输入格式：每行一个键值对，格式为 <单词\t计数>（制表符分隔）
输出格式：每行一个键值对，格式为 <单词\t总次数>

重要：Hadoop Streaming 会自动将相同 key 的所有行分组在一起传递给 Reducer，
      即 Reducer 接收到的输入是按 key 排序且分组的。

使用方式（配合 Hadoop Streaming）：
  hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\
    -input /input/path \\
    -output /output/path \\
    -mapper mapper.py \\
    -reducer reducer.py \\
    -file mapper.py \\
    -file reducer.py

单独测试：
  cat mapper_output.txt | sort | python3 reducer.py
"""

import sys

def main():
    """
    主函数：读取已排序的 Mapper 输出，对相同单词的计数进行累加
    """
    current_word = None   # 当前正在处理的单词
    current_count = 0     # 当前单词的累计计数

    # 从标准输入逐行读取
    for line in sys.stdin:
        # 去除行首行尾的空白字符
        line = line.strip()

        # 如果行为空则跳过
        if not line:
            continue

        # 按制表符分割，获取单词和计数值
        parts = line.split('\t')
        if len(parts) != 2:
            # 如果格式不正确，跳过该行
            continue

        word, count_str = parts

        try:
            count = int(count_str)
        except ValueError:
            # 如果计数不是有效整数，跳过该行
            continue

        # 如果是新的单词，先输出上一个单词的结果
        if current_word != word:
            if current_word is not None:
                # 输出上一个单词的统计结果
                print(f"{current_word}\t{current_count}")
            # 切换到新的单词
            current_word = word
            current_count = 0

        # 累加当前单词的计数
        current_count += count

    # 输出最后一个单词的统计结果
    if current_word is not None:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    main()
