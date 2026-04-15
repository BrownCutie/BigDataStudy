# 06 - MapReduce 编程模型

## 一、MapReduce 是什么？

### 1.1 从一个例子说起

假设你有一台普通电脑，需要统计 100GB 文本中每个单词出现的次数。

```
方案 1: 单机处理
────────────────────────────────────────────
读取 100GB 文件 → 逐行解析 → 统计单词 → 输出结果
预计耗时: 约 10 小时（受限于单机 CPU 和磁盘 I/O）


方案 2: MapReduce 分布式处理
────────────────────────────────────────────
将 100GB 文件拆分成 100 份，每份 1GB
分配给 100 台机器并行处理
每台机器统计自己那份文件的单词数
最后将 100 台机器的结果汇总

预计耗时: 约 10 分钟（线性加速）
```

这就是 MapReduce 的核心思想：**分而治之（Divide and Conquer）**。

### 1.2 MapReduce 的定义

**MapReduce** 是一种分布式计算编程模型，由 Google 在 2004 年发表的同名论文中提出。它将复杂的分布式计算过程抽象为两个函数：

- **Map**：对每条数据独立处理，产生中间结果
- **Reduce**：对中间结果进行聚合，得到最终结果

```
┌──────────────────────────────────────────────────────────────┐
│              MapReduce 的核心抽象                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  输入: 一组键值对 (key1, value1), (key2, value2), ...        │
│                                                              │
│  Map 阶段:  对每个键值对独立处理                               │
│  ┌────────────────────────────────────────────────────┐     │
│  │  (key1, value1) → Map → [(k1, v1), (k2, v2), ...] │     │
│  │  (key2, value2) → Map → [(k1, v1), (k3, v3), ...] │     │
│  │  (key3, value3) → Map → [(k2, v2), (k4, v4), ...] │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  Shuffle 阶段: 按 key 分组                                   │
│  ┌────────────────────────────────────────────────────┐     │
│  │  k1 → [v1, v1]                                     │     │
│  │  k2 → [v2, v2, v2]                                 │     │
│  │  k3 → [v3]                                         │     │
│  │  k4 → [v4]                                         │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  Reduce 阶段: 对每个 key 的值列表进行聚合                     │
│  ┌────────────────────────────────────────────────────┐     │
│  │  (k1, [v1, v1]) → Reduce → (k1, result1)          │     │
│  │  (k2, [v2, v2, v2]) → Reduce → (k2, result2)      │     │
│  │  (k3, [v3]) → Reduce → (k3, result3)              │     │
│  │  (k4, [v4]) → Reduce → (k4, result4)              │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  输出: 一组键值对 (k1, result1), (k2, result2), ...          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 1.3 为什么需要 MapReduce？

```
┌──────────────────────────────────────────────────────────────┐
│              MapReduce 解决的问题                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 自动并行化                                               │
│     开发者只需写 Map 和 Reduce 函数                          │
│     框架自动将任务分配到多台机器并行执行                     │
│                                                              │
│  2. 自动容错                                                 │
│     某台机器挂了，框架自动在另一台机器上重新执行              │
│     开发者不需要处理分布式容错逻辑                           │
│                                                              │
│  3. 屏蔽分布式细节                                           │
│     数据分片、任务调度、网络通信、故障恢复                     │
│     全部由框架处理，开发者专注于业务逻辑                     │
│                                                              │
│  4. 横向扩展                                                 │
│     加机器就能加快计算速度（理论上线性加速）                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 二、Map/Shuffle/Reduce 三阶段详解

### 2.1 完整流程图

```
┌──────────────────────────────────────────────────────────────────────┐
│                    MapReduce 完整执行流程                              │
│                                                                      │
│  ┌─────────┐                                                         │
│  │ 输入数据 │                                                         │
│  │ (HDFS)  │                                                         │
│  └────┬────┘                                                         │
│       │                                                               │
│       │ 分片 (InputSplit)                                             │
│       │ 每个 Split 对应一个 Map Task                                  │
│       │                                                               │
│  ┌────▼─────────────────────────────────────────────────────────┐    │
│  │                                                              │    │
│  │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │    │
│  │  │ Map 1    │   │ Map 2    │   │ Map 3    │   │ Map N    │  │    │
│  │  │          │   │          │   │          │   │          │  │    │
│  │  │ 输入:    │   │ 输入:    │   │ 输入:    │   │ 输入:    │  │    │
│  │  │ Split 1  │   │ Split 2  │   │ Split 3  │   │ Split N  │  │    │
│  │  │          │   │          │   │          │   │          │  │    │
│  │  │ 输出:    │   │ 输出:    │   │ 输出:    │   │ 输出:    │  │    │
│  │  │ <k1,v1>  │   │ <k2,v1>  │   │ <k1,v2>  │   │ <k3,v1>  │  │    │
│  │  │ <k2,v2>  │   │ <k1,v3>  │   │ <k3,v2>  │   │ <k2,v4>  │  │    │
│  │  │ <k1,v4>  │   │ <k3,v3>  │   │ <k2,v5>  │   │ <k1,v5>  │  │    │
│  │  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘  │    │
│  │       │              │              │              │         │    │
│  └───────┼──────────────┼──────────────┼──────────────┼─────────┘    │
│          │              │              │              │              │
│  ┌───────▼──────────────▼──────────────▼──────────────▼─────────┐    │
│  │                   Shuffle & Sort                             │    │
│  │                                                              │    │
│  │   ① Partition: 按 key 的 hash 值分配到不同的 Reduce           │    │
│  │   ② Sort:     每个 Partition 内按 key 排序                   │    │
│  │   ③ Group:    相同 key 的 value 分组在一起                    │    │
│  │                                                              │    │
│  │   Partition 1 (Reduce 1):          Partition 2 (Reduce 2):   │    │
│  │   ┌────────────────────┐          ┌────────────────────┐    │    │
│  │   │ k1 → [v1,v3,v4,v5] │          │ k2 → [v2,v4,v5]    │    │    │
│  │   │ k3 → [v1,v2,v3]    │          │                    │    │    │
│  │   └────────────────────┘          └────────────────────┘    │    │
│  │                                                              │    │
│  └──────────┬──────────────────────────┬────────────────────────┘    │
│             │                          │                             │
│  ┌──────────▼──────────┐  ┌────────────▼──────────────┐             │
│  │                     │  │                           │             │
│  │   Reduce 1          │  │   Reduce 2                │             │
│  │                     │  │                           │             │
│  │   输入:             │  │   输入:                    │             │
│  │   k1 → [v1,v3,v4,v5]│  │   k2 → [v2,v4,v5]        │             │
│  │   k3 → [v1,v2,v3]   │  │                           │             │
│  │                     │  │   输出:                    │             │
│  │   输出:             │  │   k2 → result2             │             │
│  │   k1 → result1      │  │                           │             │
│  │   k3 → result3      │  │                           │             │
│  │                     │  │                           │             │
│  └─────────┬───────────┘  └────────────┬──────────────┘             │
│            │                           │                            │
│            └───────────┬───────────────┘                            │
│                        │                                            │
│                  ┌─────▼─────┐                                      │
│                  │  输出结果  │                                      │
│                  │  (HDFS)   │                                      │
│                  └───────────┘                                      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 Map 阶段

**Map 阶段的工作：**

1. **输入分片（InputSplit）**：Hadoop 将输入文件按逻辑切分成多个 Split，每个 Split 对应一个 Map Task
2. **逐条处理**：Map Task 从 Split 中逐条读取记录（Record）
3. **调用 map() 函数**：对每条记录调用用户自定义的 `map()` 方法
4. **输出键值对**：map() 方法输出零个或多个键值对

```java
// Map 阶段的输入输出
// 输入: 一行文本（key 是偏移量，value 是文本内容）
// 输出: (单词, 1) 的键值对

// 示例输入: "hello world hello"
// 偏移量 0: "hello" → ("hello", 1)
// 偏移量 6: "world" → ("world", 1)
// 偏移量 12: "hello" → ("hello", 1)
```

### 2.3 Shuffle 阶段

Shuffle 是 MapReduce 的**核心和灵魂**，也是最难理解的部分。它负责将 Map 的输出按照 key 分组并排序后传递给 Reduce。

```
┌──────────────────────────────────────────────────────────────┐
│              Shuffle 详细过程                                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Map 端 (Shuffle Write):                                     │
│  ┌────────────────────────────────────────────────────┐     │
│  │                                                    │     │
│  │  1. Map 输出写入环形缓冲区 (默认 100MB)            │     │
│  │     ┌──────────────────────────────────────────┐   │     │
│  │     │  数据区  │  索引区  │                     │   │     │
│  │     │  80%     │  20%    │                     │   │     │
│  │     └──────────────────────────────────────────┘   │     │
│  │                                                    │     │
│  │  2. 缓冲区达到阈值(80%)后，Spill 到磁盘            │     │
│  │     → 按 Partition 分区                           │     │
│  │     → 每个 Partition 内按 key 排序                │     │
│  │                                                    │     │
│  │  3. Map 结束后，合并所有 Spill 文件               │     │
│  │     → 每个 Partition 生成一个最终文件              │     │
│  │                                                    │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  Reduce 端 (Shuffle Read):                                   │
│  ┌────────────────────────────────────────────────────┐     │
│  │                                                    │     │
│  │  4. Reduce 通过 HTTP 从各 Map Task 拉取数据        │     │
│  │     (这就是为什么叫 mapreduce_shuffle 服务)         │     │
│  │                                                    │     │
│  │  5. 合并来自不同 Map 的数据文件                    │     │
│  │     → 按 key 排序                                  │     │
│  │     → 相同 key 的 value 聚合在一起                  │     │
│  │                                                    │     │
│  │  6. 传递给 Reduce 函数处理                          │     │
│  │                                                    │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 2.4 Reduce 阶段

**Reduce 阶段的工作：**

1. 接收 Shuffle 传来的已排序、已分组的键值对
2. 对每个 key 调用 `reduce()` 方法
3. 输出最终结果到 HDFS

```java
// Reduce 阶段的输入输出
// 输入: (key, [value1, value2, ...])
// 输出: (key, result)

// 示例: ("hello", [1, 1, 1]) → ("hello", 3)
```

## 三、WordCount 案例逐行代码讲解

### 3.1 完整代码

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WordCount - MapReduce 的 "Hello World"
 *
 * 功能: 统计输入文件中每个单词出现的次数
 *
 * 输入文件示例:
 *   hello world
 *   hello hadoop
 *
 * 输出结果示例:
 *   hadoop  1
 *   hello   2
 *   world   1
 */
public class WordCount {

    /**
     * Mapper 类
     *
     * 输入: (行偏移量, 一行文本)
     * 输出: (单词, 1)
     *
     * 泛型说明:
     *   LongWritable    - Hadoop 的 long 类型（行偏移量）
     *   Text            - Hadoop 的 String 类型（一行文本）
     *   Text            - Hadoop 的 String 类型（单词）
     *   IntWritable     - Hadoop 的 int 类型（计数 1）
     */
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        // 创建一个 IntWritable 对象，值为 1
        // 为什么不在 map() 中每次 new？因为 map() 会被调用千万次，
        // 复用对象可以减少 GC 压力
        private final static IntWritable one = new IntWritable(1);

        // 创建一个 Text 对象用于存放单词
        private Text word = new Text();

        /**
         * map() 方法 - 每输入一行文本调用一次
         *
         * @param key     行在文件中的起始偏移量（字节位置）
         * @param value   这一行的文本内容
         * @param context 上下文对象，用于输出键值对
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 将这一行文本转为 Java String，然后按空白字符分割
            String line = value.toString();
            String[] words = line.split("\\s+");

            // 遍历每个单词，输出 (单词, 1)
            for (String w : words) {
                if (w.length() > 0) {  // 跳过空字符串
                    word.set(w);       // 设置单词
                    context.write(word, one);  // 输出键值对
                }
            }

            // 示例: 输入 "hello world hello"
            // 输出: ("hello", 1), ("world", 1), ("hello", 1)
        }
    }

    /**
     * Reducer 类
     *
     * 输入: (单词, [1, 1, 1, ...])
     * 输出: (单词, 总次数)
     *
     * 泛型说明:
     *   Text        - 单词（与 Mapper 输出的 key 类型一致）
     *   IntWritable - 计数值列表（与 Mapper 输出的 value 类型一致）
     *   Text        - 输出的 key 类型（单词）
     *   IntWritable - 输出的 value 类型（总次数）
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 用于存放求和结果的对象
        private IntWritable result = new IntWritable();

        /**
         * reduce() 方法 - 每个唯一的 key 调用一次
         *
         * @param key     单词
         * @param values  这个单词对应的所有计数值（可能来自多个 Map Task）
         * @param context 上下文对象，用于输出结果
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            int sum = 0;

            // 遍历所有的值，求和
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);          // 设置求和结果
            context.write(key, result);  // 输出 (单词, 总次数)

            // 示例: 输入 ("hello", [1, 1, 1])
            // 输出: ("hello", 3)
        }
    }

    /**
     * main 方法 - 作业的入口
     */
    public static void main(String[] args) throws Exception {

        // 创建 Hadoop 配置对象
        Configuration conf = new Configuration();

        // 创建 Job 对象（作业）
        Job job = Job.getInstance(conf, "word count");

        // 设置 Job 的主类（用于找到包含 Mapper 和 Reducer 的 jar 包）
        job.setJarByClass(WordCount.class);

        // 设置 Mapper 类
        job.setMapperClass(TokenizerMapper.class);

        // 设置 Combiner 类（可选，相当于 Map 端的本地 Reduce）
        // 设置 Combiner 可以减少网络传输量
        job.setCombinerClass(IntSumReducer.class);

        // 设置 Reducer 类
        job.setReducerClass(IntSumReducer.class);

        // 设置 Reducer 的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径（可以是文件或目录）
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置输出路径（必须不存在，MapReduce 会自动创建）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待完成
        // 参数 true 表示打印作业的进度信息
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### 3.2 代码执行过程追踪

```
┌──────────────────────────────────────────────────────────────────┐
│              WordCount 执行过程追踪                                │
│                                                                  │
│  输入文件:                                                       │
│  ┌────────────────────────────────────────┐                      │
│  │  file1: "hello world"                  │                      │
│  │  file2: "hello hadoop"                 │                      │
│  │  file3: "world hadoop"                 │                      │
│  └────────────────────────────────────────┘                      │
│                                                                  │
│  Step 1: 输入分片                                                │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  Split 1 → Map 1 (处理 file1 和 file2)              │        │
│  │  Split 2 → Map 2 (处理 file3)                      │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
│  Step 2: Map 阶段                                                │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  Map 1:                                              │        │
│  │    "hello world" → ("hello",1) ("world",1)          │        │
│  │    "hello hadoop" → ("hello",1) ("hadoop",1)        │        │
│  │                                                      │        │
│  │  Map 2:                                              │        │
│  │    "world hadoop" → ("world",1) ("hadoop",1)        │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
│  Step 3: Shuffle (Partition + Sort + Group)                      │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  Reduce 1 收到:                                      │        │
│  │    "hadoop" → [1, 1]                                │        │
│  │    "hello"  → [1, 1]                                │        │
│  │                                                      │        │
│  │  Reduce 2 收到:                                      │        │
│  │    "world"  → [1, 1]                                │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
│  Step 4: Reduce 阶段                                             │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  Reduce 1:                                           │        │
│  │    "hadoop" → 2                                      │        │
│  │    "hello"  → 2                                      │        │
│  │                                                      │        │
│  │  Reduce 2:                                           │        │
│  │    "world"  → 2                                      │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
│  最终输出:                                                       │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  hadoop  2                                           │        │
│  │  hello   2                                           │        │
│  │  world   2                                           │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 四、自定义 Mapper 和 Reducer（Java 版）

### 4.1 编译和打包

```bash
# 创建项目目录
mkdir -p ~/Programme/hadoop/wordcount/src
cd ~/Programme/hadoop/wordcount

# 将上面的代码保存为 src/WordCount.java
# （代码已在上面的 3.1 节）

# 编译（需要 Hadoop 的 classpath）
# 方法 1: 使用 hadoop classpath 获取依赖
hadoop com.sun.tools.javac.Main \
    -classpath $(hadoop classpath) \
    -d build \
    src/WordCount.java

# 方法 2: 使用 javac（如果环境变量配置正确）
javac -classpath $(hadoop classpath) -d build src/WordCount.java

# 打包为 jar
jar -cvf wordcount.jar -C build .
```

### 4.2 运行

```bash
# 准备输入数据
echo "hello world hadoop" > /tmp/input/file1.txt
echo "hello hdfs yarn mapreduce" > /tmp/input/file2.txt
echo "hadoop mapreduce yarn" > /tmp/input/file3.txt

hdfs dfs -mkdir -p /wordcount/input
hdfs dfs -put -f /tmp/input/* /wordcount/input/
hdfs dfs -rm -r /wordcount/output 2>/dev/null

# 运行自定义的 WordCount
hadoop jar ~/Programme/hadoop/wordcount/wordcount.jar \
    WordCount \
    /wordcount/input \
    /wordcount/output

# 查看结果
hdfs dfs -cat /wordcount/output/part-r-00000
```

## 五、Shuffle 详解

### 5.1 Partition（分区）

Partition 决定了 Map 的输出由哪个 Reduce 来处理。

```
┌──────────────────────────────────────────────────────────────┐
│              Partition 分区                                    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  默认分区: HashPartitioner                                    │
│  partition = hash(key) % numReduceTasks                      │
│                                                              │
│  示例 (3 个 Reduce):                                         │
│                                                              │
│  "hello"  → hash("hello") % 3 = 1 → Reduce 1               │
│  "world"  → hash("world") % 3 = 2 → Reduce 2               │
│  "hadoop" → hash("hadoop") % 3 = 0 → Reduce 0               │
│                                                              │
│  自定义 Partitioner:                                         │
│                                                              │
│  // 按单词首字母分区                                          │
│  public class FirstLetterPartitioner                         │
│      extends Partitioner<Text, IntWritable> {                │
│      @Override                                               │
│      public int getPartition(Text key, IntWritable value,    │
│                              int numReduceTasks) {           │
│          char first = key.toString().charAt(0);              │
│          if (first < 'h') return 0;                          │
│          if (first < 'p') return 1;                          │
│          return 2;                                           │
│      }                                                       │
│  }                                                           │
│                                                              │
│  在 Job 中设置:                                              │
│  job.setPartitionerClass(FirstLetterPartitioner.class);      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 5.2 Sort（排序）

MapReduce 保证在进入 Reduce 之前，数据按 key 排序。排序是在 Shuffle 阶段自动完成的。

```
Map 输出: ("banana", 1), ("apple", 1), ("cherry", 1), ("apple", 1)
                    │
                    ▼  Sort
Reduce 输入: ("apple", [1, 1]), ("banana", [1]), ("cherry", [1])
```

### 5.3 Combiner（组合器）

Combiner 是 Map 端的"本地 Reduce"，它可以**减少 Shuffle 阶段需要传输的数据量**。

```
┌──────────────────────────────────────────────────────────────────┐
│              Combiner 的作用                                       │
│                                                                  │
│  没有 Combiner:                                                  │
│  ─────────────                                                   │
│  Map 1 输出: ("hello",1), ("hello",1), ("world",1)               │
│                    │                                              │
│                    ▼  Shuffle (网络传输)                           │
│  Reduce 收到: ("hello", [1,1,1,1,1])  ← 传输了 5 个值           │
│                                                                  │
│  有 Combiner:                                                    │
│  ───────────                                                     │
│  Map 1 输出: ("hello",1), ("hello",1), ("world",1)               │
│                    │                                              │
│                    ▼  Combiner (本地合并)                         │
│  Map 1 输出: ("hello",2), ("world",1)  ← 只传输 2 个值          │
│                    │                                              │
│                    ▼  Shuffle (网络传输量减少 60%)                 │
│  Reduce 收到: ("hello", [2,3])  ← 传输了 2 个值                 │
│                                                                  │
│  注意: Combiner 的逻辑必须满足交换律和结合律！                    │
│        求和(sum)、最大值(max)、最小值(min) 可以用                 │
│        平均值(avg) 不能用！（因为局部平均 ≠ 全局平均）            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.4 配置 Combiner

```java
// 在 Job 配置中设置 Combiner
job.setCombinerClass(IntSumReducer.class);
// Combiner 类和 Reducer 类通常是同一个（如果逻辑满足交换律和结合律）
```

## 六、MapReduce 的局限性

```
┌──────────────────────────────────────────────────────────────┐
│              MapReduce 的局限性                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 中间结果写磁盘                                            │
│     Map → Shuffle → Reduce 每个阶段都可能写磁盘              │
│     对于需要多次迭代的算法（如机器学习），性能很差             │
│     → Spark 通过内存计算解决了这个问题                       │
│                                                              │
│  2. 只适合批处理                                              │
│     一次运行处理大量数据，不适合实时流式处理                   │
│     → Storm / Flink 更适合流式处理                           │
│                                                              │
│  3. 编程模型不够灵活                                          │
│     只能用 Map → Reduce 两阶段，复杂任务需要多次 MR 串联     │
│     → Spark 的 DAG 模型更灵活                                │
│                                                              │
│  4. 启动开销大                                                │
│     即使处理少量数据，也需要启动整个 MapReduce 框架           │
│     适合处理 GB 级以上的数据                                  │
│                                                              │
│  5. 不适合交互式分析                                          │
│     每次查询都需要启动新的 MapReduce 作业                     │
│     → Hive / Spark SQL 更适合                                │
│                                                              │
│  尽管有这些局限，MapReduce 仍然是大数据领域的基石，            │
│  理解 MapReduce 对于学习其他框架至关重要。                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## 七、总结

```
┌────────────────────────────────────────────────────────────────┐
│                    本篇要点总结                                  │
│                                                                │
│  MapReduce 核心思想:                                           │
│  分而治之 → Map(并行处理) → Shuffle(分组排序) → Reduce(聚合)   │
│                                                                │
│  编程模型:                                                     │
│  Mapper:  (输入key, 输入value) → [(输出key, 输出value), ...]  │
│  Reducer: (输出key, [value1, value2, ...]) → (key, result)    │
│                                                                │
│  Shuffle 是核心:                                               │
│  Partition(分区) → Sort(排序) → Group(分组)                   │
│                                                                │
│  Combiner 是优化:                                               │
│  Map 端本地 Reduce，减少网络传输                               │
│  必须满足交换律和结合律                                       │
│                                                                │
│  Hadoop 数据类型:                                              │
│  LongWritable(long), Text(String), IntWritable(int)           │
│  为什么用 Writable? 序列化/反序列化性能比 Java 原生好          │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

> **上一篇：[05-YARN资源调度](05-YARN资源调度.md) — YARN 资源调度**
>
> **下一篇：[07-配置与调优](07-配置与调优.md) — YARN 与 MapReduce 配置详解**
