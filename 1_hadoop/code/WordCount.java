import java.io.IOException;

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

/**
 * Hadoop MapReduce WordCount 示例程序
 *
 * 功能：统计文本文件中每个单词出现的次数
 * 适用于 Hadoop 3.4.2 + Java 21 环境
 *
 * 编译方式：
 *   # 设置 Hadoop classpath
 *   export HADOOP_CLASSPATH=$(hadoop classpath)
 *   # 编译
 *   javac -classpath $HADOOP_CLASSPATH WordCount.java
 *   # 打包
 *   jar cf wordcount.jar *.class
 *
 * 运行方式：
 *   hadoop jar wordcount.jar WordCount /input/path /output/path
 */
public class WordCount {

    /**
     * Mapper 类
     *
     * 输入：文件中的每一行（key 为行偏移量，value 为行内容）
     * 输出：每个单词作为 key，计数值 1 作为 value
     *
     * 泛型说明：
     *   LongWritable  — 输入 key 的类型（行偏移量，相当于 Java 的 long）
     *   Text          — 输入 value 的类型（行内容，相当于 Java 的 String）
     *   Text          — 输出 key 的类型（单词）
     *   IntWritable   — 输出 value 的类型（计数，相当于 Java 的 int）
     */
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        // 定义输出 value 的常量对象，避免在 map 方法中重复创建
        private static final IntWritable ONE = new IntWritable(1);
        // 定义输出 key 的变量，用于存储每个单词
        private Text word = new Text();

        /**
         * map 方法
         *
         * @param key     行偏移量（当前行在文件中的字节偏移位置）
         * @param value   当前行的文本内容
         * @param context Mapper 上下文，用于写入输出
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 将当前行文本转换为 Java 字符串
            String line = value.toString();

            // 按空白字符（空格、制表符、换行符等）分割单词
            for (String token : line.split("\\s+")) {
                // 跳过空字符串（可能由连续空格产生）
                if (token.isEmpty()) {
                    continue;
                }
                // 将单词设置为输出 key
                word.set(token);
                // 写入 <单词, 1> 的键值对
                context.write(word, ONE);
            }
        }
    }

    /**
     * Reducer 类
     *
     * 输入：Mapper 的输出，key 为单词，values 为该单词的所有计数值集合
     * 输出：key 为单词，value 为该单词的总出现次数
     *
     * 注意：在 Mapper 和 Reducer 之间，Hadoop 框架会自动执行 Shuffle 过程，
     *       将相同 key 的所有 value 聚合到一起，并按键排序后传递给 Reducer。
     *
     * 泛型说明：
     *   Text          — 输入/输出 key 的类型（单词）
     *   IntWritable   — 输入 value 的类型（单个计数）
     *   IntWritable   — 输出 value 的类型（总计数）
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 定义输出 value 的变量，用于存储累计结果
        private IntWritable result = new IntWritable();

        /**
         * reduce 方法
         *
         * @param key     单词
         * @param values  该单词对应的所有计数值（每个都是 1）
         * @param context Reducer 上下文，用于写入输出
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // 累加所有计数值
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将累计结果设置为输出 value
            result.set(sum);
            // 写入 <单词, 总次数> 的键值对
            context.write(key, result);
        }
    }

    /**
     * Driver 类（主程序入口）
     *
     * 负责配置和提交 MapReduce 作业，包括：
     * - 设置作业名称
     * - 指定 Mapper 和 Reducer 类
     * - 指定输入输出路径
     * - 指定输出 key/value 的类型
     */
    public static void main(String[] args) throws Exception {
        // 检查命令行参数：需要输入路径和输出路径两个参数
        if (args.length != 2) {
            System.err.println("用法: WordCount <输入路径> <输出路径>");
            System.err.println("示例: hadoop jar wordcount.jar WordCount /user/input /user/output");
            System.exit(1);
        }

        // 创建 Hadoop 配置对象
        Configuration conf = new Configuration();

        // 创建 MapReduce 作业对象
        Job job = Job.getInstance(conf, "Word Count 统计");

        // 设置主类（包含 main 方法的类）
        job.setJarByClass(WordCount.class);

        // 设置 Mapper 类
        job.setMapperClass(TokenizerMapper.class);

        // 设置 Combiner 类（本地聚合，减少网络传输量）
        // Combiner 在 Map 端运行，逻辑与 Reducer 相同
        job.setCombinerClass(IntSumReducer.class);

        // 设置 Reducer 类
        job.setReducerClass(IntSumReducer.class);

        // 设置输出 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径（支持多个输入路径）
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置输出路径（必须是尚未存在的路径，否则会报错）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待完成
        // 参数 true 表示打印作业执行进度
        boolean success = job.waitForCompletion(true);

        // 根据作业执行结果设置退出状态码
        System.exit(success ? 0 : 1);
    }
}
