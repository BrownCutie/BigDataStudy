package com.demo.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;

/**
 * 博客事件实时统计 Flink 作业
 *
 * 功能：
 * 1. 从 Kafka 读取博客事件（浏览、评论、点赞、搜索）
 * 2. 按事件类型统计数量
 * 3. 5 分钟滚动窗口统计每篇文章的浏览量
 * 4. 将统计结果输出到控制台
 *
 * 技术栈：
 * - Flink 2.2.0 Standalone 模式
 * - Kafka 4.2.0 作为数据源（端口 9092）
 * - Java 21
 *
 * 编译与运行：
 *   mvn clean package
 *   flink run -c com.demo.flink.BlogEventStatistics target/demo-flink-1.0.jar
 */
public class BlogEventStatistics {

    /** JSON 解析器 */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // ================================================================
        // 1. 创建执行环境
        // ================================================================
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);

        // 启用 Checkpoint，间隔 30 秒
        env.enableCheckpointing(30000);

        // ================================================================
        // 2. 配置 Kafka Source
        // ================================================================
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-demo-stats");

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "blog-events",               // Topic 名称
                new SimpleStringSchema(),    // 消息反序列化：字符串
                kafkaProps                   // Kafka 配置
        );

        // 从最早的消息开始消费
        kafkaSource.setStartFromEarliest();

        // ================================================================
        // 3. 从 Kafka 读取数据流
        // ================================================================
        DataStream<String> rawStream = env.addSource(kafkaSource);

        // ================================================================
        // 4. 解析 JSON 并提取事件时间戳
        // ================================================================
        // 定义 Watermark 策略：允许 10 秒的乱序延迟
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        // 从事件 JSON 中提取 timestamp 字段
                        JsonNode node = objectMapper.readTree(event);
                        String ts = node.has("timestamp") ? node.get("timestamp").asText() : null;
                        if (ts != null) {
                            // ISO 格式时间戳转换为毫秒
                            return java.time.Instant.parse(ts).toEpochMilli();
                        }
                    } catch (Exception e) {
                        // 解析失败，使用默认时间戳
                    }
                    return System.currentTimeMillis();
                });

        // 应用 Watermark 策略
        SingleOutputStreamOperator<String> streamWithWatermark = rawStream
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // ================================================================
        // 5. 解析 JSON 为 BlogEvent 对象
        // ================================================================
        DataStream<BlogEvent> events = streamWithWatermark.map(new MapFunction<String, BlogEvent>() {
            @Override
            public BlogEvent map(String value) throws Exception {
                JsonNode node = objectMapper.readTree(value);
                BlogEvent event = new BlogEvent();
                event.eventType = node.has("eventType") ? node.get("eventType").asText() : "unknown";
                if (node.has("data")) {
                    JsonNode data = node.get("data");
                    event.postId = data.has("postId") ? data.get("postId").asInt() : 0;
                    event.userId = data.has("userId") ? data.get("userId").asText() : "";
                    event.content = data.has("content") ? data.get("content").asText() : "";
                    event.keyword = data.has("keyword") ? data.get("keyword").asText() : "";
                }
                return event;
            }
        });

        // ================================================================
        // 6. 统计一：按事件类型统计数量
        // ================================================================
        DataStream<Tuple2<String, Integer>> eventTypeCounts = events
                .map(new MapFunction<BlogEvent, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(BlogEvent event) {
                        return Tuple2.of(event.eventType, 1);
                    }
                })
                .keyBy(value -> value.f0)  // 按事件类型分组
                .sum(1);                    // 累加计数

        // 输出到控制台
        eventTypeCounts.print("事件类型统计");

        // ================================================================
        // 7. 统计二：5 分钟滚动窗口统计每篇文章的浏览量
        // ================================================================
        DataStream<Tuple3<Integer, Long, Long>> postViewWindowStats = events
                // 过滤出浏览事件
                .filter(event -> "view".equals(event.eventType))
                .map(new MapFunction<BlogEvent, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(BlogEvent event) {
                        return Tuple2.of(event.postId, 1);
                    }
                })
                // 按文章 ID 分组
                .keyBy(value -> value.f0)
                // 5 分钟滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                // 窗口内求和
                .sum(1)
                // 转换输出格式：(文章ID, 浏览量, 窗口结束时间)
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Long, Long>>() {
                    @Override
                    public Tuple3<Integer, Long, Long> map(Tuple2<Integer, Integer> value) {
                        return Tuple3.of(value.f0, (long) value.f1, System.currentTimeMillis());
                    }
                });

        // 输出到控制台
        postViewWindowStats.print("文章浏览量(5分钟窗口)");

        // ================================================================
        // 8. 统计三：搜索关键词实时统计（5 分钟窗口）
        // ================================================================
        DataStream<Tuple2<String, Integer>> searchStats = events
                // 过滤出搜索事件
                .filter(event -> "search".equals(event.eventType))
                .map(new MapFunction<BlogEvent, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(BlogEvent event) {
                        return Tuple2.of(event.keyword, 1);
                    }
                })
                // 按搜索关键词分组
                .keyBy(value -> value.f0)
                // 5 分钟滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                // 窗口内求和
                .sum(1);

        // 输出到控制台
        searchStats.print("搜索关键词统计");

        // ================================================================
        // 9. 执行作业
        // ================================================================
        env.execute("博客事件实时统计");
    }

    // ================================================================
    // 博客事件数据类
    // ================================================================
    /**
     * 博客事件数据结构
     * 对应 Kafka 中 blog-events Topic 的 JSON 消息格式
     */
    public static class BlogEvent {
        /** 事件类型：view（浏览）、comment（评论）、like（点赞）、search（搜索） */
        public String eventType;
        /** 文章 ID */
        public int postId;
        /** 用户 ID */
        public String userId;
        /** 事件附加内容（如评论内容） */
        public String content;
        /** 搜索关键词 */
        public String keyword;

        @Override
        public String toString() {
            return String.format("BlogEvent{type='%s', postId=%d, userId='%s'}",
                    eventType, postId, userId);
        }
    }
}
