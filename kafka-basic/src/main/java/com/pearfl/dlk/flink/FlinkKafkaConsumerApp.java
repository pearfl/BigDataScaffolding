package com.pearfl.dlk.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import com.pearfl.dlk.config.KafkaServerConfig;
import com.pearfl.dlk.config.KafkaTopicConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka数据消费者应用 - 实时消费并处理设备状态数据
 *
 * 功能亮点：
 * 1. 支持Exactly-Once语义（端到端一致性）
 * 2. 动态加载Kafka多集群配置
 * 3. 结构化JSON数据解析
 * 4. 异常数据隔离处理
 * 5. 实时处理结果输出
 */
public class FlinkKafkaConsumerApp {
    // JSON解析工具
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaConsumerApp.class);

    public static void main(String[] args) throws Exception {
        /**
         * Step 1: 初始化配置
         *
         * - 加载YAML格式的Kafka配置
         * - 指定目标集群和主题
         * - 获取消费者组ID
         */
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");
        String clusterId = "mrs-kafka";
        String topicName = "DLK_TEST_TOPIC_MXH_01_DEV";
        String groupId = config.getTopicConfigByClusterIdAndTopicName(clusterId, topicName)
                .getConsumerProperties().getProperty("group.id");

        logger.info("启动消费者: 集群={}, 主题={}, 消费者组={}", clusterId, topicName, groupId);

        /**
         * Step 2: 配置Flink环境
         *
         * - 启用Checkpoint（5秒间隔）
         * - 设置Exactly-Once语义
         * - 并行度设置为4（建议与主题分区数一致）
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureFlinkEnvironment(env);

        /**
         * Step 3: 创建Kafka数据源
         *
         * - 从最早偏移量开始消费
         * - 自动合并集群和主题配置
         * - 使用字符串反序列化器
         */
        KafkaSource<String> kafkaSource = createKafkaSource(config, clusterId, topicName, groupId);
        DataStream<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka数据源"
        );

        // 打印原始JSON数据（调试用）
        sourceStream.map(json -> {
            logger.debug("收到原始数据: {}", json);
            return json;
        }).name("原始数据记录器");

        /**
         * Step 4: 数据处理流水线
         *
         * 1. JSON解析处理器：提取关键字段
         * 2. 异常处理器：隔离解析失败的数据
         * 3. 结果输出：格式化打印到控制台
         */
        DataStream<String> processedStream = sourceStream
                .map(new JsonProcessor())
                .name("JSON解析器");

        processedStream.addSink(new PrintSinkFunction<>("处理结果: ", false))
                .name("控制台输出");

        /**
         * Step 5: 执行任务
         */
        env.execute("设备状态监控消费者");
    }

    /**
     * 配置Flink执行环境
     *
     * @param env Flink流处理环境
     */
    private static void configureFlinkEnvironment(StreamExecutionEnvironment env) {
        // 启用Checkpoint（5秒间隔）
        env.enableCheckpointing(5000);

        // 设置Exactly-Once语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置并行度（建议与主题分区数一致）
        env.setParallelism(4);

        logger.info("Flink环境配置完成: Checkpoint间隔=5秒, 并行度=4");
    }

    /**
     * 创建Kafka数据源（支持Exactly-Once）
     *
     * @param config Kafka全局配置
     * @param clusterId 集群ID
     * @param topicName 主题名称
     * @param groupId 消费者组ID
     * @return 配置好的KafkaSource实例
     */
    private static KafkaSource<String> createKafkaSource(
            KafkaGlobalConfig config, String clusterId, String topicName, String groupId
    ) {
        // 获取集群配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        // 获取主题配置
        KafkaTopicConfig topic = cluster.getTopicsConfig().get(topicName);
        // 合并配置（集群默认值 + 主题覆盖值）
        Properties props = mergeConfigs(cluster, topic, true);

        // 关键消费者配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 禁用自动提交

        logger.debug("Kafka消费者配置: {}", props);

        // 构建KafkaSource
        return KafkaSource.<String>builder()
                .setBootstrapServers(getBootstrapServers(cluster)) // Kafka集群地址
                .setTopics(topicName) // 订阅主题
                .setGroupId(groupId) // 消费者组ID
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        new SimpleStringSchema())) // 字符串反序列化
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最早偏移量开始
                .setProperties(props) // 应用配置
                .build();
    }

    /**
     * 获取Kafka集群地址
     *
     * @param cluster Kafka集群配置
     * @return bootstrap.servers地址
     */
    private static String getBootstrapServers(KafkaServerConfig cluster) {
        return (String) cluster.getServerConfig().get("bootstrap.servers");
    }

    /**
     * 合并Kafka配置（集群默认值 + 主题覆盖值）
     *
     * @param cluster 集群配置
     * @param topic 主题配置
     * @param isConsumer 是否为消费者配置
     * @return 合并后的Properties对象
     */
    private static Properties mergeConfigs(KafkaServerConfig cluster,
                                           KafkaTopicConfig topic,
                                           boolean isConsumer) {
        Properties props = new Properties();

        // 1. 添加基础连接配置
        props.putAll(cluster.getServerConfig());

        // 2. 添加消费者/生产者默认配置
        props.putAll(isConsumer ?
                cluster.getServerConsumerConfig() :
                cluster.getServerProducerConfig());

        // 3. 添加主题级别覆盖配置
        if (topic != null) {
            props.putAll(isConsumer ?
                    topic.getConsumerProperties() :
                    topic.getProducerProperties());
        }

        return props;
    }

    /**
     * JSON数据处理器 - 解析设备状态信息
     *
     * 数据结构示例:
     * {
     *   "event": "device_status",
     *   "timestamp": 1672531200000,
     *   "payload": {
     *     "id": "device-10001",
     *     "status": "online",
     *     "temperature": 25,
     *     "voltage": 4.2
     *   }
     * }
     */
    private static class JsonProcessor implements MapFunction<String, String> {
        @Override
        public String map(String json) {
            try {
                // 1. 解析JSON为Map结构
                Map<String, Object> event = objectMapper.readValue(json, Map.class);

                // 2. 提取payload字段
                Map<?, ?> payload = (Map<?, ?>) event.get("payload");

                // 3. 构建格式化结果
                return String.format("事件类型: %s | 设备ID: %s | 状态: %s | 温度: %s℃ | 电压: %sV",
                        event.get("event"),
                        payload.get("id"),
                        payload.get("status"),
                        payload.get("temperature"),
                        payload.get("voltage"));

            } catch (Exception e) {
                // 4. 异常处理：记录解析失败的数据
                logger.error("JSON解析失败: {}", json, e);
                return "解析失败: " + json.substring(0, Math.min(json.length(), 100)) + "...";
            }
        }
    }
}