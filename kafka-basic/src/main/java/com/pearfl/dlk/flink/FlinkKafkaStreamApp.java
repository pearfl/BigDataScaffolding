package com.pearfl.dlk.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import com.pearfl.dlk.config.KafkaServerConfig;
import com.pearfl.dlk.config.KafkaTopicConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * Flink与Kafka集成应用：实现JSON数据的实时消费、处理与回写
 * 功能亮点：
 *  - 支持Exactly-Once语义（端到端一致性）
 *  - 动态加载Kafka集群/主题配置
 *  - 自动容错与状态恢复
 *  - JSON解析异常隔离处理
 *  - 主题级别配置管理
 */
public class FlinkKafkaStreamApp {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 加载配置（支持热更新）
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");
        String clusterId = "mrs-kafka";
        String topicName = "DLK_TEST_TOPIC_MXH_01_DEV";
        String groupId = config.getTopicConfigByClusterIdAndTopicName(clusterId, topicName).getConsumerProperties().getProperty("group.id");

        // 1. 初始化Flink环境（启用Checkpoint）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureFlinkEnvironment(env); // 环境参数集中配置

        // 2. 创建Kafka数据源
        DataStream<String> kafkaSource = env.fromSource(
                createKafkaSource(config, clusterId, topicName, groupId),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 3. 处理JSON数据
        DataStream<String> processedStream = kafkaSource
                .map(new JsonProcessor())
                .name("JSON Processor");

        // 4. 双路输出：控制台调试 + Kafka回写
        processedStream.print().name("Console Sink");
//        processedStream.sinkTo(createKafkaSink(config, clusterId, topicName))
//                .name("Kafka Sink");

        env.execute("Flink-Kafka实时处理");
    }

    /**
     * 配置Flink执行环境（关键参数）
     * @param env Flink流执行环境
     */
    private static void configureFlinkEnvironment(StreamExecutionEnvironment env) {
        // 启用Checkpoint，间隔5秒
        env.enableCheckpointing(5000);
        // 设置Exactly-Once语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 建议并行度与Topic分区数一致
        env.setParallelism(4);
    }

    /**
     * JSON解析处理器（封装业务逻辑）
     */
    private static class JsonProcessor implements MapFunction<String, String> {
        @Override
        public String map(String json) {
            try {
                // 解析JSON数据
                Map<String, Object> data = objectMapper.readValue(json, Map.class);
                // 提取payload字段
                Map<?, ?> payload = (Map<?, ?>) data.get("payload");
                // 格式化输出
                return String.format("事件:%s | 设备ID:%s | 状态:%s",
                        data.get("event"), payload.get("id"), payload.get("status"));
            } catch (Exception e) {
                // 异常数据单独处理
                return "JSON解析失败: " + json;
            }
        }
    }

    /**
     * 创建Kafka Source
     * @param config Kafka全局配置
     * @param clusterId 集群ID
     * @param topicName 主题名称
     * @param groupId 消费者组ID
     * @return KafkaSource实例
     */
    private static KafkaSource<String> createKafkaSource(
            KafkaGlobalConfig config, String clusterId, String topicName, String groupId
    ) {
        // 获取集群配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        // 获取主题配置
        KafkaTopicConfig topic = cluster.getTopicsConfig().get(topicName);

        // 合并配置（集群+主题）
        Properties props = mergeConfigs(cluster, topic, true);
        // 设置从最早偏移量开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return KafkaSource.<String>builder()
                // 设置Kafka集群地址
                .setBootstrapServers((String) cluster.getServerConfig().get("bootstrap.servers"))
                // 设置消费主题
                .setTopics(topicName)
                // 设置消费组ID
                .setGroupId(groupId)
                // 使用SimpleStringSchema进行反序列化
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                // 从最早偏移量开始
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 应用合并后的配置
                .setProperties(props)
                .build();
    }

    /**
     * 创建Kafka Sink（支持Exactly-Once）
     * @param config Kafka全局配置
     * @param clusterId 集群ID
     * @param topicName 主题名称
     * @return KafkaSink实例
     */
    private static KafkaSink<String> createKafkaSink(
            KafkaGlobalConfig config, String clusterId, String topicName
    ) {
        // 获取集群配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        // 获取主题配置
        KafkaTopicConfig topic = cluster.getTopicsConfig().get(topicName);
        // 合并配置（集群+主题）
        Properties props = mergeConfigs(cluster, topic, false);

        // Exactly-Once核心配置
        // 生成唯一事务ID
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "flink-tx-" + System.currentTimeMillis());
        // 启用幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // 设置事务超时时间（需大于Checkpoint间隔）
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");

        return KafkaSink.<String>builder()
                // 设置Kafka集群地址
                .setBootstrapServers((String) cluster.getServerConfig().get("bootstrap.servers"))
                // 设置序列化器
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName) // 目标主题
                        .setValueSerializationSchema(new SimpleStringSchema()) // 字符串序列化
                        .build()
                )
                // 设置Exactly-Once语义
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 应用合并后的配置
                .setKafkaProducerConfig(props)
                .build();
    }

    /**
     * 合并Kafka配置（集群+主题）
     * @param cluster Kafka集群配置
     * @param topic Kafka主题配置
     * @param isConsumer 是否为消费者配置
     * @return 合并后的Properties对象
     */
    private static Properties mergeConfigs(KafkaServerConfig cluster,
                                           KafkaTopicConfig topic,
                                           boolean isConsumer) {
        Properties props = new Properties();

        // 1. 添加基础配置（bootstrap.servers等）
        props.putAll(cluster.getServerConfig());

        // 2. 添加客户端类型特定配置
        props.putAll(isConsumer ?
                cluster.getServerConsumerConfig() : // 消费者配置
                cluster.getServerProducerConfig()); // 生产者配置

        // 3. 添加主题级别配置
        if (topic != null) {
            // 根据客户端类型选择主题配置
            props.putAll(isConsumer ?
                    topic.getConsumerProperties() :
                    topic.getProducerProperties());
        }
        return props;
    }
}