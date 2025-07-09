package com.pearfl.dlk.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import com.pearfl.dlk.config.KafkaServerConfig;
import com.pearfl.dlk.config.KafkaTopicConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Kafka数据生产者应用 - 可配置化数据生成（每条数据打印到控制台）
 *
 * 功能亮点：
 * 1. 支持Exactly-Once语义
 * 2. 可配置发送条数和间隔时间
 * 3. 生成结构化JSON设备数据
 * 4. 每条发送的数据都打印到控制台
 */
public class FlinkKafkaProducerApp {
    // JSON序列化工具
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // 随机数生成器
    private static final Random RANDOM = new Random();
    // 设备状态枚举值
    private static final String[] DEVICE_STATUS = {"online", "offline", "warning", "error"};

    // 默认配置值（可通过命令行参数覆盖）
    private static final int DEFAULT_TOTAL_RECORDS = 100;
    private static final long DEFAULT_INTERVAL_MS = 500; // 0.5秒

    public static void main(String[] args) throws Exception {
        // 解析命令行参数（格式：--records 50 --interval 1000）
        int totalRecords = parseArg(args, "records", DEFAULT_TOTAL_RECORDS);
        long intervalMs = parseArg(args, "interval", DEFAULT_INTERVAL_MS);

        System.out.printf("启动生产者: 发送 %d 条数据, 间隔 %d 毫秒%n", totalRecords, intervalMs);

        // 加载Kafka配置
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");
        String clusterId = "mrs-kafka";
        String topicName = "DLK_TEST_TOPIC_MXH_01_DEV";

        // 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureFlinkEnvironment(env);

        // 创建可配置的数据流
        DataStream<String> dataStream = env.addSource(new DeviceDataGenerator(totalRecords, intervalMs))
                .name("设备数据生成器");

        // 打印每条发送的数据（在发送到Kafka之前）
        dataStream.map(json -> {
            System.out.println("发送数据: " + json);
            return json;
        }).name("数据打印器");

        // 创建Kafka Sink并写入数据
        KafkaSink<String> kafkaSink = createKafkaSink(config, clusterId, topicName);
        dataStream.sinkTo(kafkaSink)
                .name("Kafka写入器");

        // 执行任务
        env.execute(String.format("Kafka生产者(%d条@%dms)", totalRecords, intervalMs));
    }

    /**
     * 解析命令行参数
     *
     * @param args 命令行参数数组
     * @param key 要查找的参数名
     * @param defaultValue 默认值
     * @return 解析后的整数值
     */
    private static int parseArg(String[] args, String key, int defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (("--" + key).equals(args[i]) && i + 1 < args.length) {
                try {
                    return Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("无效的数值参数: " + args[i + 1]);
                }
            }
        }
        return defaultValue;
    }

    /**
     * 解析命令行参数（长整型版本）
     */
    private static long parseArg(String[] args, String key, long defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (("--" + key).equals(args[i]) && i + 1 < args.length) {
                try {
                    return Long.parseLong(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("无效的数值参数: " + args[i + 1]);
                }
            }
        }
        return defaultValue;
    }

    /**
     * 配置Flink环境
     */
    private static void configureFlinkEnvironment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(5000); // 5秒间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1); // 单线程确保顺序
    }

    /**
     * 创建Kafka Sink（Exactly-Once语义）
     */
    private static KafkaSink<String> createKafkaSink(
            KafkaGlobalConfig config, String clusterId, String topicName
    ) {
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        KafkaTopicConfig topic = cluster.getTopicsConfig().get(topicName);
        Properties props = mergeConfigs(cluster, topic, false);

        // Exactly-Once核心配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-producer-" + System.currentTimeMillis());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000"); // 15分钟超时

        return KafkaSink.<String>builder()
                .setBootstrapServers(getBootstrapServers(cluster))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topicName)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setKafkaProducerConfig(props)
                .build();
    }

    /**
     * 获取Kafka集群地址
     */
    private static String getBootstrapServers(KafkaServerConfig cluster) {
        return (String) cluster.getServerConfig().get("bootstrap.servers");
    }

    /**
     * 合并Kafka配置
     */
    private static Properties mergeConfigs(KafkaServerConfig cluster,
                                           KafkaTopicConfig topic,
                                           boolean isConsumer) {
        Properties props = new Properties();
        props.putAll(cluster.getServerConfig());
        props.putAll(isConsumer ?
                cluster.getServerConsumerConfig() :
                cluster.getServerProducerConfig());

        if (topic != null) {
            props.putAll(isConsumer ?
                    topic.getConsumerProperties() :
                    topic.getProducerProperties());
        }
        return props;
    }

    /**
     * 设备数据生成器（可配置条数和间隔，每条数据打印）
     */
    private static class DeviceDataGenerator implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private int recordCount = 0;
        private final int totalRecords;
        private final long intervalMs;

        /**
         * 构造函数
         * @param totalRecords 要生成的数据总量
         * @param intervalMs 每条数据间隔时间（毫秒）
         */
        public DeviceDataGenerator(int totalRecords, long intervalMs) {
            this.totalRecords = totalRecords;
            this.intervalMs = intervalMs;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            System.out.printf("开始生成 %d 条数据，间隔 %d 毫秒%n", totalRecords, intervalMs);

            while (isRunning && recordCount < totalRecords) {
                try {
                    // 1. 构建事件对象
                    Map<String, Object> event = new HashMap<>();
                    event.put("event", "device_status");
                    event.put("timestamp", System.currentTimeMillis());

                    // 2. 构建负载数据
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("id", "device-" + (10000 + recordCount));
                    payload.put("status", DEVICE_STATUS[RANDOM.nextInt(DEVICE_STATUS.length)]);
                    payload.put("temperature", 20 + RANDOM.nextInt(15));
                    payload.put("voltage", 3.5 + RANDOM.nextDouble() * 1.5);

                    event.put("payload", payload);

                    // 3. 序列化为JSON
                    String json = objectMapper.writeValueAsString(event);

                    // 4. 发送数据（打印操作在单独的map操作中完成）
                    ctx.collect(json);

                    // 5. 更新计数器
                    recordCount++;

                    // 6. 进度显示
                    if (recordCount % 10 == 0) {
                        System.out.printf("已生成 %d/%d 条数据%n", recordCount, totalRecords);
                    }

                    // 7. 按配置间隔等待
                    Thread.sleep(intervalMs);

                } catch (Exception e) {
                    System.err.println("生成数据异常: " + e.getMessage());
                }
            }

            System.out.printf("数据生成完成! 共生成 %d 条数据%n", recordCount);
        }

        @Override
        public void cancel() {
            isRunning = false;
            System.out.println("数据生成已取消");
        }
    }
}