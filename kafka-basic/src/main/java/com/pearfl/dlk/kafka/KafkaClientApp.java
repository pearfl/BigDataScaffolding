package com.pearfl.dlk.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import com.pearfl.dlk.config.KafkaServerConfig;
import com.pearfl.dlk.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka生产者消费者客户端应用，实现JSON数据的发送与消费
 * 功能包括：
 * 1. 加载Kafka集群配置
 * 2. 构造JSON格式消息
 * 3. 发送JSON数据到指定Kafka主题
 * 4. 从Kafka主题消费并解析JSON数据
 * 5. 手动提交消费偏移量
 */
public class KafkaClientApp {
    // 线程安全的JSON处理器，用于序列化/反序列化Java对象与JSON字符串
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 主入口方法
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        // 加载Kafka全局配置（集群、主题等配置信息）
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");

        // 1. 构造JSON消息数据（Java Map结构）
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", 1001);
        payload.put("name", "mxh做的简单测试");
        payload.put("status", "active");

        Map<String, Object> messageData = new HashMap<>();
        messageData.put("event", "data_update");
        messageData.put("timestamp", System.currentTimeMillis());
        messageData.put("payload", payload);  // 嵌套数据结构

        // 2. 发送JSON消息到指定主题
        sendJsonToTopic(config, "mrs-kafka", "DLK_TEST_TOPIC_MXH_01_DEV", messageData);

        // 3. 启动消费者监听并处理消息
        consumeJsonFromTopic(config, "mrs-kafka", "DLK_TEST_TOPIC_MXH_01_DEV");
    }

    /**
     * 发送JSON数据到Kafka主题
     *
     * @param config Kafka全局配置对象
     * @param clusterId Kafka集群ID
     * @param topicName 目标主题名称
     * @param messageObject 待发送的Java对象（将被序列化为JSON）
     */
    private static void sendJsonToTopic(KafkaGlobalConfig config, String clusterId,
                                        String topicName, Object messageObject) {
        // 获取指定集群和主题的配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        KafkaTopicConfig topic = config.getTopicConfigByIds(clusterId, topicName);

        // 合并配置：集群基础配置 + 集群级生产者配置 + 主题级生产者配置
        Properties producerProps = new Properties();
        producerProps.putAll(cluster.getServerConfig());          // 基础连接配置
        producerProps.putAll(cluster.getServerProducerConfig());  // 生产者通用配置
        producerProps.putAll(topic.getProducerProperties());     // 主题特有配置

        // 创建Kafka生产者实例（使用try-with-resources确保自动关闭）
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // 将Java对象序列化为JSON字符串
            String jsonMessage = objectMapper.writeValueAsString(messageObject);

            // 构造生产者记录（指定主题、消息键和JSON值）
            // 消息键用于分区路由（此处使用固定键"json-key"）
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,
                    "json-key",  // 分区路由键
                    jsonMessage  // JSON格式的消息体
            );

            // 异步发送消息（带回调函数处理发送结果）
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    // 成功发送日志
                    System.out.printf("[Producer] JSON发送成功！主题:%s 分区:%d 偏移量:%d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    // 发送失败日志
                    System.err.printf("[Producer] 发送失败: %s%n", exception.getMessage());
                }
            });
            producer.flush();  // 强制刷新缓冲区，确保所有消息完成发送
        } catch (JsonProcessingException e) {
            // JSON序列化异常处理
            System.err.println("[Producer] JSON序列化失败: " + e.getMessage());
        }
    }

    /**
     * 从Kafka主题消费并解析JSON数据
     *
     * @param config Kafka全局配置对象
     * @param clusterId Kafka集群ID
     * @param topicName 消费的主题名称
     */
    private static void consumeJsonFromTopic(KafkaGlobalConfig config, String clusterId, String topicName) {
        // 获取指定集群和主题的配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        KafkaTopicConfig topic = config.getTopicConfigByIds(clusterId, topicName);

        // 合并消费者配置
        Properties consumerProps = new Properties();
        consumerProps.putAll(cluster.getServerConfig());          // 基础连接配置
        consumerProps.putAll(cluster.getServerConsumerConfig());  // 消费者通用配置
        consumerProps.putAll(topic.getConsumerProperties());     // 主题特有配置

        // 创建Kafka消费者实例（使用try-with-resources确保自动关闭）
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicName));  // 订阅单个主题
            System.out.println("[Consumer] 开始监听主题: " + topicName);

            // 持续轮询消息
            while (true) {
                // 拉取消息（最长等待1秒）
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // 遍历处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // 将JSON字符串反序列化为Map结构
                        Map<String, Object> data = objectMapper.readValue(
                                record.value(),  // 原始JSON字符串
                                objectMapper.getTypeFactory().constructMapType(
                                        HashMap.class, String.class, Object.class)  // 目标类型
                        );

                        // 提取嵌套的payload字段
                        Map<?, ?> payload = (Map<?, ?>) data.get("payload");

                        // 打印消费详情（分区、偏移量、事件类型和设备状态）
                        System.out.printf("\n[Consumer] 收到JSON消息 → 分区:%d 偏移量:%d\n  事件:%s\n  设备ID:%s\n  状态:%s\n",
                                record.partition(), record.offset(),
                                data.get("event"), payload.get("id"), payload.get("status"));
                    } catch (Exception e) {
                        // JSON解析异常处理（记录原始数据和错误信息）
                        System.err.printf("[Consumer] JSON解析失败! 原始数据:%s\n  错误:%s\n",
                                record.value(), e.getMessage());
                    }
                }

                // 手动同步提交偏移量（确保消息处理完成）
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.printf("[Consumer] 已提交偏移量 → 处理%d条消息\n", records.count());
                }
            }
        }
    }
}