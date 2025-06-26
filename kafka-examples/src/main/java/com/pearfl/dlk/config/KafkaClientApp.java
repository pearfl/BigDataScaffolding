package com.pearfl.dlk.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;

public class KafkaClientApp {
    public static void main(String[] args) {
        // 初始化全局配置
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");

        // 发送消息到指定集群的主题
        sendToTopic(config, "mrs-kafka", "scene-topic1", "order-data");

        // 从指定集群的主题消费
        consumeFromTopic(config, "cluster2", "logs");
    }

    private static void sendToTopic(KafkaGlobalConfig config, String clusterId,
                                    String topicName, String message) {
        // 1. 获取集群配置
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        if (cluster == null) throw new IllegalArgumentException("无效集群ID");

        // 2. 获取主题配置
        KafkaTopicConfig topic = config.getTopicConfigByIds(clusterId, topicName);
        if (topic == null) throw new IllegalArgumentException("无效主题");

        // 3. 构建生产者（合并集群+主题配置）
        Properties producerProps = new Properties();
        producerProps.putAll(cluster.getServerConfig()); // 集群基础配置
        producerProps.putAll(topic.getProducerProperties()); // 主题定制配置

        KafkaProducer producer = new KafkaProducer<>(producerProps);
        producer.send(new ProducerRecord<>(topicName, message));
    }

    private static void consumeFromTopic(KafkaGlobalConfig config, String clusterId, String topicName) {
        // 同上逻辑获取消费者配置
        Properties consumerProps = new Properties();
        consumerProps.putAll(config.getServerConfigById(clusterId).getServerConfig());
        consumerProps.putAll(config.getTopicConfigByIds(clusterId, topicName).getConsumerProperties());

        KafkaConsumer consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topicName));
        // ... 消费逻辑
    }
}
