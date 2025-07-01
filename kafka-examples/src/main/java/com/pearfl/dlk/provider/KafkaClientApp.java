package com.pearfl.dlk.provider;

import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import com.pearfl.dlk.config.KafkaServerConfig;
import com.pearfl.dlk.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaClientApp {
    public static void main(String[] args) {
        KafkaGlobalConfig config = KafkaConfigLoader.loadConfig("kafka-config.yml");
        sendToTopic(config, "mrs-kafka", "scene-topic1", "order-data");
        consumeFromTopic(config, "cluster2", "logs");
    }

    private static void sendToTopic(KafkaGlobalConfig config, String clusterId,
                                    String topicName, String message) {
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        KafkaTopicConfig topic = config.getTopicConfigByIds(clusterId, topicName);

        // 合并配置：集群基础配置 + 集群级生产者配置 + 主题级生产者配置
        Properties producerProps = new Properties();
        producerProps.putAll(cluster.getServerConfig()); // 基础配置（如bootstrap.servers）
        producerProps.putAll(cluster.getServerProducerConfig()); // 集群级生产者配置（如acks, retries）
        producerProps.putAll(topic.getProducerProperties()); // 主题级生产者配置（如linger.ms）

        try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topicName, message));
        }
    }

    private static void consumeFromTopic(KafkaGlobalConfig config, String clusterId, String topicName) {
        KafkaServerConfig cluster = config.getServerConfigById(clusterId);
        KafkaTopicConfig topic = config.getTopicConfigByIds(clusterId, topicName);

        // 合并配置：集群基础配置 + 集群级消费者配置 + 主题级消费者配置
        Properties consumerProps = new Properties();
        consumerProps.putAll(cluster.getServerConfig()); // 基础配置
        consumerProps.putAll(cluster.getServerConsumerConfig()); // 集群级消费者配置（如group.id）
        consumerProps.putAll(topic.getConsumerProperties()); // 主题级消费者配置（如max.poll.records）

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(topicName));
            while (true) {
                consumer.poll(Duration.ofMillis(1000)).forEach(record ->
                        System.out.printf("Consumed: %s%n", record.value())
                );
            }
        }
    }
}
