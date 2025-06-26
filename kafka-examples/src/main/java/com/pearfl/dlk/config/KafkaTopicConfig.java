package com.pearfl.dlk.config;

import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.Set;

@Data
@Accessors(chain = true)
public class KafkaTopicConfig {
    private String topicName;

    // 获取生产者配置Properties（用于遍历）
    // 使用 Properties 存储配置（与官方 API 兼容）
    @Getter
    private final Properties producerProperties = new Properties();
    // 获取消费者配置Properties（用于遍历）
    @Getter
    private final Properties consumerProperties = new Properties();

    // 获取类型安全的 ProducerConfig 实例（用于创建生产者）
    public ProducerConfig getProducerConfig() {
        return new ProducerConfig(producerProperties);
    }

    // 获取类型安全的 ConsumerConfig 实例（用于创建消费者）
    public ConsumerConfig getConsumerConfig() {
        return new ConsumerConfig(consumerProperties);
    }

    // 链式添加生产者配置
    public KafkaTopicConfig addProducerConfig(String key, Object value) {
        validateConfigKey(key, ProducerConfig.configNames());
        producerProperties.put(key, value);
        return this;
    }

    // 链式添加消费者配置
    public KafkaTopicConfig addConsumerConfig(String key, Object value) {
        validateConfigKey(key, ConsumerConfig.configNames());
        consumerProperties.put(key, value);
        return this;
    }

    // 验证配置键有效性（防止拼写错误）
    private void validateConfigKey(String key, Set<String> validKeys) {
        if (!validKeys.contains(key)) {
            throw new IllegalArgumentException("无效配置项: " + key);
        }
    }
}