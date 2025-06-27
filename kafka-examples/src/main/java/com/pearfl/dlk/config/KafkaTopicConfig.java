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
    private final Properties producerProperties = new Properties();
    private final Properties consumerProperties = new Properties();

    /** 添加生产者配置（自动验证+类型安全） */
    public KafkaTopicConfig addProducerConfig(String key, Object value) {
        validateConfigKey(key, ProducerConfig.configNames());
        producerProperties.put(key, value);
        return this;
    }

    /** 添加消费者配置（自动验证+类型安全） */
    public KafkaTopicConfig addConsumerConfig(String key, Object value) {
        validateConfigKey(key, ConsumerConfig.configNames());
        consumerProperties.put(key, value);
        return this;
    }

    /** 获取类型安全的ProducerConfig实例 */
    public ProducerConfig getProducerConfig() {
        return new ProducerConfig(producerProperties);
    }

    /** 获取类型安全的ConsumerConfig实例 */
    public ConsumerConfig getConsumerConfig() {
        return new ConsumerConfig(consumerProperties);
    }

    /** 配置键有效性验证 */
    private void validateConfigKey(String key, Set<String> validKeys) {
        if (!validKeys.contains(key)) {
            throw new IllegalArgumentException("无效主题配置: " + key);
        }
    }
}