package com.pearfl.dlk.config;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.*;

/**
 * Kafka主题配置容器 - 存储单个主题的生产者和消费者配置
 *
 * 核心特性：
 * 1. 自动继承集群级默认配置
 * 2. 提供类型安全的配置获取方式
 * 3. 自动验证配置键有效性
 */
@Data
@Accessors(chain = true)
public class KafkaTopicConfig {
    private String topicName;  // 主题逻辑名称

    /** 主题专属生产者配置（继承+覆盖集群默认值） */
    private final Properties producerProperties = new Properties();

    /** 主题专属消费者配置（继承+覆盖集群默认值） */
    private final Properties consumerProperties = new Properties();

    /**
     * 添加生产者配置（带键有效性验证）
     *
     * @throws IllegalArgumentException 当使用无效配置键时
     */
    public KafkaTopicConfig addProducerConfig(String key, Object value) {
        // 验证键是否在Kafka官方配置集中
        validateKey(key, ProducerConfig.configNames());
        producerProperties.put(key, value);
        return this;
    }

    /**
     * 添加消费者配置（带键有效性验证）
     *
     * @throws IllegalArgumentException 当使用无效配置键时
     */
    public KafkaTopicConfig addConsumerConfig(String key, Object value) {
        // 验证键是否在Kafka官方配置集中
        validateKey(key, ConsumerConfig.configNames());
        consumerProperties.put(key, value);
        return this;
    }

    /**
     * 获取消费者配置（Map形式）
     */
    public Map<String, Object> getConsumerPropertiesAsMap() {
        return toMap(consumerProperties);
    }

    /**
     * 获取生产者配置（Map形式）
     */
    public Map<String, Object> getProducerPropertiesAsMap() {
        return toMap(producerProperties);
    }

    /**
     * Properties转Map工具方法
     */
    private Map<String, Object> toMap(Properties props) {
        Map<String, Object> map = new HashMap<>();
        // 遍历所有键值对并存入Map
        props.forEach((k, v) -> map.put((String) k, v));
        return map;
    }

    /**
     * 配置键有效性验证（核心安全机制）
     *
     * @param key 待验证的配置键
     * @param validKeys 官方有效的配置键集合
     * @throws IllegalArgumentException 当键无效时
     */
    private void validateKey(String key, Set<String> validKeys) {
        if (!validKeys.contains(key)) {
            throw new IllegalArgumentException("Invalid topic config: " + key);
        }
    }
}