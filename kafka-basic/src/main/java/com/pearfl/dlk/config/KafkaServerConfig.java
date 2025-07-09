package com.pearfl.dlk.config;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.*;

/**
 * Kafka集群配置容器 - 存储单集群的完整配置信息
 *
 * 配置分层：
 * 1. 基础连接配置（bootstrap.servers等）
 * 2. 生产者默认配置
 * 3. 消费者默认配置
 * 4. 主题专属配置
 *
 * 安全特性：
 * - 配置键有效性验证（防止拼写错误）
 * - 返回不可修改的配置视图
 */
@Data
@Accessors(chain = true)
public class KafkaServerConfig {
    private String clusterId;  // 集群唯一标识符

    /** 基础连接配置存储 */
    private final Map<String, Object> serverConfig = new HashMap<>();

    /** 生产者默认配置存储（自动验证键有效性） */
    private final Map<String, Object> serverProducerConfig = new HashMap<>();

    /** 消费者默认配置存储（自动验证键有效性） */
    private final Map<String, Object> serverConsumerConfig = new HashMap<>();

    /** 主题配置存储：主题名 → 主题配置 */
    private final Map<String, KafkaTopicConfig> topicsConfig = new HashMap<>();

    /**
     * 添加基础连接配置
     */
    public KafkaServerConfig addServerConfig(String key, Object value) {
        serverConfig.put(key, value);
        return this;
    }

    /**
     * 添加生产者默认配置（带键有效性验证）
     *
     * @throws IllegalArgumentException 当使用无效配置键时
     */
    public KafkaServerConfig addServerProducerConfig(String key, Object value) {
        // 验证键是否在Kafka官方配置集中
        validateKey(key, ProducerConfig.configNames());
        serverProducerConfig.put(key, value);
        return this;
    }

    /**
     * 添加消费者默认配置（带键有效性验证）
     *
     * @throws IllegalArgumentException 当使用无效配置键时
     */
    public KafkaServerConfig addServerConsumerConfig(String key, Object value) {
        // 验证键是否在Kafka官方配置集中
        validateKey(key, ConsumerConfig.configNames());
        serverConsumerConfig.put(key, value);
        return this;
    }

    /**
     * 添加主题配置到当前集群
     */
    public KafkaServerConfig addTopicConfig(String name, KafkaTopicConfig config) {
        topicsConfig.put(name, config);
        return this;
    }

    /**
     * 获取生产者默认配置（不可修改视图）
     */
    public Map<String, Object> getServerProducerConfig() {
        return Collections.unmodifiableMap(serverProducerConfig);
    }

    /**
     * 获取消费者默认配置（不可修改视图）
     */
    public Map<String, Object> getServerConsumerConfig() {
        return Collections.unmodifiableMap(serverConsumerConfig);
    }

    /**
     * 获取所有主题配置（不可修改视图）
     */
    public Map<String, KafkaTopicConfig> getTopicsConfig() {
        return Collections.unmodifiableMap(topicsConfig);
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
            throw new IllegalArgumentException("Invalid config key: " + key);
        }
    }
}