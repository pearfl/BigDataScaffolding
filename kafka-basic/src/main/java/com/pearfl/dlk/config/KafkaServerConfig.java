package com.pearfl.dlk.config;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.*;

@Data
@Accessors(chain = true)
public class KafkaServerConfig {
    private String clusterId;

    // 配置存储（使用不可变视图防止篡改）
    private final Map<String, Object> serverConfig = new HashMap<>();
    private final Map<String, Object> serverProducerConfig = new HashMap<>();
    private final Map<String, Object> serverConsumerConfig = new HashMap<>();
    private final Map<String, KafkaTopicConfig> topicConfigs = new HashMap<>();

    /** 添加基础配置（bootstrap.servers等） */
    public KafkaServerConfig addServerConfig(String key, Object value) {
        serverConfig.put(key, value);
        return this;
    }

    /** 添加生产者默认配置（自动验证键有效性） */
    public KafkaServerConfig addServerProducerConfig(String key, Object value) {
        validateConfigKey(key, ProducerConfig.configNames());
        serverProducerConfig.put(key, value);
        return this;
    }

    /** 添加消费者默认配置（自动验证键有效性） */
    public KafkaServerConfig addServerConsumerConfig(String key, Object value) {
        validateConfigKey(key, ConsumerConfig.configNames());
        serverConsumerConfig.put(key, value);
        return this;
    }

    /** 添加主题配置 */
    public KafkaServerConfig addTopicConfig(String name, KafkaTopicConfig config) {
        topicConfigs.put(name, config);
        return this;
    }

    /** 获取生产者默认配置（不可变视图） */
    public Map<String, Object> getServerProducerConfig() {
        return Collections.unmodifiableMap(serverProducerConfig);
    }

    /** 获取消费者默认配置（不可变视图） */
    public Map<String, Object> getServerConsumerConfig() {
        return Collections.unmodifiableMap(serverConsumerConfig);
    }

    /** 配置键有效性验证（防止拼写错误） */
    private void validateConfigKey(String key, Set<String> validKeys) {
        if (!validKeys.contains(key)) {
            throw new IllegalArgumentException("无效配置项: " + key);
        }
    }
}