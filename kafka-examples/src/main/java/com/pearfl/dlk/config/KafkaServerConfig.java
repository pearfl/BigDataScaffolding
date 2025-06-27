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

    // 基础连接配置（如bootstrap.servers）
    @Getter(AccessLevel.NONE)
    private final Map<String, Object> serverConfig = new HashMap<>();

    // 集群级生产者配置（原名producerDefaults）
    @Getter(AccessLevel.NONE)
    private final  Map<String, Object> serverProducerConfig = new HashMap<>();

    // 集群级消费者配置（原名consumerDefaults）
    @Getter(AccessLevel.NONE)
    private final  Map<String, Object> serverConsumerConfig = new HashMap<>();

    // 主题配置（逻辑名 → 配置对象）
    @Getter(AccessLevel.NONE)
    private final Map<String, KafkaTopicConfig> topics = new HashMap<>();

    //============== 安全访问接口 ==============
    /** 获取只读服务器配置（基础连接参数） */
    public Map<String, Object> getServerConfig() {
        return Collections.unmodifiableMap(serverConfig);
    }

    /** 获取集群级生产者配置（副本） */
    public Map<String, Object> getServerProducerConfig() {
        return Collections.unmodifiableMap(serverProducerConfig); // 返回副本防篡改
    }

    /** 获取集群级消费者配置（副本） */
    public Map<String, Object> getServerConsumerConfig() {
        return Collections.unmodifiableMap(serverConsumerConfig);
    }

    /** 获取所有主题配置（不可修改视图） */
    public Map<String, KafkaTopicConfig> getTopicConfigs() {
        return Collections.unmodifiableMap(topics);
    }

    /** 按逻辑名获取主题配置 */
    public KafkaTopicConfig getTopicConfig(String logicalName) {
        return topics.get(logicalName);
    }

    //============== 链式配置方法 ==============
    /** 添加基础连接配置（如bootstrap.servers） */
    public KafkaServerConfig addServerConfig(String key, Object value) {
        serverConfig.put(key, value);
        return this;
    }

    /** 添加集群级生产者配置（如序列化器） */
    public KafkaServerConfig addServerProducerConfig(String key, Object value) {
        validateConfigKey(key, ProducerConfig.configNames());
        serverProducerConfig.put(key, value);
        return this;
    }

    /** 添加集群级消费者配置（如反序列化器） */
    public KafkaServerConfig addServerConsumerConfig(String key, Object value) {
        validateConfigKey(key, ConsumerConfig.configNames());
        serverConsumerConfig.put(key, value);
        return this;
    }

    /** 添加主题配置 */
    public KafkaServerConfig addTopicConfig(String logicalName, KafkaTopicConfig config) {
        topics.put(logicalName, config);
        return this;
    }

    //============== 配置校验 ==============
    /** 验证配置键合法性（防止拼写错误） */
    private void validateConfigKey(String key, Set<String> validKeys) {
        if (!validKeys.contains(key)) {
            throw new IllegalArgumentException("无效配置项: " + key);
        }
    }
}