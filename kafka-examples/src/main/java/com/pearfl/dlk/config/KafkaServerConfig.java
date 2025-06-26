package com.pearfl.dlk.config;

import lombok.*;
import lombok.experimental.Accessors;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Data
@Accessors(chain = true)
public class KafkaServerConfig {
    private String clusterId;

    @Getter(AccessLevel.NONE)
    private final Map<String, Object> serverConfig = new HashMap<>();

    @Getter(AccessLevel.NONE)
    private final Map<String, KafkaTopicConfig> topics = new HashMap<>();

    // 安全获取只读服务器配置
    public Map<String, Object> getServerConfig() {
        return Collections.unmodifiableMap(serverConfig);
    }

    // 获取所有主题配置（不可修改视图）
    public Map<String, KafkaTopicConfig> getTopicConfigs() {
        return Collections.unmodifiableMap(topics);
    }

    // 获取单个主题配置
    public KafkaTopicConfig getTopicConfig(String logicalName) {
        return topics.get(logicalName);
    }

    // 链式添加配置项
    public KafkaServerConfig addServerConfig(String key, Object value) {
        serverConfig.put(key, value);
        return this;
    }

    // 链式添加主题配置
    public KafkaServerConfig addTopicConfig(String logicalName, KafkaTopicConfig config) {
        topics.put(logicalName, config);
        return this;
    }
}