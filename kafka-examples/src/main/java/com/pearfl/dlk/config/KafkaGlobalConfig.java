package com.pearfl.dlk.config;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class KafkaGlobalConfig {
    // 元数据信息
    private String version;
    private String author;
    private String description;
    private String env;

    // 基础存储（初始化后只读）
    @Getter(AccessLevel.NONE)
    private final Map<String, KafkaServerConfig> servers = new HashMap<>();

    // 快速索引（线程安全）
    private final Map<String, KafkaServerConfig> serverConfigMap = new ConcurrentHashMap<>();
    private final Map<String, KafkaTopicConfig> topicConfigMap = new ConcurrentHashMap<>();

    //============== 核心方法 ==============
    /**
     * 安全添加集群配置（自动同步索引）
     */
    public KafkaGlobalConfig addServerConfig(String clusterId, KafkaServerConfig config) {
        servers.put(clusterId, config);
        serverConfigMap.put(clusterId, config);
        config.getTopicConfigs().forEach((topicName, topicConfig) -> {
            topicConfigMap.put(generateCombinedKey(clusterId, topicName), topicConfig);
        });
        return this;
    }

    /**
     * 生成主题配置组合键（线程安全分隔符）
     */
    public String generateCombinedKey(String clusterId, String topicName) {
        return clusterId + "|" + topicName; // Kafka主题名禁用'|'字符[3](@ref)
    }

    //============== 查询接口 ==============
    /** 获取所有集群（不可修改视图） */
    public Map<String, KafkaServerConfig> getServerConfigs() {
        return Collections.unmodifiableMap(servers);
    }

    /** 按集群ID获取配置（O(1)查询） */
    public KafkaServerConfig getServerConfigById(String clusterId) {
        return serverConfigMap.get(clusterId);
    }

    /** 按集群+主题获取配置（O(1)查询） */
    public KafkaTopicConfig getTopicConfigByIds(String clusterId, String topicName) {
        return topicConfigMap.get(generateCombinedKey(clusterId, topicName));
    }
}