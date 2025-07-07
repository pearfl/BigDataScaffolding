package com.pearfl.dlk.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import java.util.*;

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

    // 集群ID到集群配置的映射
    private final Map<String, KafkaServerConfig> servers = new HashMap<>();

    // 主题配置的快速索引：key为clusterId和topicName的组合，value为主题配置
    private final Map<String, KafkaTopicConfig> topicConfigMap = new HashMap<>();

    /**
     * 添加集群配置，同时建立主题配置的快速索引
     */
    public KafkaGlobalConfig addServerConfig(String clusterId, KafkaServerConfig config) {
        servers.put(clusterId, config);
        // 将该集群下的所有主题配置添加到全局主题配置映射中
        config.getTopicConfigs().forEach((topicName, topicConfig) -> {
            topicConfigMap.put(generateCombinedKey(clusterId, topicName), topicConfig);
        });
        return this;
    }

    private String generateCombinedKey(String clusterId, String topicName) {
        return clusterId + "|" + topicName;
    }

    // 获取所有集群配置（不可修改视图）
    public Map<String, KafkaServerConfig> getServerConfigs() {
        return Collections.unmodifiableMap(servers);
    }

    // 按集群ID获取集群配置
    public KafkaServerConfig getServerConfigById(String clusterId) {
        return servers.get(clusterId);
    }

    // 按集群ID和主题名获取主题配置
    public KafkaTopicConfig getTopicConfigByIds(String clusterId, String topicName) {
        return topicConfigMap.get(generateCombinedKey(clusterId, topicName));
    }
}