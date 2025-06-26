package com.pearfl.dlk.config;

import lombok.*;
import lombok.experimental.Accessors;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class KafkaGlobalConfig {
    private String version;
    private String author;
    private String description;
    private String env;

    @Getter(AccessLevel.NONE)
    private final Map<String, KafkaServerConfig> servers = new HashMap<>();

    // 返回不可修改的配置映射
    public Map<String, KafkaServerConfig> getServerConfigs() {
        return Collections.unmodifiableMap(servers);
    }

    // 获取单个集群配置
    public KafkaServerConfig getServerConfig(String clusterId) {
        return servers.get(clusterId);
    }

    // 安全添加配置
    public KafkaGlobalConfig addServerConfig(String clusterId, KafkaServerConfig config) {
        servers.put(clusterId, config);
        return this;
    }
}