package com.pearfl.dlk.config;

import lombok.*;
import lombok.experimental.Accessors;
import java.util.*;

/**
 * Kafka全局配置容器 - 管理所有Kafka集群配置
 *
 * 核心职责：
 * 1. 存储全局元数据（版本、环境等）
 * 2. 管理集群ID到集群配置的映射
 * 3. 提供快速配置检索方法
 */
@Getter
@Setter
@Accessors(chain = true)
@ToString
public class KafkaGlobalConfig {
    /** 元数据信息 */
    private String version;         // 配置版本
    private String author;          // 配置作者
    private String description;     // 配置描述
    private String env;             // 运行环境（dev/test/prod）

    /** 集群存储：集群ID → 集群配置 */
    private final Map<String, KafkaServerConfig> serversConfig = new HashMap<>();

    /**
     * 添加集群配置到全局管理器
     *
     * @param clusterId 集群唯一标识符
     * @param config 集群配置对象
     * @return 当前对象（支持链式调用）
     */
    public KafkaGlobalConfig addServerConfig(String clusterId, KafkaServerConfig config) {
        serversConfig.put(clusterId, config);
        return this;
    }

    /**
     * 获取所有集群配置（不可修改视图）
     *
     * @return 集群ID到配置的只读映射
     */
    public Map<String, KafkaServerConfig> getServerConfigs() {
        return Collections.unmodifiableMap(serversConfig);
    }

    /**
     * 通过集群ID获取集群配置
     *
     * @param clusterId 集群唯一标识符
     * @return 对应的集群配置对象（未找到返回null）
     */
    public KafkaServerConfig getServerConfigById(String clusterId) {
        return serversConfig.get(clusterId);
    }

    /**
     * 通过集群ID和主题名获取主题配置
     *
     * @param clusterId 集群唯一标识符
     * @param topicName 主题逻辑名称
     * @return 对应的主题配置对象（未找到返回null）
     */
    public KafkaTopicConfig getTopicConfigByClusterIdAndTopicName(String clusterId, String topicName) {
        KafkaServerConfig serverConfig = serversConfig.get(clusterId);
        // 双重检查：集群存在性检查 + 主题存在性检查
        return serverConfig != null ? serverConfig.getTopicsConfig().get(topicName) : null;
    }
}