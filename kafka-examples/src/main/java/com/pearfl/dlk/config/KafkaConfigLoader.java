package com.pearfl.dlk.config;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*** Kafka配置加载工具类
 *
 * 核心功能：
 * 1. 从YAML加载多集群配置（支持环境隔离）
 * 2. 实现配置继承机制：集群默认值 → 主题覆盖值
 * 3. 自动验证配置键有效性（防止拼写错误）
 * 4. 提供快速索引查询（O(1)复杂度）
 */
public class KafkaConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigLoader.class);

    /**
     * 加载并解析Kafka配置文件
     *
     * 执行流程：
     * 1. 加载YAML配置文件
     * 2. 解析全局元数据（版本、环境等）
     * 3. 递归解析各集群配置
     * 4. 构建配置索引
     *
     * @param configPath 类路径下的配置文件路径
     * @return 完全初始化的全局配置对象
     * @throws RuntimeException 配置文件不存在或解析失败时抛出
     */
    public static KafkaGlobalConfig loadConfig(String configPath) {
        try (InputStream inputStream = KafkaConfigLoader.class
                .getClassLoader()
                .getResourceAsStream(configPath)) {

            if (inputStream == null) {
                throw new RuntimeException("配置文件不存在: " + configPath);
            }

            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(inputStream);
            KafkaGlobalConfig globalConfig = new KafkaGlobalConfig()
                    .setVersion((String) root.get("version"))
                    .setAuthor((String) root.get("author"))
                    .setDescription((String) root.get("description"))
                    .setEnv((String) root.get("env"));

            // 解析所有集群配置
            Map<String, Map<String, Object>> kafkaConfig = (Map<String, Map<String, Object>>) root.get("kafka");
            for (Map.Entry<String, Map<String, Object>> entry : kafkaConfig.entrySet()) {
                globalConfig.addServerConfig(entry.getKey(), parseServerConfig(entry.getKey(), entry.getValue()));
            }

            return globalConfig;
        } catch (Exception e) {
            logger.error("配置文件解析失败: {}", configPath, e);
            throw new RuntimeException("配置加载异常", e);
        }
    }

    /**
     * 解析单个集群配置
     *
     * 配置继承逻辑：
     * ┌───────────────────┐
     * │ 集群基础配置        │ → bootstrap.servers等
     * ├───────────────────┤
     * │ 生产者默认配置      │ → 所有主题共享
     * ├───────────────────┤
     * │ 消费者默认配置      │ → 所有主题共享
     * └───────────────────┘
     *
     * @param clusterId 集群标识符（如kafka-prod）
     * @param clusterConfig 集群配置数据
     * @return 完全初始化的集群配置对象
     */
    private static KafkaServerConfig parseServerConfig(
            String clusterId, Map<String, Object> clusterConfig) {

        KafkaServerConfig serverConfig = new KafkaServerConfig().setClusterId(clusterId);

        // 解析基础连接配置（使用传统空值检查）
        Map<String, Object> serverProps = (Map<String, Object>) clusterConfig.get("server");
        if (serverProps != null) {
            serverProps.forEach(serverConfig::addServerConfig);
        }

        // 解析生产者默认配置（使用传统空值检查）
        Map<String, Object> clusterProducerProps = (Map<String, Object>) clusterConfig.get("producer");
        if (clusterProducerProps != null) {
            clusterProducerProps.forEach(
                    (k, v) -> serverConfig.addServerProducerConfig(k, v));
        }

        // 解析消费者默认配置（使用传统空值检查）
        Map<String, Object> clusterConsumerProps = (Map<String, Object>) clusterConfig.get("consumer");
        if (clusterConsumerProps != null) {
            clusterConsumerProps.forEach(
                    (k, v) -> serverConfig.addServerConsumerConfig(k, v));
        }

        // 解析主题配置（继承集群级默认值）
        Map<String, Map<String, Object>> topics = (Map<String, Map<String, Object>>) clusterConfig.get("topics");
        if (topics != null) {
            for (Map.Entry<String, Map<String, Object>> topicEntry : topics.entrySet()) {
                serverConfig.addTopicConfig(topicEntry.getKey(),
                        parseTopicConfig(topicEntry.getKey(),
                                topicEntry.getValue(),
                                serverConfig.getServerProducerConfig(),
                                serverConfig.getServerConsumerConfig()));
            }
        }

        return serverConfig;
    }

    /**
     * 构建主题配置（实现配置继承）
     *
     * 合并策略：
     * 1. 先注入集群级默认配置
     * 2. 用主题级配置覆盖默认值
     *
     * @param logicalName 主题逻辑名称
     * @param topicConfig 主题专属配置
     * @param defaultProducerProps 生产者默认配置
     * @param defaultConsumerProps 消费者默认配置
     * @return 合并后的主题配置对象
     */
    private static KafkaTopicConfig parseTopicConfig(
            String logicalName,
            Map<String, Object> topicConfig,
            Map<String, Object> defaultProducerProps,
            Map<String, Object> defaultConsumerProps) {

        KafkaTopicConfig topic = new KafkaTopicConfig().setTopicName(logicalName);

        // 合并生产者配置（默认值 + 覆盖值）
        defaultProducerProps.forEach(topic::addProducerConfig);
        Map<String, Object> topicProducerProps = (Map<String, Object>) topicConfig.get("producer");
        if (topicProducerProps != null) {
            topicProducerProps.forEach(topic::addProducerConfig);
        }

        // 合并消费者配置（默认值 + 覆盖值）
        defaultConsumerProps.forEach(topic::addConsumerConfig);
        Map<String, Object> topicConsumerProps = (Map<String, Object>) topicConfig.get("consumer");
        if (topicConsumerProps != null) {
            topicConsumerProps.forEach(topic::addConsumerConfig);
        }

        return topic;
    }
}