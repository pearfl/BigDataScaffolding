package com.pearfl.dlk.config;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka配置加载器 - 负责从YAML配置文件加载和解析多集群Kafka配置
 *
 * 核心功能：
 * 1. 加载并解析YAML格式的配置文件
 * 2. 构建全局配置对象（KafkaGlobalConfig）
 * 3. 处理配置继承：集群默认值 → 主题覆盖值
 * 4. 提供配置键有效性验证（防止拼写错误）
 *
 * 设计特点：
 * - 支持环境隔离（dev/test/prod）
 * - 提供O(1)复杂度的配置查询
 * - 配置值继承机制确保灵活性
 */
public class KafkaConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigLoader.class);

    /**
     * 加载并解析Kafka配置文件
     *
     * @param configPath 类路径下的配置文件路径
     * @return 完全初始化的全局配置对象
     * @throws RuntimeException 当配置文件不存在或解析失败时抛出
     */
    public static KafkaGlobalConfig loadConfig(String configPath) {
        // 从类路径加载资源文件
        try (InputStream is = KafkaConfigLoader.class.getClassLoader().getResourceAsStream(configPath)) {
            // 配置存在性检查
            if (is == null) throw new RuntimeException("Config not found: " + configPath);

            // 使用SnakeYAML解析YAML文件
            Map<String, Object> root = new Yaml().load(is);

            // 构建全局配置基础信息
            KafkaGlobalConfig config = new KafkaGlobalConfig()
                    .setVersion((String) root.get("version"))        // 配置版本号
                    .setAuthor((String) root.get("author"))          // 配置作者
                    .setDescription((String) root.get("description"))// 配置描述
                    .setEnv((String) root.get("env"));               // 运行环境标识

            // 解析kafka配置块（多集群配置）
            Map<String, Map<String, Object>> kafkaConfig = (Map<String, Map<String, Object>>) root.get("kafka");

            // 遍历并解析每个集群配置
            kafkaConfig.forEach((clusterId, clusterMap) ->
                    config.addServerConfig(clusterId, parseServerConfig(clusterId, clusterMap)));

            return config;
        } catch (Exception e) {
            log.error("Config parsing failed: {}", configPath, e);
            throw new RuntimeException("Configuration error", e);
        }
    }

    /**
     * 解析单个Kafka集群配置
     *
     * @param clusterId 集群唯一标识符（如kafka-prod）
     * @param config 集群配置数据映射
     * @return 完全初始化的集群配置对象
     */
    private static KafkaServerConfig parseServerConfig(String clusterId, Map<String, Object> config) {
        // 创建基础集群配置对象
        KafkaServerConfig server = new KafkaServerConfig().setClusterId(clusterId);

        // 配置分区处理：
        // server - 基础连接配置（bootstrap.servers等）
        // producer - 集群级生产者默认配置
        // consumer - 集群级消费者默认配置
        addConfigSection(config, "server", server::addServerConfig);
        addConfigSection(config, "producer", server::addServerProducerConfig);
        addConfigSection(config, "consumer", server::addServerConsumerConfig);

        // 解析主题配置（继承集群级默认值）
        Map<String, Map<String, Object>> topics = (Map<String, Map<String, Object>>) config.get("topics");
        if (topics != null) {
            topics.forEach((topicName, topicMap) -> server.addTopicConfig(topicName,
                    parseTopicConfig(topicName, topicMap,
                            server.getServerProducerConfig(),  // 传递生产者默认值
                            server.getServerConsumerConfig())  // 传递消费者默认值
            ));
        }
        return server;
    }

    /**
     * 构建主题配置（实现配置继承机制）
     *
     * 合并策略：
     * 1. 先注入集群级默认配置
     * 2. 用主题级配置覆盖默认值
     *
     * @param name 主题逻辑名称
     * @param config 主题专属配置数据
     * @param defaultProducer 集群级生产者默认配置
     * @param defaultConsumer 集群级消费者默认配置
     * @return 合并后的主题配置对象
     */
    private static KafkaTopicConfig parseTopicConfig(String name,
                                                     Map<String, Object> config,
                                                     Map<String, Object> defaultProducer,
                                                     Map<String, Object> defaultConsumer) {

        KafkaTopicConfig topic = new KafkaTopicConfig().setTopicName(name);

        // 应用生产者默认配置
        defaultProducer.forEach(topic::addProducerConfig);

        // 应用消费者默认配置
        defaultConsumer.forEach(topic::addConsumerConfig);

        // 应用主题级覆盖配置（会覆盖默认值）
        addConfigSection(config, "producer", topic::addProducerConfig);
        addConfigSection(config, "consumer", topic::addConsumerConfig);

        return topic;
    }

    /**
     * 通用配置分区处理方法
     *
     * @param source 源配置映射
     * @param section 配置分区名称（如"producer"）
     * @param adder 键值添加处理器
     */
    private static void addConfigSection(Map<String, Object> source, String section, KeyValueAdder adder) {
        // 从配置映射中获取指定分区
        Map<String, Object> props = (Map<String, Object>) source.get(section);

        // 如果分区存在，遍历并处理所有键值对
        if (props != null) {
            props.forEach(adder::add);
        }
    }

    /** 函数式接口 - 键值添加处理器 */
    @FunctionalInterface
    private interface KeyValueAdder {
        void add(String key, Object value);
    }
}