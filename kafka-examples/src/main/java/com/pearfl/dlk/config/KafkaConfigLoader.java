package com.pearfl.dlk.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;
import java.lang.reflect.Field;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka配置加载工具类
 *
 * 功能说明：
 * 1. 从YAML配置文件加载Kafka集群配置
 * 2. 动态验证配置键有效性（使用反射机制）
 * 3. 自动转换配置键为Kafka官方常量
 */
public class KafkaConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigLoader.class);

    /**
     * 加载并解析Kafka配置文件
     *
     * @param configPath 配置文件的类路径地址
     * @return 全局配置对象
     * @throws RuntimeException 当文件不存在或配置解析失败时抛出
     */
    public static KafkaGlobalConfig loadConfig(String configPath) {
        // 通过类加载器获取资源流
        InputStream inputStream = KafkaConfigLoader.class
                .getClassLoader()
                .getResourceAsStream(configPath);

        // 资源存在性校验
        if (inputStream == null) {
            throw new RuntimeException("Config file not found: " + configPath);
        }

        // 创建YAML解析器实例
        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(inputStream);

        // 构建全局配置基础信息
        KafkaGlobalConfig globalConfig = new KafkaGlobalConfig()
                .setVersion((String) root.get("version"))  // 读取版本号
                .setAuthor((String) root.get("author"))    // 读取作者信息
                .setDescription((String) root.get("description"))  // 读取描述
                .setEnv((String) root.get("env"));         // 读取环境标识

        // 解析集群配置节点
        Map<String, Map<String, Object>> kafkaConfig = (Map) root.get("kafka");
        for (Map.Entry<String, Map<String, Object>> clusterEntry : kafkaConfig.entrySet()) {
            String clusterId = clusterEntry.getKey();
            KafkaServerConfig serverConfig = parseServerConfig(
                    clusterId,
                    clusterEntry.getValue()
            );
            globalConfig.addServerConfig(clusterId, serverConfig);
        }
        return globalConfig;
    }

     /**
      * 解析集群级别配置并构建服务器配置对象
      *
      * 实现功能：
      * 1. 加载基础连接配置（如 bootstrap.servers）
      * 2. 加载集群级生产者/消费者默认配置
      * 3. 构建主题配置并继承集群级默认值
      *
      * 配置继承机制：
      *   集群级配置 → 主题级配置（后者可覆盖前者）
      *
      * 处理流程：
      *   ┌───────────────────────┐
      *   │ 1. 解析server基础配置   │
      *   ├───────────────────────┤
      *   │ 2. 解析producer配置    │→ serverProducerConfig
      *   ├───────────────────────┤
      *   │ 3. 解析consumer配置    │→ serverConsumerConfig
      *   ├───────────────────────┤
      *   │ 4. 获取配置副本        │（用于主题级配置继承）
      *   ├───────────────────────┤
      *   │ 5. 解析topics节点      │→ 注入集群级默认配置
      *   └───────────────────────┘
      *
      * @param clusterId 集群唯一标识符（如：kafka-cluster-1）
      * @param clusterConfig 集群配置数据（YAML解析后的Map结构）
      * @return 完全初始化的KafkaServerConfig对象
      * @see KafkaServerConfig#addServerConfig(String, Object) 基础配置添加方法
      * @see KafkaServerConfig#addServerProducerConfig(String, Object) 生产者默认配置
      */
     private static KafkaServerConfig parseServerConfig(
             String clusterId, Map<String, Object> clusterConfig) {

         KafkaServerConfig serverConfig = new KafkaServerConfig().setClusterId(clusterId);

         // 1. 解析基础连接配置
         Map<String, Object> serverProps = (Map) clusterConfig.get("server");
         if (serverProps != null) {
             serverProps.forEach(serverConfig::addServerConfig);
         }

         // 2. 解析集群级生产者配置
         Map<String, Object> clusterProducerProps = (Map) clusterConfig.get("producer");
         if (clusterProducerProps != null) {
             clusterProducerProps.forEach((key, value) ->
                     serverConfig.addServerProducerConfig((String) key, value)
             );
         }

         // 3. 解析集群级消费者配置
         Map<String, Object> clusterConsumerProps = (Map) clusterConfig.get("consumer");
         if (clusterConsumerProps != null) {
             clusterConsumerProps.forEach((key, value) ->
                     serverConfig.addServerConsumerConfig((String) key, value)
             );
         }

         // 4. 获取集群级配置副本（用于主题继承）
         Properties serverProducerProps = serverConfig.getServerProducerConfig();
         Properties serverConsumerProps = serverConfig.getServerConsumerConfig();

         // 5. 解析主题配置（继承集群级默认值）
         Map<String, Map<String, Object>> topics = (Map) clusterConfig.get("topics");
         if (topics != null) {
             topics.forEach((logicalName, topicConfig) -> {
                 KafkaTopicConfig topic = parseTopicConfig(
                         logicalName,
                         topicConfig,
                         serverProducerProps,
                         serverConsumerProps
                 );
                 serverConfig.addTopicConfig(logicalName, topic);
             });
         }
         return serverConfig;
     }

    /**
     * 构建主题配置对象并实现配置继承
     *
     * 核心逻辑：
     *   集群默认配置 + 主题覆盖配置 → 最终生效配置
     *
     * 配置合并优先级：
     *   ┌──────────────────────────────┐
     *   │ 主题级配置参数                 │ → 最高优先级（覆盖集群默认）
     *   ├──────────────────────────────┤
     *   │ 集群级默认配置                 │ → 基础值（可被主题覆盖）
     *   └──────────────────────────────┘
     *
     * 特殊处理：
     *   - 生产者配置：先注入集群级默认值，再合并主题级覆盖
     *   - 消费者配置：同生产者合并逻辑
     *   - 未显式配置的参数：保留集群级默认值
     *
     * @param logicalName 主题逻辑名称（如：order-events）
     * @param topicConfig 主题专属配置数据
     * @param defaultProducerProps 集群级生产者默认配置（不可变副本）
     * @param defaultConsumerProps 集群级消费者默认配置（不可变副本）
     * @return 合并后的主题配置对象
     * @see KafkaTopicConfig#addProducerConfig(String, Object) 生产者配置合并
     * @see KafkaTopicConfig#addConsumerConfig(String, Object) 消费者配置合并
     *
     * 示例：
     *   集群配置：key.serializer = StringSerializer
     *   主题配置：key.serializer = AvroSerializer
     *   生效值：key.serializer = AvroSerializer
     */
    private static KafkaTopicConfig parseTopicConfig(
            String logicalName,
            Map<String, Object> topicConfig,
            Properties defaultProducerProps,
            Properties defaultConsumerProps) {

        KafkaTopicConfig topic = new KafkaTopicConfig().setTopicName(logicalName);

        // 1. 合并生产者配置（集群默认值 + 主题覆盖）
        defaultProducerProps.forEach((key, value) ->
                topic.addProducerConfig((String) key, value)
        );
        Map<String, Object> topicProducerProps = (Map) topicConfig.get("producer");
        if (topicProducerProps != null) {
            topicProducerProps.forEach((key, value) ->
                    topic.addProducerConfig((String) key, value)
            );
        }

        // 2. 合并消费者配置（集群默认值 + 主题覆盖）
        defaultConsumerProps.forEach((key, value) ->
                topic.addConsumerConfig((String) key, value)
        );
        Map<String, Object> topicConsumerProps = (Map) topicConfig.get("consumer");
        if (topicConsumerProps != null) {
            topicConsumerProps.forEach((key, value) ->
                    topic.addConsumerConfig((String) key, value)
            );
        }
        return topic;
    }

    /**
     * 获取当前Kafka客户端版本（示例方法）
     */
    private static String kafkaVersion() {
        // 成功连接集群后，利用 AdminClient API 获取实际版本
        return "3.9.1";  // 实际项目中应从依赖获取
    }
}