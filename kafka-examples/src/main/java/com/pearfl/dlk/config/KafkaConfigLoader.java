package com.pearfl.dlk.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.Field;

/**
 * Kafka配置加载工具类
 *
 * 功能说明：
 * 1. 从YAML配置文件加载Kafka集群配置
 * 2. 动态验证配置键有效性（使用反射机制）
 * 3. 自动转换配置键为Kafka官方常量
 */
public class KafkaConfigLoader {

    public static void main(String[] args) {
        try {
            // 1. 加载配置文件
            KafkaGlobalConfig globalConfig = loadConfig("kafka-config.yml");

            // 2. 打印全局配置
            System.out.println("===== GLOBAL CONFIG =====");
            System.out.println("Version: " + globalConfig.getVersion());
            System.out.println("Author: " + globalConfig.getAuthor());
            System.out.println("Description: " + globalConfig.getDescription());
            System.out.println("Environment: " + globalConfig.getEnv());
            System.out.println("================================\n");

            // 3. 遍历所有集群配置
            for (String clusterId : globalConfig.getServerConfigs().keySet()) {
                KafkaServerConfig serverConfig = globalConfig.getServerConfig(clusterId);

                // 4. 打印集群基础信息
                System.out.println("===== CLUSTER CONFIG: " + clusterId + " =====");
                System.out.println("Cluster ID: " + serverConfig.getClusterId());

                // 5. 打印服务器配置
                System.out.println("\nSERVER CONFIGURATION:");
                for (Map.Entry<String, Object> entry : serverConfig.getServerConfig().entrySet()) {
                    System.out.printf("  %-30s = %s%n", entry.getKey(), entry.getValue());
                }

                // 6. 遍历所有主题配置
                System.out.println("\nTOPIC CONFIGURATIONS:");
                for (String topicName : serverConfig.getTopicConfigs().keySet()) {
                    KafkaTopicConfig topic = serverConfig.getTopicConfig(topicName);

                    // 7. 打印主题基础信息
                    System.out.println("\n  --- TOPIC: " + topicName + " ---");
                    System.out.println("  Actual Topic Name: " + topic.getTopicName());

                    // 8. 打印生产者配置
                    System.out.println("\n  PRODUCER SETTINGS:");
                    // 使用getProducerProperties()获取Properties，然后遍历
                    for (Map.Entry<Object, Object> entry : topic.getProducerProperties().entrySet()) {
                        System.out.printf("    %-30s = %s%n", entry.getKey(), entry.getValue());
                    }

                    // 9. 打印消费者配置
                    System.out.println("\n  CONSUMER SETTINGS:");
                    // 使用getConsumerProperties()获取Properties，然后遍历
                    for (Map.Entry<Object, Object> entry : topic.getConsumerProperties().entrySet()) {
                        System.out.printf("    %-30s = %s%n", entry.getKey(), entry.getValue());
                    }
                }
                System.out.println("========================================\n");
            }

            // 10. 模拟配置验证错误
            System.out.println("TESTING INVALID CONFIG HANDLING...");
            try {
                // 触发无效配置异常
                getKafkaConstant("invalid.config", ProducerConfig.class, "Producer");
            } catch (IllegalArgumentException e) {
                System.out.println("ERROR HANDLING DEMO:");
                System.out.println(e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("FATAL ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

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
      * 解析单个Kafka集群配置
     *
      * @param clusterId 集群标识符
     * @param clusterConfig 集群配置数据
     * @return 服务器配置对象
     */
    private static KafkaServerConfig parseServerConfig(
            String clusterId, Map<String, Object> clusterConfig) {

        KafkaServerConfig serverConfig = new KafkaServerConfig()
                .setClusterId(clusterId);

        // 处理通用服务配置
        Map<String, Object> serverProps = (Map) clusterConfig.get("server");
        for (Map.Entry<String, Object> prop : serverProps.entrySet()) {
            serverConfig.addServerConfig(prop.getKey(), prop.getValue());
        }

        // 遍历所有主题配置
        Map<String, Map<String, Object>> topics = (Map) clusterConfig.get("topics");
        for (Map.Entry<String, Map<String, Object>> topicEntry : topics.entrySet()) {
            KafkaTopicConfig topic = parseTopicConfig(
                    topicEntry.getKey(),
                    topicEntry.getValue()
            );
            serverConfig.addTopicConfig(topicEntry.getKey(), topic);
        }
        return serverConfig;
    }

    /**
     * 解析主题级别配置
     *
     * @param topicName 主题名称
     * @param topicConfig 主题配置数据
     * @return 主题配置对象
     */
    private static KafkaTopicConfig parseTopicConfig(
            String topicName, Map<String, Object> topicConfig) {

        KafkaTopicConfig topic = new KafkaTopicConfig()
                .setTopicName(topicName);

        // 生产者配置验证与加载
        Map<String, Object> producerProps = (Map) topicConfig.get("producer");
        for (Map.Entry<String, Object> prop : producerProps.entrySet()) {
            String kafkaKey = getKafkaConstant(
                    prop.getKey(),
                    ProducerConfig.class,
                    "Producer"
            );
            topic.addProducerConfig(kafkaKey, prop.getValue());
        }

        // 消费者配置验证与加载
        Map<String, Object> consumerProps = (Map) topicConfig.get("consumer");
        for (Map.Entry<String, Object> prop : consumerProps.entrySet()) {
            String kafkaKey = getKafkaConstant(
                    prop.getKey(),
                    ConsumerConfig.class,
                    "Consumer"
            );
            topic.addConsumerConfig(kafkaKey, prop.getValue());
        }
        return topic;
    }


    /**
     * 通过反射获取Kafka配置常量
     *
     * 实现原理：
     * 1. 将配置键转换为大写并替换特殊字符（如batch.size -> BATCH_SIZE）
     * 2. 追加_CONFIG后缀形成常量名
     * 3. 通过反射从指定配置类获取字段值
     *
     *
     * @param yamlKey 配置键（如"acks"）
     * @param configClass 配置类（ProducerConfig.class或ConsumerConfig.class）
     * @param configType 配置类型描述（用于错误信息）
     * @return Kafka标准配置键
     * @throws IllegalArgumentException 当常量不存在时抛出
     */
    private static String getKafkaConstant(
            String yamlKey,
            Class<?> configClass,
            String configType) {

        // 格式转换规则：替换特殊字符并标准化命名
        String constantName = yamlKey.toUpperCase()
                .replace('.', '_')
                .replace('-', '_')
                + "_CONFIG";

        try {
            // 反射获取字段值
            Field field = configClass.getDeclaredField(constantName);
            return (String) field.get(null);
        } catch (NoSuchFieldException e) {
            // 构造详细错误信息
            String errorMsg = String.format(
                    "Invalid %s config key: '%s' (Attempted mapping: %s.%s)%n" +
                            "Possible causes: 1. Typo in key 2. Unsupported in Kafka %s",
                    configType, yamlKey, configClass.getSimpleName(), constantName,
                    kafkaVersion()
            );
            throw new IllegalArgumentException(errorMsg);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Field access denied: " + constantName, e);
        }
    }

    /**
     * 获取当前Kafka客户端版本（示例方法）
     */
    private static String kafkaVersion() {
        return "3.9.1";  // 实际项目中应从依赖获取
    }
}