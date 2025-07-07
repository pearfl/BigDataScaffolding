package com.pearfl.dlk.provider;

import com.pearfl.dlk.config.KafkaConfigLoader;
import com.pearfl.dlk.config.KafkaGlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pearfl.dlk.config.KafkaConfigLoader.loadConfig;

public class KafkaConfigOutput {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigLoader.class);

    private static String repeatString(String str, int count) {
        return Optional.ofNullable(str)
                .filter(s -> count > 0)
                .map(s -> String.join("", Collections.nCopies(count, s)))
                .orElse("");
    }

    private static void logKeyValueConfig(String header, Map<String, Object> config, int indent) {
        String indentStr = repeatString("  ", indent);
        logger.info("\n{}{}:", indentStr, header);

        config.forEach((key, value) -> {
            int spaces = Math.max(0, 30 - key.length());
            logger.info("{}{}{} = {}",
                    indentStr,
                    key,
                    repeatString(" ", spaces),
                    value);
        });
    }

    public static void main(String[] args) {
        try {
            KafkaGlobalConfig globalConfig = loadConfig("kafka-config.yml");

            // 全局配置日志
            logger.info("===== GLOBAL CONFIG =====");
            Stream.of(
                    new AbstractMap.SimpleImmutableEntry<>("Version", globalConfig.getVersion()),
                    new AbstractMap.SimpleImmutableEntry<>("Author", globalConfig.getAuthor()),
                    new AbstractMap.SimpleImmutableEntry<>("Description", globalConfig.getDescription()),
                    new AbstractMap.SimpleImmutableEntry<>("Environment", globalConfig.getEnv())
            ).forEach(e -> logger.info("{}: {}", e.getKey(), e.getValue()));
            logger.info("================================\n");

            // 集群配置处理
            globalConfig.getServerConfigs().forEach((clusterId, serverConfig) -> {
                logger.info("===== CLUSTER CONFIG: {} =====", clusterId);
                logger.info("Cluster ID: {}", serverConfig.getClusterId());

                // 使用函数式处理三类配置
                Map<String, Map<String, Object>> configGroups = new LinkedHashMap<>();
                configGroups.put("SERVER CONFIGURATION", serverConfig.getServerConfig());
                configGroups.put("SERVER PRODUCER CONFIGURATION", serverConfig.getServerProducerConfig());
                configGroups.put("SERVER CONSUMER CONFIGURATION", serverConfig.getServerConsumerConfig());

                configGroups.forEach((name, config) ->
                        logKeyValueConfig(name, config, 0)
                );

                // 主题配置处理
                logger.info("\nTOPIC CONFIGURATIONS:");
                serverConfig.getTopicConfigs().forEach((topicName, topic) -> {
                    logger.info("\n  --- TOPIC: {} ---", topicName);
                    logger.info("  Actual Topic Name: {}", topic.getTopicName());

                    // 统一处理生产者和消费者配置
                    Map<String, Map<Object, Object>> topicConfigs = new HashMap<>();
                    topicConfigs.put("PRODUCER SETTINGS", topic.getProducerProperties());
                    topicConfigs.put("CONSUMER SETTINGS", topic.getConsumerProperties());

                    topicConfigs.forEach((name, config) ->
                            logKeyValueConfig(name, config.entrySet().stream()
                                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)),
                                    1)
                    );
                });
                logger.info("========================================\n");
            });
        } catch (Exception e) {
            logger.error("FATAL ERROR: {}", e.getMessage(), e);
        }
    }

    // Properties转Map的Java 8实现
    private static Map<String, Object> toMap(Properties props) {
        return props.stringPropertyNames()
                .stream()
                .collect(Collectors.toMap(
                        key -> key,
                        props::getProperty
                ));
    }
}
