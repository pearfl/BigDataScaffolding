package com.pearfl.dlk.springboot2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Spring Boot 2.7.18 + Kafka ≥ 2.7 完整集成示例
 * 功能亮点：
 *  - 使用CompletableFuture异步发送[2,6](@ref)
 *  - JSON消息序列化支持
 *  - 手动提交偏移量配置
 *  - 异常处理机制
 *  - REST API消息发送接口
 */
@SpringBootApplication
public class SpringBootKafkaDemo {

    private static final String TOPIC = "DLK_TEST_TOPIC_MXH_01_DEV";

    public static void main(String[] args) {
        SpringApplication.run(SpringBootKafkaDemo.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(KafkaProducerService producerService) {
        return args -> {
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", 1001);
            payload.put("name", "SpringBoot测试");
            payload.put("status", "active");

            Map<String, Object> message = new HashMap<>();
            message.put("event", "system_init");
            message.put("timestamp", System.currentTimeMillis());
            message.put("payload", payload);

            producerService.sendJsonMessage(TOPIC, message);
        };
    }

    /**
     * Kafka生产者服务
     */
    @Service
    public static class KafkaProducerService {
        @Autowired
        private KafkaTemplate<String, Object> kafkaTemplate;

        /**
         * 发送JSON消息（异步+回调）
         * @param topic 目标主题
         * @param message 消息内容
         */
        public void sendJsonMessage(String topic, Object message) {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.printf("[Producer] ✅ 发送成功! 主题:%s 分区:%d 偏移量:%d%n",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    System.err.printf("[Producer] ❌ 发送失败: %s%n", ex.getMessage());
                    // 生产环境建议添加重试逻辑
                }
            });
        }
    }

    /**
     * Kafka消费者服务
     */
    @Service
    public static class KafkaConsumerService {
        /**
         * 监听指定主题的消息[4,5](@ref)
         * @param record 消息记录
         */
        @KafkaListener(topics = TOPIC, groupId = "springboot-consumer-group")
        public void consume(ConsumerRecord<String, Map<String, Object>> record) {
            try {
                Map<String, Object> value = record.value();
                Map<?, ?> payload = (Map<?, ?>) value.get("payload");

                System.out.printf(
                        "\n[Consumer] 收到消息 → 分区:%d 偏移量:%d\n  事件:%s\n  设备ID:%s\n  状态:%s\n",
                        record.partition(),
                        record.offset(),
                        value.get("event"),
                        payload.get("id"),
                        payload.get("status")
                );
            } catch (Exception e) {
                System.err.printf(
                        "[Consumer] ❌ 消息处理失败! Key:%s Value:%s\n  错误:%s\n",
                        record.key(), record.value(), e.getMessage()
                );
            }
        }
    }

    /**
     * REST消息发送接口
     */
    @RestController
    @RequestMapping("/kafka")
    public static class KafkaController {
        @Autowired
        private KafkaProducerService producerService;

        @PostMapping("/send")
        public String sendMessage(
                @RequestParam String event,
                @RequestParam String deviceId
        ) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", deviceId);
            payload.put("status", "active");

            Map<String, Object> message = new HashMap<>();
            message.put("event", event);
            message.put("timestamp", System.currentTimeMillis());
            message.put("payload", payload);

            producerService.sendJsonMessage(TOPIC, message);
            return "消息已发送: " + event;
        }
    }
}