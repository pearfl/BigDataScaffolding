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
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

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

    @Service
    public static class KafkaProducerService {
        @Autowired
        private KafkaTemplate<String, Object> kafkaTemplate;

        public void sendJsonMessage(String topic, Object message) {
            ListenableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, message);

            future.addCallback(
                    // 成功回调
                    result -> {
                        assert result != null;
                        System.out.printf("[Producer] ✅ 发送成功! 主题:%s 分区:%d 偏移量:%d%n",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    },
                    // 失败回调
                    ex -> System.err.printf("[Producer] ❌ 发送失败: %s%n", ex.getMessage())
            );
        }
    }

    @Service
    public static class KafkaConsumerService {
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