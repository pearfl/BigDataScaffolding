package com.pearfl.dlk.springboot2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class KafkaScheduledConsumerApp {

    private static final String TOPIC = "DLK_TEST_TOPIC_MXH_01_DEV";

    public static void main(String[] args) {
        SpringApplication.run(KafkaScheduledConsumerApp.class, args);
    }

    // 创建专用的任务调度器
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("kafka-scheduler-");
        scheduler.initialize();
        return scheduler;
    }

    @Configuration
    public static class KafkaConfig {
        @Autowired
        private ConsumerFactory<String, Map<String, Object>> consumerFactory;

        @Bean("scheduledContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, Map<String, Object>> scheduledContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, Map<String, Object>> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setAutoStartup(false); // 禁止自启动

            // 配置手动提交偏移量
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

            return factory;
        }
    }

    @Service
    public static class KafkaScheduledConsumer {
        @KafkaListener(
                id = "fiveMinuteListener",
                topics = TOPIC,
                groupId = "scheduled-consumer-group",
                containerFactory = "scheduledContainerFactory"
        )
        public void consume(ConsumerRecord<String, Map<String, Object>> record) {
            try {
                Map<String, Object> value = record.value();
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = (Map<String, Object>) value.get("payload");

                System.out.printf(
                        "\n[定时消费] 收到消息 → 时间:%tT 分区:%d\n  事件:%s\n  设备ID:%s\n  状态:%s%n",
                        new Date(),
                        record.partition(),
                        value.get("event"),
                        payload.get("id"),
                        payload.get("status")
                );
            } catch (Exception e) {
                System.err.printf(
                        "[定时消费] ❌ 处理失败! Key:%s\n  错误:%s%n",
                        record.key(), e.getMessage()
                );
            }
        }
    }

    @Service
    public static class SchedulerService {
        @Autowired
        private KafkaListenerEndpointRegistry registry;

        @Autowired
        private TaskScheduler taskScheduler;

        // 使用原子布尔值替代volatile
        private final AtomicBoolean isRunning = new AtomicBoolean(false);

        @Scheduled(cron = "0 0/5 * * * ?")
        public void startListenerOnSchedule() {
            if (!isRunning.get()) {
                MessageListenerContainer container = registry.getListenerContainer("fiveMinuteListener");

                // 确保容器存在且未运行
                if (container != null && !container.isRunning()) {
                    container.start();
                    isRunning.set(true);
                    System.out.printf("%n--- 启动消费监听器 [%s] ---%n", new Date());

                    // 使用Spring TaskScheduler调度停止任务
                    taskScheduler.schedule(this::stopListener,
                            new Date(System.currentTimeMillis() + 290 * 1000)); // 290秒后
                }
            }
        }

        public void stopListener() {
            MessageListenerContainer container = registry.getListenerContainer("fiveMinuteListener");
            if (container != null && container.isRunning()) {
                container.pause(); // 暂停容器
                isRunning.set(false);
                System.out.printf("--- 停止消费监听器 [%s] ---%n%n", new Date());
            }
        }
    }
}