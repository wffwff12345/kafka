package com.example.kafka.Context;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

public class KafkaConsumerContext {

    private static final Map<String, KafkaConsumer<?, ?>> consumerMap = new ConcurrentHashMap<>();

    private static final Map<String, ScheduledFuture<?>> scheduledMap = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(24);

    /**
     * 移除Kafka消费者定时任务并关闭消费者订阅
     *
     * @param groupId
     */
    public static void removeConsumerTask(String groupId) {
        if (!consumerMap.containsKey(groupId)) {
            return;
        }
        consumerMap.remove(groupId);
    }

    /**
     * 增加Kafka消费者定时任务
     *
     * @param groupId
     * @param kafkaConsumer
     * @param <K>
     * @param <V>
     */
    public static <K, V> void addConsumerTask(String groupId, KafkaConsumer<K, V> kafkaConsumer) {
        consumerMap.put(groupId, kafkaConsumer);
        // 创建定时任务，每隔1s拉取消息并处理
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(() -> {
            // 每次执行拉取消息之前，先检查订阅者是否已被取消（如果订阅者不存在于订阅者列表中说明被取消了）
            // 因为Kafka消费者对象是非线程安全的，因此在这里把取消订阅的逻辑和拉取并处理消息的逻辑写在一起并放入定时器中，判断列表中是否存在消费者对象来确定是否取消任务
            if (!consumerMap.containsKey(groupId)) {
                kafkaConsumer.unsubscribe();
                kafkaConsumer.close();
                scheduledMap.remove(groupId).cancel(true);
                return;
            }
            // 拉取消息
            ConsumerRecords<K, V> poll = kafkaConsumer.poll(Duration.ofMillis(100));
            poll.forEach(x-> System.out.println(x.value()));
        }, 0, 1, TimeUnit.MICROSECONDS);
        scheduledMap.put(groupId, scheduledFuture);

    }
}
