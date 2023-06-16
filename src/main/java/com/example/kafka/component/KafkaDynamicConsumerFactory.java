package com.example.kafka.component;

import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaDynamicConsumerFactory {

    @Resource
    private KafkaProperties kafkaProperties;

    @Value("${kafkaServer.server}")
    private String Server;

    /**
     * 创建一个Kafka消费者
     *
     * @param groupId 消费者组
     * @param topic   消费者订阅的话题
     * @return 消费者对象
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(String topic, String groupId) throws ClassNotFoundException {
        Properties consumerProperties = new Properties();
        // 设定一些关于新的消费者的配置信息
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Server);
        // 设定新的消费者的组名
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 设定反序列化方式
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 值的反序列化方式
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 设定信任所有类型以反序列化
        consumerProperties.put("spring.json.trusted.packages", "*");
        // 新建一个消费者
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
        // 使这个消费者订阅对应话题
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

}