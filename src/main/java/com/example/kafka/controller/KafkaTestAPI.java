package com.example.kafka.controller;

import com.example.kafka.Context.KafkaConsumerContext;
import com.example.kafka.component.KafkaDynamicConsumerFactory;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 消息测试api
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaTestAPI {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Resource
    private KafkaDynamicConsumerFactory factory;

    @GetMapping("/send")
    public String send() {
        kafkaTemplate.send("my-topic", "hello!");
        return "发送完成！";
    }

    @GetMapping("/create/{groupId}")
    public String create(@PathVariable String groupId) throws ClassNotFoundException {
        // 这里统一使用一个topic
        KafkaConsumer<String, String> consumer = factory.createConsumer("my-topic", groupId);
        KafkaConsumerContext.addConsumerTask(groupId, consumer);
        return "创建成功！";
    }

    @GetMapping("/remove/{groupId}")
    public String remove(@PathVariable String groupId) {
        KafkaConsumerContext.removeConsumerTask(groupId);
        return "移除成功！";
    }

}