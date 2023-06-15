package com.example.kafka.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.kafka.server.KafkaProducerManager;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Value("${kafkaServer.topic}")
    private String topic;

    @Resource
    KafkaProducerManager kafkaProducerManager;

    @PostMapping("/send")
    public String send(@RequestBody JSONObject data) {
        kafkaProducerManager.sendMessageAsync(topic, data.toJSONString());
        log.info(data.toJSONString());
        return String.format("消息 %s 发送成功！", data.toJSONString());
    }
}
