package com.example.kafka.component;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumerComponent {

    /*@KafkaListener(topics = "my-replicated-topic", groupId = "my")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment){
        JSONObject object = JSONObject.parseObject(record.value());
        System.out.println(object);
        System.out.println(record.topic());
        log.debug("接受消息");
        acknowledgment.acknowledge();
    }*/
}
