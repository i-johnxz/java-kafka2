package com.example.demo.controller;

import com.example.demo.domain.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SendMessageController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private KafkaTemplate<String, Message> kafkaTemplate;

    @GetMapping("send/{message}")
    public void send(@PathVariable String message) {
        //this.kafkaTemplate.send("TestJava", new Message("mrbird", message));
        ListenableFuture<SendResult<String, Message>> future = this.kafkaTemplate.send("TestJava", new Message("mrbird", message));
        future.addCallback(new ListenableFutureCallback<SendResult<String, Message>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("消息：{} 发送失败，原因：{}", message, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Message> stringStringSendResult) {
                logger.info("成功发送消息：{}，offset=[{}]", message, stringStringSendResult.getRecordMetadata().offset());
            }
        });
    }
}
