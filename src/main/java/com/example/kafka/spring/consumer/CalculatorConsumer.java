package com.example.kafka.spring.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CalculatorConsumer {
//TODO: make listen() to a listener method

    public void listen(@Payload ConsumerRecord<String, Integer> message, Acknowledgment acknowledgment) {
        if (message.value() == 10) {
            throw new SerializationException("Cannot serialize");
        }
        log.info("-- Consumed message key = " + message.key() +
            ", value = " + message.value() +
            ", topic = " + message.topic() +
            ", partition = " + message.partition() +
            ", offset = " + message.offset());
        acknowledgment.acknowledge();
    }
}
