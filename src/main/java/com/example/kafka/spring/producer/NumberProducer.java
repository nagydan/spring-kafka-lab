package com.example.kafka.spring.producer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;
@Slf4j
@Component
@RequiredArgsConstructor
public class NumberProducer {

    private final KafkaTemplate kafkaTemplate;

    @Value("${kafka.topic.logging-requests}")
    private String TOPIC;

    @PostConstruct
    public void postConstruct() {
        System.out.println("Producer is created.");
    }


    public void sendNumbers(int number) {
        for (int i = 1; i < number; i++) {
            //TODO: create the record and send it to Kafka Topic then enable the future resp handling as below
            //TODO: if number == 10 throw new SerializationException to check how common error handling works
//            completableFuture.handle((res, ex) -> {
//                if (ex != null) {
//                    log.info("Error during publishing message: " + ex.getMessage());
//                }
//                var record = res.getProducerRecord();
//                var metadata = res.getRecordMetadata();
//                log.info("Published message key = " + record.key() +
//                    ", value = " + record.value() +
//                    ", topic = " + metadata.topic() +
//                    ", partition = " + metadata.partition() +
//                    ", offset = " + metadata.offset());
//                return res;
//            });
        }
    }






    private boolean isPrime(int n) {
        if (n<= 1) {
            return false;
        }
        for (int i = 2; i< n; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

}
