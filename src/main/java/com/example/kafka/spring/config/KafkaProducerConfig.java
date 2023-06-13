package com.example.kafka.spring.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    //TODO: add Producer Factory and Kafka Template

    @Bean
    public Map<String, Object> producerProperties() {
        Map<String, Object> props = new HashMap<>();
        //TODO: 1 configure bootstrap server and serdes
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }


}
