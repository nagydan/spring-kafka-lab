package com.example.kafka.spring.config;

import com.example.kafka.spring.handler.FailSafeHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Integer> kafkaListenerContainerFactory(ConsumerFactory<String, Integer> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Integer> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(10);
        factory.setCommonErrorHandler(new FailSafeHandler());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Integer> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    @Bean
    public Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();
        //TODO: 1 configure bootstrap server and serdes
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_api_1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

}
