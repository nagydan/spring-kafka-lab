package com.example.kafka.spring.handler;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
@Slf4j
public class FailSafeHandler implements CommonErrorHandler {

    @Override
    public void handleRemaining(@NotNull Exception thrownException, List<ConsumerRecord<?, ?>> records, @NotNull Consumer<?, ?> consumer, MessageListenerContainer container) {
        log.error("Failed to consume record {}, groupId {}.", records.stream().map(ConsumerRecord::key).collect(Collectors.toList()),
                container.getGroupId(), thrownException);
    }

    @Override
    public void handleOtherException(@NotNull Exception thrownException, @NotNull Consumer<?, ?> consumer, @NotNull MessageListenerContainer container, boolean batchListener) {
        if (thrownException instanceof RecordDeserializationException) {
            RecordDeserializationException recordException = (RecordDeserializationException) thrownException;
            log.error("Failed to parse record {}, groupId {}.", recordException.topicPartition(), container.getGroupId(), thrownException);
            consumer.seek(new TopicPartition(recordException.topicPartition().topic(), recordException.topicPartition().partition()), recordException.offset() + 1);
        } else {
            log.error("Failed to consume record groupId {}.", container.getGroupId(), thrownException);
        }
    }
}