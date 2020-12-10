package com.wave.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;

import java.lang.reflect.Type;

public class Converter implements RecordMessageConverter {
    @Override
    public Message<?> toMessage(ConsumerRecord<?, ?> consumerRecord, Acknowledgment acknowledgment, Consumer<?, ?> consumer, Type type) {
        return null;
    }

    @Override
    public ProducerRecord<?, ?> fromMessage(Message<?> message, String s) {
        return null;
    }
}
