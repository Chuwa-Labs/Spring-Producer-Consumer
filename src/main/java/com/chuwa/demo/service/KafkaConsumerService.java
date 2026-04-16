package com.chuwa.demo.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerService {
    // Demo 1: at most once
    // Commit the offset before the business logic runs.
    // If processing fails after acknowledgment, Kafka will not redeliver the record.
    // That prevents duplicates, but can lose messages.
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "at-most-once-demo-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAtMostOnce(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();

        System.out.println("[AT_MOST_ONCE] committed first, then processing message: "
                + record.key() + " " + record.value()
                + " from partition: " + record.partition()
                + " offset: " + record.offset());

        processMessage(record, "AT_MOST_ONCE");
    }

    // Demo 2: at least once
    // Commit the offset only after the business logic succeeds.
    // If processing fails before acknowledgment, Kafka will redeliver the record.
    // That avoids message loss, but duplicates are possible.
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "at-least-once-demo-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAtLeastOnce(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        System.out.println("[AT_LEAST_ONCE] processing first, commit after success: "
                + record.key() + " " + record.value()
                + " from partition: " + record.partition()
                + " offset: " + record.offset());

        processMessage(record, "AT_LEAST_ONCE");
        acknowledgment.acknowledge();
    }

    private void processMessage(ConsumerRecord<String, String> record, String mode) {
        if (record.value() != null && record.value().contains("FAIL")) {
            throw new IllegalStateException("Simulated failure in " + mode + " demo for message: " + record.value());
        }

        System.out.println("Finished business logic for " + mode + " message: " + record.value());
    }

}
