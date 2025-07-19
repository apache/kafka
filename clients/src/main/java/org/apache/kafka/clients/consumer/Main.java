package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<String, Object> producerConfigs = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfigs);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic3", "value");
            producer.send(record);
        }
        Map<String, Object> consumerConfigs = Map.of("key.deserializer", StringDeserializer.class.getName(),
                "value.deserializer", StringDeserializer.class.getName(),
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", "group.id", "group1");
        KafkaShareConsumer<String, String> shareConsumer = new KafkaShareConsumer<>(consumerConfigs);
        Consumer<String, String> simpleConsuemr = new KafkaConsumer<>(consumerConfigs);
        shareConsumer.subscribe(List.of("topic"));
        while(true) {
            shareConsumer.poll(Duration.ofMillis(5000));
            simpleConsuemr.commitAsync();
        }

    }
}
