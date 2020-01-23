package kafka.examples;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A demo class for how to write a customized EOS app. It takes a consume-process-produce loop
 * The things to pay attention to beyond a general consumer + producer app are:
 * 1. Define a unique transactional.id for your app
 * 2. Turn on read_committed isolation level
 */
public class KafkaExactlyOnceDemo {

    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String TRANSACTIONAL_ID = "transactional-id-" + UUID.randomUUID();
    private static final boolean READ_COMMITTED = true;

    public static void main(String[] args) {
        KafkaProducer<Integer, String> producer = new Producer(KafkaProperties.TOPIC, true, TRANSACTIONAL_ID).get();
        producer.initTransactions();

        KafkaConsumer<Integer, String> consumer = new Consumer(KafkaProperties.TOPIC, READ_COMMITTED).get();
        consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
            if (records.count() > 0) {
                try {
                    producer.beginTransaction();
                    for (ConsumerRecord<Integer, String> record : records) {
                        ConsumerRecord<Integer, String> customizedRecord = transform(record);
                        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, customizedRecord.key(), customizedRecord.value()));
                    }

                    Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
                    for (TopicPartition topicPartition : consumer.assignment()) {
                        positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
                    }
                    producer.sendOffsetsToTransaction(positions, consumer.groupMetadata());
                    producer.commitTransaction();
                } catch (CommitFailedException e) {
                    producer.abortTransaction();
                } catch (ProducerFencedException | FencedInstanceIdException e) {
                    throw new KafkaException("Encountered fatal error during processing: " + e.getMessage());
                }
            }
        }
    }

    private static ConsumerRecord<Integer, String> transform(ConsumerRecord<Integer, String> record) {
        // Customized business logic here
        return record;
    }
}
