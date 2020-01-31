package kafka.examples;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singleton;

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
        if (args.length < 1) {
            throw new IllegalArgumentException("Should accept at least one parameter");
        } else if (args[0].equals("standaloneMode") && args.length != 2) {
            throw new IllegalArgumentException("Should have specified partition in standalone mode");
        }

        KafkaProducer<Integer, String> producer = new Producer(KafkaProperties.TOPIC, true, TRANSACTIONAL_ID).get();
        // Init transactions call should always happen first in order to clear zombie transactions.
        producer.initTransactions();

        KafkaConsumer<Integer, String> consumer = new Consumer(KafkaProperties.TOPIC, READ_COMMITTED).get();
        // Under group mode, topic based subscription is sufficient as Consumers are safe to work transactionally after 2.5.
        // Under standalone mode, user needs to manually assign the topic partitions and make sure the assignment is unique
        // across the consumer group.
        if (args[0].equals("groupMode")) {
            consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Get partition assignment " + partitions);
                }
            });
        } else {
            consumer.assign(Collections.singletonList(new TopicPartition(KafkaProperties.TOPIC,
                Integer.valueOf(args[1]))));
        }
        long messageRemaining = messagesRemaining(consumer);

        while (messageRemaining > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
            if (records.count() > 0) {
                try {
                    // Begin a new transaction session.
                    producer.beginTransaction();
                    for (ConsumerRecord<Integer, String> record : records) {
                        ProducerRecord<Integer, String> customizedRecord = transform(record);
                        // Send records to the downstream.
                        producer.send(customizedRecord);
                    }
                    Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
                    for (TopicPartition topicPartition : consumer.assignment()) {
                        positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
                    }
                    // Checkpoint the progress by sending offsets to group coordinator broker.
                    producer.sendOffsetsToTransaction(positions, consumer.groupMetadata());
                    // Finish the transaction.
                    producer.commitTransaction();
                } catch (CommitFailedException e) {
                    producer.abortTransaction();
                } catch (ProducerFencedException | FencedInstanceIdException e) {
                    throw new KafkaException("Encountered fatal error during processing: " + e.getMessage());
                }
            }
            messageRemaining = messagesRemaining(consumer);
            System.out.println("Message remaining: " + messageRemaining);
        }
    }

    private static ProducerRecord<Integer, String> transform(ConsumerRecord<Integer, String> record) {
        // Customized business logic here
        return new ProducerRecord<>(OUTPUT_TOPIC, record.key() / 2, record.value());
    }

    private static long messagesRemaining(KafkaConsumer<Integer, String> consumer) {
        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(singleton(partition));
            if (endOffsets.containsKey(partition)) {
                return endOffsets.get(partition) - currentPosition;
            }
            return 0;
        }).sum();
    }
}
