/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RebalanceConsumer;
import org.apache.kafka.clients.consumer.RebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demonstrates manual offset management with a {@link RebalanceListener} and the callback-scoped
 * {@link RebalanceConsumer}. The {@code RebalanceConsumer} passed to a callback is valid only while that callback is
 * executing and must not be retained for later use.
 *
 * <p>Expected callback behavior:
 * <ul>
 *   <li>{@code onPartitionsAssigned}: reports assigned partitions' starting positions,
 *       initialized from committed offsets or the earliest offsets.</li>
 *   <li>{@code onPartitionsRevoked}: commits the next offsets for revoked partitions
 *       before giving up ownership.</li>
 *   <li>{@code onPartitionsLost}: reports lost partitions without committing their offsets.</li>
 * </ul>
 */
public class KafkaRebalanceListenerDemo {
    private static final String TOPIC_NAME = "rebalance-listener-demo";
    private static final String GROUP_NAME = "rebalance-listener-group";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public static void main(String[] args) {
        if (args.length > 1) {
            Utils.printHelp("Usage: KafkaRebalanceListenerDemo [classic|consumer]");
            return;
        }

        String groupProtocol = args.length == 0 ? ConsumerConfig.DEFAULT_GROUP_PROTOCOL : args[0];
        String clientId = "rebalance-consumer-" + UUID.randomUUID().toString().substring(0, 8);
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProperties(clientId, groupProtocol));
        AtomicBoolean closing = new AtomicBoolean();
        Thread shutdownHook = registerShutdownHook(consumer, clientId, closing);

        try {
            consume(consumer, clientId, groupProtocol, closing);
        } finally {
            removeShutdownHook(shutdownHook, closing);
            consumer.close();
        }
    }

    private static void consume(KafkaConsumer<Integer, String> consumer,
                                String clientId,
                                String groupProtocol,
                                AtomicBoolean closing) {
        consumer.setRebalanceListener(new OffsetManagementRebalanceListener(clientId));
        consumer.subscribe(List.of(TOPIC_NAME));
        Utils.printOut("clientId=%s subscribed to %s in group %s with groupProtocol=%s",
            clientId, TOPIC_NAME, GROUP_NAME, groupProtocol);

        try {
            // Return to poll after processing so that the consumer thread observes a shutdown wakeup.
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<Integer, String> record : records) {
                    Utils.printOut("clientId=%s processed key=%s, partition=%d, offset=%d",
                        clientId, record.key(), record.partition(), record.offset());
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            Utils.printErr("clientId=%s failed to commit offsets %s: %s",
                                clientId, offsets, exception);
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            if (!closing.get()) {
                throw e;
            }
        }
    }

    private static Thread registerShutdownHook(KafkaConsumer<Integer, String> consumer,
                                               String clientId,
                                               AtomicBoolean closing) {
        Thread consumerThread = Thread.currentThread();
        Thread shutdownHook = new Thread(() -> {
            closing.set(true);
            consumer.wakeup();
            try {
                // Wait for the consumer thread to close the non-thread-safe consumer.
                consumerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, clientId + "-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        return shutdownHook;
    }

    private static void removeShutdownHook(Thread shutdownHook, AtomicBoolean closing) {
        if (!closing.get()) {
            try {
                if (!Runtime.getRuntime().removeShutdownHook(shutdownHook)) {
                    Utils.printErr("Unable to remove shutdown hook %s", shutdownHook.getName());
                }
            } catch (IllegalStateException e) {
                // Shutdown has already started, so the registered hook will stop the consumer.
            }
        }
    }

    private static Properties consumerProperties(String clientId, String groupProtocol) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private static final class OffsetManagementRebalanceListener implements RebalanceListener {
        private final String clientId;

        private OffsetManagementRebalanceListener(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            if (partitions.isEmpty()) {
                Utils.printOut("[onPartitionsRevoked] clientId=%s had no partitions to revoke", clientId);
                return;
            }

            // Each poll batch is processed synchronously, so positions reflect the next offsets to consume.
            Map<TopicPartition, OffsetAndMetadata> revokedOffsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                revokedOffsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
            }
            consumer.commitSync(revokedOffsets);
            Utils.printOut("[onPartitionsRevoked] clientId=%s committed %s before revocation", clientId, revokedOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            if (partitions.isEmpty()) {
                Utils.printOut("[onPartitionsAssigned] clientId=%s completed a rebalance with no newly assigned partitions",
                    clientId);
                return;
            }

            Map<TopicPartition, Long> positions = new HashMap<>();
            for (TopicPartition partition : partitions) {
                positions.put(partition, consumer.position(partition));
            }
            Utils.printOut("[onPartitionsAssigned] clientId=%s assigned %s at positions %s",
                clientId, partitions, positions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            // These partitions are no longer owned by this consumer, so do not perform offset management
            // operations on them, such as committing offsets, querying positions, or seeking.
            Utils.printOut("[onPartitionsLost] clientId=%s lost %s", clientId, partitions);
        }
    }
}
