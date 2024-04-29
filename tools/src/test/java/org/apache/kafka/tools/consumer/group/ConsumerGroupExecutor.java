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

package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.GroupType.CONSUMER;

class ConsumerGroupExecutor {

    private ConsumerGroupExecutor() {
    }

    static AutoCloseable buildConsumerGroup(String brokerAddress,
                                            int numberOfConsumers,
                                            String groupId,
                                            String topic,
                                            String groupProtocol,
                                            Optional<String> remoteAssignor,
                                            Map<String, Object> customConfigs,
                                            boolean syncCommit) {
        return buildConsumers(
                brokerAddress,
                numberOfConsumers,
                groupId,
                groupProtocol,
                topic,
                RangeAssignor.class.getName(),
                remoteAssignor,
                customConfigs,
                syncCommit
        );
    }

    static AutoCloseable buildClassicGroup(String brokerAddress,
                                           int numberOfConsumers,
                                           String groupId,
                                           String topic,
                                           String assignmentStrategy,
                                           Map<String, Object> customConfigs,
                                           boolean syncCommit) {
        return buildConsumers(
                brokerAddress,
                numberOfConsumers,
                groupId,
                GroupProtocol.CLASSIC.name,
                topic,
                assignmentStrategy,
                Optional.empty(),
                customConfigs,
                syncCommit
        );
    }

    private static AutoCloseable buildConsumers(
            String brokerAddress,
            int numberOfConsumers,
            String groupId,
            String groupProtocol,
            String topic,
            String assignmentStrategy,
            Optional<String> remoteAssignor,
            Map<String, Object> customConfigs,
            boolean syncCommit
    ) {
        List<Map<String, Object>> allConfigs = IntStream.range(0, numberOfConsumers)
                .mapToObj(ignored ->
                        composeConfigs(
                                brokerAddress,
                                groupId,
                                groupProtocol,
                                assignmentStrategy,
                                remoteAssignor,
                                customConfigs
                        )
                )
                .collect(Collectors.toList());

        Queue<KafkaConsumer<String, String>> kafkaConsumers = buildConsumersSafely(allConfigs);
        ExecutorService executor = Executors.newFixedThreadPool(kafkaConsumers.size());
        AtomicBoolean closed = new AtomicBoolean(false);
        final AutoCloseable closeable = () -> releaseConsumers(closed, kafkaConsumers, executor);

        try {
            while (!kafkaConsumers.isEmpty()) {
                KafkaConsumer<String, String> consumer = kafkaConsumers.poll();
                executor.execute(() -> {
                    try {
                        consumer.subscribe(singleton(topic));
                        while (!closed.get()) {
                            consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                            if (syncCommit)
                                consumer.commitSync();
                        }
                    } catch (WakeupException | InterruptException e) {
                        // OK
                        Thread.interrupted();
                    } finally {
                        consumer.close();
                    }
                });
            }

            return closeable;
        } catch (Throwable e) {
            Utils.closeQuietly(closeable, "Release Consumer");
            throw e;
        }
    }

    private static void releaseConsumers(AtomicBoolean closed, Queue<KafkaConsumer<String, String>> kafkaConsumers, ExecutorService executor) throws InterruptedException {
        closed.set(true);
        kafkaConsumers.forEach(consumer -> Utils.closeQuietly(consumer, "Release Consumer"));
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    private static Map<String, Object> composeConfigs(String brokerAddress, String groupId, String groupProtocol, String assignmentStrategy, Optional<String> remoteAssignor, Map<String, Object> customConfigs) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        configs.put(GROUP_ID_CONFIG, groupId);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(GROUP_PROTOCOL_CONFIG, groupProtocol);

        if (Objects.equals(groupProtocol, CONSUMER.toString())) {
            remoteAssignor.ifPresent(assignor -> configs.put(GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
        } else {
            configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
        }
        configs.putAll(customConfigs);

        return configs;
    }

    private static Queue<KafkaConsumer<String, String>> buildConsumersSafely(Iterable<Map<String, Object>> allConfigs/*, Queue<KafkaConsumer<String, String>> container*/) {
        Queue<KafkaConsumer<String, String>> container = new LinkedList<>();
        try {
            allConfigs.forEach(configs -> container.offer(new KafkaConsumer<>(configs)));
            return container;
        } catch (Throwable e) {
            container.forEach(consumer -> Utils.closeQuietly(consumer, "Release Consumers"));
            throw e;
        }
    }
}
