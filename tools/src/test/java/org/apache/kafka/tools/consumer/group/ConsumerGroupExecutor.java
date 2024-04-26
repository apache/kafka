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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

class ConsumerGroupExecutor implements AutoCloseable {
    private final ExecutorService executor;
    private final List<ConsumerRunnable> consumers = new ArrayList<>();

    private ConsumerGroupExecutor(
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
        this.executor = Executors.newFixedThreadPool(numberOfConsumers);
        IntStream.rangeClosed(1, numberOfConsumers).forEach(i -> {
            ConsumerRunnable consumerThread = new ConsumerRunnable(
                    brokerAddress,
                    groupId,
                    groupProtocol,
                    topic,
                    assignmentStrategy,
                    remoteAssignor,
                    customConfigs,
                    syncCommit
            );
            submit(consumerThread);
        });
    }

    static ConsumerGroupExecutor buildConsumerGroup(String brokerAddress,
                                                    int numberOfConsumers,
                                                    String groupId,
                                                    String topic,
                                                    String groupProtocol,
                                                    Optional<String> remoteAssignor,
                                                    Map<String, Object> customConfigs,
                                                    boolean syncCommit) {
        return new ConsumerGroupExecutor(
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

    static ConsumerGroupExecutor buildClassicGroup(String brokerAddress,
                                                   int numberOfConsumers,
                                                   String groupId,
                                                   String topic,
                                                   String assignmentStrategy,
                                                   Map<String, Object> customConfigs,
                                                   boolean syncCommit) {
        return new ConsumerGroupExecutor(
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

    void submit(ConsumerRunnable consumerThread) {
        consumers.add(consumerThread);
        executor.execute(consumerThread);
    }

    @Override
    public void close() throws Exception {
        consumers.forEach(ConsumerRunnable::shutdown);
        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ConsumerRunnable implements Runnable {
        private final boolean syncCommit;
        private final String topic;
        private final KafkaConsumer<String, String> consumer;
        private volatile boolean isShutdown = false;

        private ConsumerRunnable(String brokerAddress,
                                 String groupId,
                                 String groupProtocol,
                                 String topic,
                                 String assignmentStrategy,
                                 Optional<String> remoteAssignor,
                                 Map<String, Object> customConfigs,
                                 boolean syncCommit) {
            this.syncCommit = syncCommit;
            this.topic = topic;

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
            consumer = new KafkaConsumer<>(configs);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(singleton(topic));
                while (!isShutdown) {
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    if (syncCommit)
                        consumer.commitSync();
                }
            } catch (WakeupException e) {
                // OK
            } finally {
                consumer.close();
            }
        }

        void shutdown() {
            isShutdown = true;
            consumer.wakeup();
        }
    }
}
