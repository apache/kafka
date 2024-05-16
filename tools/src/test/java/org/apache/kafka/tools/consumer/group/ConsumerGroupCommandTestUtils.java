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

import kafka.test.ClusterConfig;
import kafka.test.ClusterGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static kafka.test.annotation.Type.CO_KRAFT;
import static kafka.test.annotation.Type.KRAFT;
import static kafka.test.annotation.Type.ZK;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;

class ConsumerGroupCommandTestUtils {

    private ConsumerGroupCommandTestUtils() {
    }

    static void generator(ClusterGenerator clusterGenerator) {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put(OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        serverProperties.put(OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        serverProperties.put(NEW_GROUP_COORDINATOR_ENABLE_CONFIG, "false");

        ClusterConfig classicGroupCoordinator = ClusterConfig.defaultBuilder()
                .setTypes(Stream.of(ZK, KRAFT, CO_KRAFT).collect(Collectors.toSet()))
                .setServerProperties(serverProperties)
                .setTags(Collections.singletonList("classicGroupCoordinator"))
                .build();
        clusterGenerator.accept(classicGroupCoordinator);

        // Following are test case config with new group coordinator
        serverProperties.put(NEW_GROUP_COORDINATOR_ENABLE_CONFIG, "true");

        ClusterConfig consumerGroupCoordinator = ClusterConfig.defaultBuilder()
                .setTypes(Stream.of(KRAFT, CO_KRAFT).collect(Collectors.toSet()))
                .setServerProperties(serverProperties)
                .setTags(Collections.singletonList("newGroupCoordinator"))
                .build();
        clusterGenerator.accept(consumerGroupCoordinator);
    }

    static <T> AutoCloseable buildConsumers(int numberOfConsumers,
                                            boolean syncCommit,
                                            String topic,
                                            Supplier<KafkaConsumer<T, T>> consumerSupplier) {
        List<KafkaConsumer<T, T>> consumers = new ArrayList<>(numberOfConsumers);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfConsumers);
        AtomicBoolean closed = new AtomicBoolean(false);
        final AutoCloseable closeable = () -> releaseConsumers(closed, consumers, executor);
        try {
            for (int i = 0; i < numberOfConsumers; i++) {
                KafkaConsumer<T, T> consumer = consumerSupplier.get();
                consumers.add(consumer);
                executor.execute(() -> initConsumer(topic, syncCommit, consumer, closed));
            }
            return closeable;
        } catch (Throwable e) {
            Utils.closeQuietly(closeable, "Release Consumer");
            throw e;
        }
    }

    private static <T> void releaseConsumers(AtomicBoolean closed, List<KafkaConsumer<T, T>> consumers, ExecutorService executor) throws InterruptedException {
        closed.set(true);
        consumers.forEach(KafkaConsumer::wakeup);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    private static <T> void initConsumer(String topic, boolean syncCommit, KafkaConsumer<T, T> consumer, AtomicBoolean closed) {
        try (KafkaConsumer<T, T> kafkaConsumer = consumer) {
            kafkaConsumer.subscribe(singleton(topic));
            while (!closed.get()) {
                consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                if (syncCommit)
                    consumer.commitSync();
            }
        } catch (WakeupException e) {
            // OK
        }
    }
}
