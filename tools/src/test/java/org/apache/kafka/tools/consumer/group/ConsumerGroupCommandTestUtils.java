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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * This class provides methods to build and manage consumer instances.
 */
class ConsumerGroupCommandTestUtils {

    private ConsumerGroupCommandTestUtils() {
    }

    static <T> AutoCloseable buildConsumers(int numberOfConsumers,
                                            Set<TopicPartition> partitions,
                                            Supplier<KafkaConsumer<T, T>> consumerSupplier) {
        return buildConsumers(numberOfConsumers, false, consumerSupplier,
                consumer -> consumer.assign(partitions));
    }

    static <T> AutoCloseable buildConsumers(int numberOfConsumers,
                                            boolean syncCommit,
                                            String topic,
                                            Supplier<KafkaConsumer<T, T>> consumerSupplier) {
        return buildConsumers(numberOfConsumers, syncCommit, consumerSupplier,
                consumer -> consumer.subscribe(Set.of(topic)));
    }

    static <T> AutoCloseable buildConsumers(
        int numberOfConsumers,
        boolean syncCommit,
        Supplier<KafkaConsumer<T, T>> consumerSupplier,
        Consumer<KafkaConsumer<T, T>> setPartitions
    ) {
        List<KafkaConsumer<T, T>> consumers = new ArrayList<>(numberOfConsumers);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfConsumers);
        AtomicBoolean closed = new AtomicBoolean(false);
        final AutoCloseable closeable = () -> releaseConsumers(closed, consumers, executor);
        try {
            for (int i = 0; i < numberOfConsumers; i++) {
                KafkaConsumer<T, T> consumer = consumerSupplier.get();
                consumers.add(consumer);
                executor.execute(() -> initConsumer(syncCommit, () -> {
                    setPartitions.accept(consumer);
                    return consumer;
                }, closed));
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

    private static <T> void initConsumer(boolean syncCommit,
                                         Supplier<KafkaConsumer<T, T>> consumerSupplier,
                                         AtomicBoolean closed) {
        try (KafkaConsumer<T, T> kafkaConsumer = consumerSupplier.get()) {
            while (!closed.get()) {
                kafkaConsumer.poll(Duration.ofMillis(1000));
                if (syncCommit)
                    kafkaConsumer.commitSync();
            }
        } catch (WakeupException e) {
            // OK
        }
    }
}
