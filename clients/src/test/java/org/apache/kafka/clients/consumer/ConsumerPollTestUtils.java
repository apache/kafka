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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;

/**
 * This class provides utilities for tests to wait for a call to {@link Consumer#poll(Duration)} to produce a
 * result (error, records, specific condition, etc.). This is mostly due to the subtle difference in behavior
 * of the non-blocking {@link AsyncKafkaConsumer}. A single pass of {@link AsyncKafkaConsumer#poll(Duration)}
 * may not be sufficient to provide an immediate result.
 */
public class ConsumerPollTestUtils {

    /**
     * Wait up to {@link TestUtils#DEFAULT_MAX_WAIT_MS} to return records from the given {@link Consumer}.
     */
    public static <T> ConsumerRecords<T, T> waitForRecords(Consumer<?, ?> consumer) {
        Timer timer = Time.SYSTEM.timer(DEFAULT_MAX_WAIT_MS);

        while (timer.notExpired()) {
            @SuppressWarnings("unchecked")
            ConsumerRecords<T, T> records = (ConsumerRecords<T, T>) consumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty())
                return records;

            timer.update();
        }

        throw new TimeoutException("no records to return");
    }

    /**
     * Wait up to {@link TestUtils#DEFAULT_MAX_WAIT_MS} for the {@link Consumer} to produce the side effect
     * that causes {@link Supplier condition} to evaluate to {@code true}.
     */
    public static void waitForCondition(Consumer<?, ?> consumer,
                                        Supplier<Boolean> testCondition,
                                        String conditionDetails) {
        try {
            TestUtils.waitForCondition(
                () -> {
                    consumer.poll(Duration.ZERO);
                    return testCondition.get();
                },
                conditionDetails
            );
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Wait up to {@link TestUtils#DEFAULT_MAX_WAIT_MS} for the {@link Consumer} to throw an exception that,
     * when tested against the {@link Function condition}, will evaluate to {@code true}.
     */
    public static void waitForException(Consumer<?, ?> consumer,
                                        Function<Throwable, Boolean> testCondition,
                                        String conditionDetails) {
        try {
            TestUtils.waitForCondition(
                () -> {
                    try {
                        consumer.poll(Duration.ZERO);
                        return false;
                    } catch (Throwable t) {
                        return testCondition.apply(t);
                    }
                },
                conditionDetails
            );
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

}
