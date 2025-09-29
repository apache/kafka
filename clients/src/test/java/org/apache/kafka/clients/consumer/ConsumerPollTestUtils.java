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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConsumerPollTestUtils {

    @SuppressWarnings("unchecked")
    public static <T> ConsumerRecords<T, T> waitForRecords(Consumer<?, ?> consumer, Time time) {
        Timer timer = time.timer(15000);

        while (timer.notExpired()) {
            ConsumerRecords<T, T> records = (ConsumerRecords<T, T>) consumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty())
                return records;
        }

        throw new org.apache.kafka.common.errors.TimeoutException("no records to return");
    }

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

    public static void waitForException(Consumer<?, ?> consumer,
                                        Function<KafkaException, Boolean> testCondition,
                                        String conditionDetails) {
        try {
            TestUtils.waitForCondition(
                () -> {
                    try {
                        consumer.poll(Duration.ZERO);
                        return false;
                    } catch (KafkaException e) {
                        return testCondition.apply(e);
                    }
                },
                conditionDetails
            );
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

}
