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
package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreChangeLoggerTest {

    private final String topic = "topic";

    private final Map<Integer, ValueAndTimestamp<String>> logged = new HashMap<>();
    private final Map<Integer, Headers> loggedHeaders = new HashMap<>();

    private final InternalMockProcessorContext context = new InternalMockProcessorContext(
        StateSerdes.withBuiltinTypes(topic, Integer.class, String.class),
        new RecordCollectorImpl(
            "StoreChangeLoggerTest",
            new LogContext("StoreChangeLoggerTest "),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")) {

            @Override
            public <K1, V1> void send(final String topic,
                                      final K1 key,
                                      final V1 value,
                                      final Headers headers,
                                      final Integer partition,
                                      final Long timestamp,
                                      final Serializer<K1> keySerializer,
                                      final Serializer<V1> valueSerializer) {
                logged.put((Integer) key, ValueAndTimestamp.make((String) value, timestamp));
                loggedHeaders.put((Integer) key, headers);
            }

            @Override
            public <K1, V1> void send(final String topic,
                                      final K1 key,
                                      final V1 value,
                                      final Headers headers,
                                      final Long timestamp,
                                      final Serializer<K1> keySerializer,
                                      final Serializer<V1> valueSerializer,
                                      final StreamPartitioner<? super K1, ? super V1> partitioner) {
                throw new UnsupportedOperationException();
            }
        }
    );

    private final StoreChangeLogger<Integer, String> changeLogger =
        new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, Integer.class, String.class));

    @Test
    public void testAddRemove() {
        context.setTime(1);
        changeLogger.logChange(0, "zero");
        context.setTime(5);
        changeLogger.logChange(1, "one");
        changeLogger.logChange(2, "two");
        changeLogger.logChange(3, "three", 42L);

        assertEquals(ValueAndTimestamp.make("zero", 1L), logged.get(0));
        assertEquals(ValueAndTimestamp.make("one", 5L), logged.get(1));
        assertEquals(ValueAndTimestamp.make("two", 5L), logged.get(2));
        assertEquals(ValueAndTimestamp.make("three", 42L), logged.get(3));

        changeLogger.logChange(0, null);
        assertNull(logged.get(0));
    }

    @Test
    public void shouldNotSendRecordHeadersToChangelogTopic() {
        context.headers().add(new RecordHeader("key", "value".getBytes()));
        changeLogger.logChange(0, "zero");
        changeLogger.logChange(0, "zero", 42L);

        assertNull(loggedHeaders.get(0));
    }
}
