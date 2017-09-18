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


import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreChangeLoggerTest {

    private final String topic = "topic";

    private final Map<Integer, String> logged = new HashMap<>();

    private final MockProcessorContext context = new MockProcessorContext(StateSerdes.withBuiltinTypes(topic, Integer.class, String.class),
            new RecordCollectorImpl(null, "StoreChangeLoggerTest", new LogContext("StoreChangeLoggerTest ")) {
                @Override
                public <K1, V1> void send(final String topic,
                                          final K1 key,
                                          final V1 value,
                                          final Integer partition,
                                          final Long timestamp,
                                          final Serializer<K1> keySerializer,
                                          final Serializer<V1> valueSerializer) {
                    logged.put((Integer) key, (String) value);
                }

                @Override
                public <K1, V1> void send(final String topic,
                                          final K1 key,
                                          final V1 value,
                                          final Long timestamp,
                                          final Serializer<K1> keySerializer,
                                          final Serializer<V1> valueSerializer,
                                          final StreamPartitioner<? super K1, ? super V1> partitioner) {
                    throw new UnsupportedOperationException();
                }
            }
    );

    private final StoreChangeLogger<Integer, String> changeLogger = new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, Integer.class, String.class));

    @After
    public void after() {
        context.close();
    }

    @Test
    public void testAddRemove() {
        context.setTime(1);
        changeLogger.logChange(0, "zero");
        changeLogger.logChange(1, "one");
        changeLogger.logChange(2, "two");

        assertEquals("zero", logged.get(0));
        assertEquals("one", logged.get(1));
        assertEquals("two", logged.get(2));

        changeLogger.logChange(0, null);
        assertNull(logged.get(0));

    }
}
