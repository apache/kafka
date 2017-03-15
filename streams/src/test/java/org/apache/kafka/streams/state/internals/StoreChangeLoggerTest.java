/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state.internals;


import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreChangeLoggerTest {

    private final String topic = "topic";

    private final Map<Integer, String> logged = new HashMap<>();
    private final Map<Integer, String> written = new HashMap<>();

    private final MockProcessorContext context = new MockProcessorContext(StateSerdes.withBuiltinTypes(topic, Integer.class, String.class),
            new RecordCollectorImpl(null, "StoreChangeLoggerTest") {
                @SuppressWarnings("unchecked")
                @Override
                public <K1, V1> void send(final String topic,
                                          K1 key,
                                          V1 value,
                                          Integer partition,
                                          Long timestamp,
                                          Serializer<K1> keySerializer,
                                          Serializer<V1> valueSerializer) {
                    logged.put((Integer) key, (String) value);
                }

                @Override
                public <K1, V1> void send(final String topic,
                                           K1 key,
                                           V1 value,
                                           Integer partition,
                                           Long timestamp,
                                           Serializer<K1> keySerializer,
                                           Serializer<V1> valueSerializer,
                                           StreamPartitioner<? super K1, ? super V1> partitioner) {
                    // ignore partitioner
                    send(topic, key, value, partition, timestamp, keySerializer, valueSerializer);
                }
            }
    );

    private final StoreChangeLogger<Integer, String> changeLogger = new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, Integer.class, String.class));


    @SuppressWarnings("unchecked")
    @Test
    public void testAddRemove() throws Exception {
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
