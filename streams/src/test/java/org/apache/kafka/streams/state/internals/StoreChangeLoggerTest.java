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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StoreChangeLoggerTest {

    private final String topic = "topic";

    private final Map<Integer, String> logged = new HashMap<>();
    private final Map<Integer, String> written = new HashMap<>();

    private final ProcessorContext context = new MockProcessorContext(StateSerdes.withBuiltinTypes(topic, Integer.class, String.class),
            new RecordCollector(null) {
                @SuppressWarnings("unchecked")
                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                    logged.put((Integer) record.key(), (String) record.value());
                }

                @Override
                public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer,
                                          StreamPartitioner<K1, V1> partitioner) {
                    // ignore partitioner
                    send(record, keySerializer, valueSerializer);
                }
            }
    );

    private final StoreChangeLogger<Integer, String> changeLogger = new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, Integer.class, String.class), 3, 3);

    private final StoreChangeLogger.ValueGetter<Integer, String> getter = new StoreChangeLogger.ValueGetter<Integer, String>() {
        @Override
        public String get(Integer key) {
            return written.get(key);
        }
    };

    @Test
    public void testAddRemove() {
        written.put(0, "zero");
        changeLogger.add(0);
        written.put(1, "one");
        changeLogger.add(1);
        written.put(2, "two");
        changeLogger.add(2);
        assertEquals(3, changeLogger.numDirty());
        assertEquals(0, changeLogger.numRemoved());

        changeLogger.delete(0);
        changeLogger.delete(1);
        written.put(3, "three");
        changeLogger.add(3);
        assertEquals(2, changeLogger.numDirty());
        assertEquals(2, changeLogger.numRemoved());

        written.put(0, "zero-again");
        changeLogger.add(0);
        assertEquals(3, changeLogger.numDirty());
        assertEquals(1, changeLogger.numRemoved());

        written.put(4, "four");
        changeLogger.add(4);
        changeLogger.maybeLogChange(getter);
        assertEquals(0, changeLogger.numDirty());
        assertEquals(0, changeLogger.numRemoved());
        assertEquals(5, logged.size());
        assertEquals("zero-again", logged.get(0));
        assertEquals(null, logged.get(1));
        assertEquals("two", logged.get(2));
        assertEquals("three", logged.get(3));
        assertEquals("four", logged.get(4));
    }
}
