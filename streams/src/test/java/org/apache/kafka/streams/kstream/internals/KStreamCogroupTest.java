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
package org.apache.kafka.streams.kstream.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KStreamCogroupTest {

    private boolean sendOldValues = false;
    private final KTableValueGetterSupplier<String, Long> parentValueGetterSupplier = new KTableValueGetterSupplier<String, Long>() {
            @Override
            public KTableValueGetter<String, Long> get() {
                return null;
            }
    
            @Override
            public String[] storeNames() {
                return null;
            }
        };
    private final KStreamAggProcessorSupplier parent = new KStreamAggProcessorSupplier<String, String, Change<Long>, Long>() {
            @Override
            public Processor<String, Change<Long>> get() {
                return null;
            }
    
            @Override
            public KTableValueGetterSupplier<String, Long> view() {
                return parentValueGetterSupplier;
            }
    
            @Override
            public void enableSendingOldValues() {
                sendOldValues = true;
            }
        };
    private final KStreamCogroup<String, Long> cogroup = new KStreamCogroup<String, Long>(Collections.singleton(parent));
    private final Processor<String, Change<Long>> processor = cogroup.get();
    private MockProcessorContext context;
    private List<KeyValue> results = new ArrayList<>();

    @Before
    public void setup() {
        context = new MockProcessorContext(null, Serdes.String(), Serdes.Long(), new NoOpRecordCollector(), new ThreadCache(new LogContext("testCache"), 100000, new MockStreamsMetrics(new Metrics()))) {
                @Override
                public <K, V> void forward(final K key, final V value) {
                    results.add(KeyValue.pair(key, value));
                }
            };
        processor.init(context);
    }

    @After
    public void tearDown() {
        results.clear();
        sendOldValues = false;
    }

    @Test
    public void shouldEnableSendingOldValuesOfParent() {
        cogroup.enableSendingOldValues();
        assertTrue(sendOldValues);
    }

    @Test
    public void shouldReturnViewOfParent() {
        final KTableValueGetterSupplier<String, Long> valueGetterSupplier = cogroup.view();
        assertEquals(parentValueGetterSupplier, valueGetterSupplier);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPassChangeWithOldValueRemoved() {
        processor.process("key", new Change<>(1L, 0L));
        assertEquals(new KeyValue<>("key", new Change<>(1L, null)), (KeyValue<String, Change<Long>>) results.get(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPassChangeUnchanged() {
        cogroup.enableSendingOldValues();
        processor.process("key", new Change<>(1L, 0L));
        assertEquals(new KeyValue<>("key", new Change<>(1L, 0L)), (KeyValue<String, Change<Long>>) results.get(0));
    }
}
