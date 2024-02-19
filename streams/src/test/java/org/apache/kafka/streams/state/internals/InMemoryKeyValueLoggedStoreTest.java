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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InMemoryKeyValueLoggedStoreTest extends AbstractKeyValueStoreTest {

    @Mock
    private StreamsMetricsImpl mockStreamsMetrics;

    @Override
    protected KeyValueStoreTestDriver<Integer, String> createKeyValueStoreTestDriver() {
        mockStreamsMetrics = Mockito.mock(StreamsMetricsImpl.class);
        final Sensor mockSensor = mock(Sensor.class);
        when(mockStreamsMetrics.taskLevelSensor(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(Sensor.RecordingLevel.class), Mockito.any(Sensor[].class))).thenReturn(mockSensor);

        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class, mockStreamsMetrics);
        return driver;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("my-store"),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde())
            .withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);

        return store;
    }
}
