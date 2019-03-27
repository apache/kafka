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

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeWindowedTableSourceTest {

    private final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
    private final Serde<String> strSerde = Serdes.String();

    private File stateDir = null;

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

//    @Test
//    @SuppressWarnings("unchecked")
//    public void testWindowKTableSource() {
//        final StreamsBuilder builder = new StreamsBuilder();
//        final String topic = "topic";
//        final String stateName = "state";
//        final Long windowSizeMs = 100L;
//
//        KStream<Windowed<String>, String> stream = builder.stream(topic, Consumed.with(windowedSerde, strSerde));
//
//        WindowBytesStoreSupplier supplier = Stores.persistentWindowStore(stateName, 2 * windowSizeMs, 2, windowSizeMs, false);
//        StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.
//                windowStoreBuilder(supplier, strSerde, strSerde).
//                withLoggingDisabled();
//        builder.addStateStore(storeBuilder);
//
//        stream.process(new TimeWindowedTableSource<>(stateName), stateName);
//
//        driver.setUp(builder, stateDir);
//        ProcessorContext context = driver.context();
//        ReadOnlyWindowStore<String, String> store =  (ReadOnlyWindowStore<String, String>) context.getStateStore(stateName);
//
//        // Window [0, 100]
//        driver.process(topic, createWindow("A", 0, 100), "1");
//        driver.flushState();
//
//        driver.process(topic, createWindow("B", 0, 100), "2");
//        driver.flushState();
//
//        driver.process(topic, createWindow("C", 0, 100), "3");
//        driver.flushState();
//
//        driver.process(topic, createWindow("D", 0, 100), "4");
//        driver.flushState();
//
//        // Window [100, 200]
//        driver.process(topic, createWindow("A", 100, 200), "2");
//        driver.flushState();
//
//        driver.process(topic, createWindow("A", 100, 200), "3");
//        driver.flushState();
//
//        driver.process(topic, createWindow("B", 100, 200), "2");
//        driver.flushState();
//
//        driver.process(topic, createWindow("D", 100, 200), "4");
//        driver.flushState();
//
//        driver.process(topic, createWindow("B", 100, 200), "6");
//        driver.flushState();
//
//        driver.process(topic, createWindow("C", 100, 200), "5");
//        driver.flushState();
//
//        // Window [200, 300]
//        driver.process(topic, createWindow("A", 200, 300), "4");
//        driver.flushState();
//
//        driver.process(topic, createWindow("B", 200, 300), "2");
//        driver.flushState();
//
//        driver.process(topic, createWindow("D", 200, 300), "4");
//        driver.flushState();
//
//        driver.process(topic, createWindow("B", 200, 300), "2");
//        driver.flushState();
//
//        driver.process(topic, createWindow("C", 200, 300), "3");
//        driver.flushState();
//
//        List<KeyValue<Long, String>> expectedForA = Arrays.asList(new KeyValue<>(0L, "1"), new KeyValue<>(100L, "3"), new KeyValue<>(200L, "4"));
//        fetchAndValidate(store, "A", 0, 200, expectedForA);
//
//        List<KeyValue<Long, String>> expectedForB = Arrays.asList(new KeyValue<>(0L, "2"), new KeyValue<>(100L, "6"), new KeyValue<>(200L, "2"));
//        fetchAndValidate(store, "B", 0, 200, expectedForB);
//
//        List<KeyValue<Long, String>> expectedForC = Arrays.asList(new KeyValue<>(0L, "3"), new KeyValue<>(100L, "5"), new KeyValue<>(200L, "3"));
//        fetchAndValidate(store, "C", 0, 200, expectedForC);
//
//        List<KeyValue<Long, String>> expectedForD = Arrays.asList(new KeyValue<>(0L, "4"), new KeyValue<>(100L, "4"), new KeyValue<>(200L, "4"));
//        fetchAndValidate(store, "D", 0, 200, expectedForD);
//    }

    private Windowed<String> createWindow(final String key, final long start, final long end) {
        Window window = new TimeWindow(start, end);
        return new Windowed<>(key, window);
    }

    private void fetchAndValidate(ReadOnlyWindowStore<String, String> store, final String key, final long start, final long end, final List<KeyValue<Long, String>> expected) {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        WindowStoreIterator<String> iterator = store.fetch(key, start, end);
        while (iterator.hasNext()) {
            KeyValue<Long, String> kv = iterator.next();
            result.add(kv);
        }
        iterator.close();
        assertEquals(expected, result);
    }
}
