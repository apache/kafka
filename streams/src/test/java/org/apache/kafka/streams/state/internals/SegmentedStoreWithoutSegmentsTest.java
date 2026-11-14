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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// TopologyTestDriver#close deletes the state, so the empty directory a never-written store leaves is made by hand
public class SegmentedStoreWithoutSegmentsTest {

    private static final String APPLICATION_ID = "segmented-store-without-segments";
    private static final String STORE_NAME = "never-written";
    private static final Duration RETENTION = Duration.ofDays(2);
    private static final Duration WINDOW_SIZE = Duration.ofDays(1);

    private enum StoreType {
        WINDOW,
        TIMESTAMPED_WINDOW,
        TIMESTAMPED_WINDOW_WITH_HEADERS,
        TIME_ORDERED_WINDOW,
        SESSION,
        SESSION_WITH_HEADERS,
        TIME_ORDERED_SESSION
    }

    private static StoreBuilder<?> storeBuilder(final StoreType storeType) {
        switch (storeType) {
            case WINDOW:
                return Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIMESTAMPED_WINDOW:
                return Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIMESTAMPED_WINDOW_WITH_HEADERS:
                return Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIME_ORDERED_WINDOW:
                return Stores.timestampedWindowStoreBuilder(
                    RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(STORE_NAME, RETENTION, WINDOW_SIZE, false, true),
                    Serdes.String(),
                    Serdes.Long());
            case SESSION:
                return Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(STORE_NAME, RETENTION),
                    Serdes.String(),
                    Serdes.Long());
            case SESSION_WITH_HEADERS:
                return Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(STORE_NAME, RETENTION),
                    Serdes.String(),
                    Serdes.Long());
            case TIME_ORDERED_SESSION:
                return Stores.sessionStoreBuilder(
                    new RocksDbTimeOrderedSessionBytesStoreSupplier(STORE_NAME, RETENTION.toMillis(), true),
                    Serdes.String(),
                    Serdes.Long());
            default:
                throw new IllegalArgumentException("unknown store type: " + storeType);
        }
    }

    private static class NoOpProcessor implements Processor<String, String, Void, Void> {
        @Override
        public void process(final Record<String, String> record) {
        }
    }

    private static Topology topology(final StoreType storeType) {
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), "input");
        topology.addProcessor("processor", NoOpProcessor::new, "source");
        topology.addStateStore(storeBuilder(storeType), "processor");
        return topology;
    }

    private static Properties exactlyOnce(final File stateDir) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        return props;
    }

    // the only task of the TopologyTestDriver is 0_0
    private static File storeDir(final File stateDir) {
        return new File(stateDir, APPLICATION_ID + File.separator + "0_0" + File.separator + STORE_NAME);
    }

    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldInitializeTaskWhoseStoreHasNoSegments(final StoreType storeType) {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(storeDir(stateDir).mkdirs());

        assertDoesNotThrow(() -> new TopologyTestDriver(topology(storeType), exactlyOnce(stateDir)).close());
    }

    // a segment that holds no offset may hold uncommitted writes
    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldTreatTaskAsCorruptedWhenSegmentHasNoOffset(final StoreType storeType) {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(new File(storeDir(stateDir), STORE_NAME + ".0").mkdirs());

        assertThrows(
            TaskCorruptedException.class,
            () -> new TopologyTestDriver(topology(storeType), exactlyOnce(stateDir)).close());
    }
}
