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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A task whose segmented store was opened and never written leaves the store's empty directory behind.
 * Under exactly-once the task must not be treated as corrupted when it is initialized on that directory.
 * The directory is created by hand, because {@link TopologyTestDriver#close()} deletes the state directory.
 */
public class SegmentedStoreWithoutSegmentsTest {

    private static final String APPLICATION_ID = "segmented-store-without-segments";
    private static final String STORE_NAME = "never-written";
    private static final Duration RETENTION = Duration.ofDays(2);

    private static class NoOpProcessor implements Processor<String, String, Void, Void> {
        @Override
        public void process(final Record<String, String> record) {
        }
    }

    private static Topology topology(final StoreBuilder<?> storeBuilder) {
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), "input");
        topology.addProcessor("processor", NoOpProcessor::new, "source");
        topology.addStateStore(storeBuilder, "processor");
        return topology;
    }

    private static StoreBuilder<?> windowStore() {
        return Stores.windowStoreBuilder(
            Stores.persistentWindowStore(STORE_NAME, RETENTION, Duration.ofDays(1), false),
            Serdes.String(),
            Serdes.Long());
    }

    private static StoreBuilder<?> sessionStore() {
        return Stores.sessionStoreBuilder(
            Stores.persistentSessionStore(STORE_NAME, RETENTION),
            Serdes.String(),
            Serdes.Long());
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

    @Test
    public void shouldInitializeTaskWhoseWindowStoreHasNoSegments() {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(storeDir(stateDir).mkdirs());

        assertDoesNotThrow(() -> new TopologyTestDriver(topology(windowStore()), exactlyOnce(stateDir)).close());
    }

    @Test
    public void shouldInitializeTaskWhoseSessionStoreHasNoSegments() {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(storeDir(stateDir).mkdirs());

        assertDoesNotThrow(() -> new TopologyTestDriver(topology(sessionStore()), exactlyOnce(stateDir)).close());
    }

    // a segment that holds no offset may hold uncommitted writes
    @Test
    public void shouldTreatTaskAsCorruptedWhenSegmentHasNoOffset() {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(new File(storeDir(stateDir), STORE_NAME + ".0").mkdirs());

        assertThrows(
            TaskCorruptedException.class,
            () -> new TopologyTestDriver(topology(windowStore()), exactlyOnce(stateDir)).close());
    }
}
