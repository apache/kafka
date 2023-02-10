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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.InternalNameProvider;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.TimestampedKeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.DEAD;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.RUNNING;
import static org.apache.kafka.streams.processor.internals.testutil.ConsumerRecordUtil.record;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStreamThreadTest {
    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private final MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
    private final MockTime time = new MockTime();
    private final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private GlobalStreamThread globalStreamThread;
    private StreamsConfig config;
    private String baseDirectoryName;

    private final static String GLOBAL_STORE_TOPIC_NAME = "foo";
    private final static String GLOBAL_STORE_NAME = "bar";
    private final TopicPartition topicPartition = new TopicPartition(GLOBAL_STORE_TOPIC_NAME, 0);

    @Before
    public void before() {
        final MaterializedInternal<Object, Object, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.with(null, null),
                new InternalNameProvider() {
                    @Override
                    public String newProcessorName(final String prefix) {
                        return "processorName";
                    }

                    @Override
                    public String newStoreName(final String prefix) {
                        return GLOBAL_STORE_NAME;
                    }
                },
                "store-"
            );

        final ProcessorSupplier<Object, Object, Void, Void> processorSupplier = () ->
            new ContextualProcessor<Object, Object, Void, Void>() {
                @Override
                public void process(final Record<Object, Object> record) {
                }
            };

        builder.addGlobalStore(
            new TimestampedKeyValueStoreMaterializer<>(materialized).materialize().withLoggingDisabled(),
            "sourceName",
            null,
            null,
            null,
            GLOBAL_STORE_TOPIC_NAME,
            "processorName",
            processorSupplier);

        baseDirectoryName = TestUtils.tempDirectory().getAbsolutePath();
        final HashMap<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "blah");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "testAppId");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, baseDirectoryName);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        config = new StreamsConfig(properties);
        globalStreamThread = new GlobalStreamThread(
            builder.rewriteTopology(config).buildGlobalStateTopology(),
            config,
            mockConsumer,
            new StateDirectory(config, time, true, false),
            0,
            new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST, time),
            time,
            "clientId",
            stateRestoreListener,
            e -> { }
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnStartupIfThereIsAStreamsException() throws Exception {
        // should throw as the MockConsumer hasn't been configured and there are no
        // partitions available
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        try {
            globalStreamThread.start();
            fail("Should have thrown StreamsException if start up failed");
        } catch (final StreamsException e) {
            // ok
        }
        globalStreamThread.join();
        assertThat(globalStore.isOpen(), is(false));
        assertFalse(globalStreamThread.stillRunning());
    }

    @Test
    public void shouldThrowStreamsExceptionOnStartupIfExceptionOccurred() throws Exception {
        final MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                throw new RuntimeException("KABOOM!");
            }
        };
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        globalStreamThread = new GlobalStreamThread(
            builder.buildGlobalStateTopology(),
            config,
            mockConsumer,
            new StateDirectory(config, time, true, false),
            0,
            new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST, time),
            time,
            "clientId",
            stateRestoreListener,
            e -> { }
        );

        try {
            globalStreamThread.start();
            fail("Should have thrown StreamsException if start up failed");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(RuntimeException.class));
            assertThat(e.getCause().getMessage(), equalTo("KABOOM!"));
        }
        globalStreamThread.join();
        assertThat(globalStore.isOpen(), is(false));
        assertFalse(globalStreamThread.stillRunning());
    }

    @Test
    public void shouldBeRunningAfterSuccessfulStart() throws Exception {
        initializeConsumer();
        startAndSwallowError();
        assertTrue(globalStreamThread.stillRunning());

        globalStreamThread.shutdown();
        globalStreamThread.join();
    }

    @Test(timeout = 30000)
    public void shouldStopRunningWhenClosedByUser() throws Exception {
        initializeConsumer();
        startAndSwallowError();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @Test
    public void shouldCloseStateStoresOnClose() throws Exception {
        initializeConsumer();
        startAndSwallowError();
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        assertTrue(globalStore.isOpen());
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertFalse(globalStore.isOpen());
    }

    @Test
    public void shouldStayDeadAfterTwoCloses() throws Exception {
        initializeConsumer();
        startAndSwallowError();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        globalStreamThread.shutdown();

        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @Test
    public void shouldTransitionToRunningOnStart() throws Exception {
        initializeConsumer();
        startAndSwallowError();

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == RUNNING,
            10 * 1000,
            "Thread never started.");

        globalStreamThread.shutdown();
    }

    @Test
    public void shouldDieOnInvalidOffsetExceptionDuringStartup() throws Exception {
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        initializeConsumer();
        mockConsumer.setPollException(new InvalidOffsetException("Try Again!") {
            @Override
            public Set<TopicPartition> partitions() {
                return Collections.singleton(topicPartition);
            }
        });

        startAndSwallowError();

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == DEAD,
            10 * 1000,
            "GlobalStreamThread should have died."
        );
        globalStreamThread.join();

        assertThat(globalStore.isOpen(), is(false));
        assertFalse(new File(baseDirectoryName + File.separator + "testAppId" + File.separator + "global").exists());
    }

    @Test
    public void shouldDieOnInvalidOffsetExceptionWhileRunning() throws Exception {
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        initializeConsumer();
        startAndSwallowError();

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == RUNNING,
            10 * 1000,
            "Thread never started.");

        mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 1L));
        mockConsumer.addRecord(record(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

        TestUtils.waitForCondition(
            () -> mockConsumer.position(topicPartition) == 1L,
            10 * 1000,
            "Input record never consumed");

        mockConsumer.setPollException(new InvalidOffsetException("Try Again!") {
            @Override
            public Set<TopicPartition> partitions() {
                return Collections.singleton(topicPartition);
            }
        });

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == DEAD,
            10 * 1000,
            "GlobalStreamThread should have died."
        );
        globalStreamThread.join();

        assertThat(globalStore.isOpen(), is(false));
        assertFalse(new File(baseDirectoryName + File.separator + "testAppId" + File.separator + "global").exists());
    }

    private void initializeConsumer() {
        mockConsumer.updatePartitions(
            GLOBAL_STORE_TOPIC_NAME,
            Collections.singletonList(new PartitionInfo(
                GLOBAL_STORE_TOPIC_NAME,
                0,
                null,
                new Node[0],
                new Node[0])));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 0L));
        mockConsumer.assign(Collections.singleton(topicPartition));
    }

    private void startAndSwallowError() {
        try {
            globalStreamThread.start();
        } catch (final IllegalStateException ignored) {
        }
    }
}
