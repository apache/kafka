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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.InternalNameProvider;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.KeyValueWithTimestampStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.DEAD;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.RUNNING;
import static org.hamcrest.CoreMatchers.equalTo;
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

    private final static String GLOBAL_STORE_TOPIC_NAME = "foo";
    private final static String GLOBAL_STORE_NAME = "bar";
    private final TopicPartition topicPartition = new TopicPartition(GLOBAL_STORE_TOPIC_NAME, 0);

    @SuppressWarnings("unchecked")
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

        builder.addGlobalStore(
            new KeyValueWithTimestampStoreMaterializer<>(materialized).materialize().withLoggingDisabled(),
            "sourceName",
            null,
            null,
            null,
            GLOBAL_STORE_TOPIC_NAME,
            "processorName",
            new KTableSource<>(GLOBAL_STORE_NAME, GLOBAL_STORE_NAME));

        final HashMap<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "blah");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "blah");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config = new StreamsConfig(properties);
        globalStreamThread = new GlobalStreamThread(builder.rewriteTopology(config).buildGlobalStateTopology(),
                                                    config,
                                                    mockConsumer,
                                                    new StateDirectory(config, time, true),
                                                    0,
                                                    new Metrics(),
                                                    new MockTime(),
                                                    "clientId",
                                                     stateRestoreListener);
    }

    @Test
    public void shouldThrowStreamsExceptionOnStartupIfThereIsAStreamsException() {
        // should throw as the MockConsumer hasn't been configured and there are no
        // partitions available
        try {
            globalStreamThread.start();
            fail("Should have thrown StreamsException if start up failed");
        } catch (final StreamsException e) {
            // ok
        }
        assertFalse(globalStreamThread.stillRunning());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionOnStartupIfExceptionOccurred() {
        final MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                throw new RuntimeException("KABOOM!");
            }
        };
        globalStreamThread = new GlobalStreamThread(builder.buildGlobalStateTopology(),
                                                    config,
                                                    mockConsumer,
                                                    new StateDirectory(config, time, true),
                                                    0,
                                                    new Metrics(),
                                                    new MockTime(),
                                                    "clientId",
                                                    stateRestoreListener);

        try {
            globalStreamThread.start();
            fail("Should have thrown StreamsException if start up failed");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(RuntimeException.class));
            assertThat(e.getCause().getMessage(), equalTo("KABOOM!"));
        }
        assertFalse(globalStreamThread.stillRunning());
    }

    @Test
    public void shouldBeRunningAfterSuccessfulStart() {
        initializeConsumer();
        globalStreamThread.start();
        assertTrue(globalStreamThread.stillRunning());
    }

    @Test(timeout = 30000)
    public void shouldStopRunningWhenClosedByUser() throws Exception {
        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @Test
    public void shouldCloseStateStoresOnClose() throws Exception {
        initializeConsumer();
        globalStreamThread.start();
        final StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
        assertTrue(globalStore.isOpen());
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertFalse(globalStore.isOpen());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTransitionToDeadOnClose() throws Exception {
        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();

        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldStayDeadAfterTwoCloses() throws Exception {
        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        globalStreamThread.shutdown();

        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTransitionToRunningOnStart() throws Exception {
        initializeConsumer();
        globalStreamThread.start();

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == RUNNING,
            10 * 1000,
            "Thread never started.");

        globalStreamThread.shutdown();
    }

    @Test
    public void shouldDieOnInvalidOffsetException() throws Exception {
        initializeConsumer();
        globalStreamThread.start();

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == RUNNING,
            10 * 1000,
            "Thread never started.");

        mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 1L));
        mockConsumer.addRecord(new ConsumerRecord<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

        TestUtils.waitForCondition(
            () -> mockConsumer.position(topicPartition) == 1L,
            10 * 1000,
            "Input record never consumed");

        mockConsumer.setException(new InvalidOffsetException("Try Again!") {
            @Override
            public Set<TopicPartition> partitions() {
                return Collections.singleton(topicPartition);
            }
        });
        // feed first record for recovery
        final byte[] valueAndTimestamp = ByteBuffer.allocate(10).putLong(0L).put("V1".getBytes()).array();
        mockConsumer.addRecord(new ConsumerRecord<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), valueAndTimestamp));

        TestUtils.waitForCondition(
            () -> globalStreamThread.state() == DEAD,
            10 * 1000,
            "GlobalStreamThread should have died.");
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
}
