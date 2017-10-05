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

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.RUNNING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStreamThreadTest {
    private final KStreamBuilder builder = new KStreamBuilder();
    private final MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockTime time = new MockTime();
    private final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private GlobalStreamThread globalStreamThread;
    private StreamsConfig config;

    @Before
    public void before() {
        builder.globalTable("foo", "bar");
        final HashMap<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "blah");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "blah");
        config = new StreamsConfig(properties);
        globalStreamThread = new GlobalStreamThread(builder.buildGlobalStateTopology(),
                                                    config,
                                                    mockConsumer,
                                                    new StateDirectory("appId", TestUtils.tempDirectory().getPath(), time),
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
        } catch (StreamsException e) {
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
                                                    new StateDirectory("appId", TestUtils.tempDirectory().getPath(), time),
                                                    new Metrics(),
                                                    new MockTime(),
                                                    "clientId",
                                                    stateRestoreListener);

        try {
            globalStreamThread.start();
            fail("Should have thrown StreamsException if start up failed");
        } catch (StreamsException e) {
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
    public void shouldStopRunningWhenClosedByUser() throws InterruptedException {
        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @Test
    public void shouldCloseStateStoresOnClose() throws InterruptedException {
        initializeConsumer();
        globalStreamThread.start();
        final StateStore globalStore = builder.globalStateStores().get("bar");
        assertTrue(globalStore.isOpen());
        globalStreamThread.shutdown();
        globalStreamThread.join();
        assertFalse(globalStore.isOpen());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTransitionToDeadOnClose() throws InterruptedException {

        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();

        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldStayDeadAfterTwoCloses() throws InterruptedException {

        initializeConsumer();
        globalStreamThread.start();
        globalStreamThread.shutdown();
        globalStreamThread.join();
        globalStreamThread.shutdown();

        assertEquals(GlobalStreamThread.State.DEAD, globalStreamThread.state());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTransitiontoRunningOnStart() throws InterruptedException {

        initializeConsumer();
        globalStreamThread.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return globalStreamThread.state() == RUNNING;
            }
        }, 10 * 1000, "Thread never started.");
        globalStreamThread.shutdown();
    }



    private void initializeConsumer() {
        mockConsumer.updatePartitions("foo", Collections.singletonList(new PartitionInfo("foo",
                                                                                         0,
                                                                                         null,
                                                                                         new Node[0],
                                                                                         new Node[0])));
        final TopicPartition topicPartition = new TopicPartition("foo", 0);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 0L));
    }



}