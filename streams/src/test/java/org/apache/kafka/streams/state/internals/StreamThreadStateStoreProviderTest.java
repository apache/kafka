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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.state.QueryableStoreTypes.windowStore;
import static org.junit.Assert.assertEquals;

public class StreamThreadStateStoreProviderTest {

    private StreamTask taskOne;
    private StreamThreadStateStoreProvider provider;
    private StateDirectory stateDirectory;
    private File stateDir;
    private final String topicName = "topic";
    private StreamThread threadMock;
    private Map<TaskId, StreamTask> tasks;

    @SuppressWarnings("deprecation")
    @Before
    public void before() {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("the-source", topicName);
        builder.addProcessor("the-processor", new MockProcessorSupplier(), "the-source");
        builder.addStateStore(Stores.create("kv-store")
            .withStringKeys()
            .withStringValues().inMemory().build(), "the-processor");

        builder.addStateStore(Stores.create("window-store")
            .withStringKeys()
            .withStringValues()
            .persistent()
            .windowed(10, 10, 2, false).build(), "the-processor");

        final Properties properties = new Properties();
        final String applicationId = "applicationId";
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        stateDir = TestUtils.tempDirectory();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());

        final StreamsConfig streamsConfig = new StreamsConfig(properties);
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        configureRestoreConsumer(clientSupplier, "applicationId-kv-store-changelog");
        configureRestoreConsumer(clientSupplier, "applicationId-window-store-changelog");

        builder.setApplicationId(applicationId);
        final ProcessorTopology topology = builder.build(null);

        tasks = new HashMap<>();
        stateDirectory = new StateDirectory(streamsConfig, new MockTime());

        taskOne = createStreamsTask(streamsConfig, clientSupplier, topology, new TaskId(0, 0));
        taskOne.initializeStateStores();
        tasks.put(new TaskId(0, 0), taskOne);

        final StreamTask taskTwo = createStreamsTask(streamsConfig, clientSupplier, topology, new TaskId(0, 1));
        taskTwo.initializeStateStores();
        tasks.put(new TaskId(0, 1), taskTwo);

        threadMock = EasyMock.createNiceMock(StreamThread.class);
        provider = new StreamThreadStateStoreProvider(threadMock);

    }

    @After
    public void cleanUp() throws IOException {
        Utils.delete(stateDir);
    }

    @Test
    public void shouldFindKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, String>> kvStores =
            provider.stores("kv-store", QueryableStoreTypes.<String, String>keyValueStore());
        assertEquals(2, kvStores.size());
    }

    @Test
    public void shouldFindWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<Object, Object>>
            windowStores =
            provider.stores("window-store", windowStore());
        assertEquals(2, windowStores.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfWindowStoreClosed() {
        mockThread(true);
        taskOne.getStore("window-store").close();
        provider.stores("window-store", QueryableStoreTypes.windowStore());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfKVStoreClosed() {
        mockThread(true);
        taskOne.getStore("kv-store").close();
        provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
    }

    @Test
    public void shouldReturnEmptyListIfNoStoresFoundWithName() {
        mockThread(true);
        assertEquals(Collections.emptyList(), provider.stores("not-a-store", QueryableStoreTypes
            .keyValueStore()));
    }


    @Test
    public void shouldReturnEmptyListIfStoreExistsButIsNotOfTypeValueStore() {
        mockThread(true);
        assertEquals(
            Collections.emptyList(),
            provider.stores("window-store", QueryableStoreTypes.keyValueStore())
        );
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfNotAllStoresAvailable() {
        mockThread(false);
        provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
    }

    private StreamTask createStreamsTask(final StreamsConfig streamsConfig,
                                         final MockClientSupplier clientSupplier,
                                         final ProcessorTopology topology,
                                         final TaskId taskId) {
        return new StreamTask(
            taskId,
            Collections.singletonList(new TopicPartition(topicName, taskId.partition)),
            topology,
            clientSupplier.consumer,
            new StoreChangelogReader(clientSupplier.restoreConsumer, new MockStateRestoreListener(), new LogContext("test-stream-task ")),
            streamsConfig,
            new MockStreamsMetrics(new Metrics()),
            stateDirectory,
            null,
            new MockTime(),
            clientSupplier.getProducer(new HashMap<String, Object>())
        ) {
            @Override
            protected void updateOffsetLimits() {}
        };
    }

    private void mockThread(final boolean initialized) {
        EasyMock.expect(threadMock.isRunningAndNotRebalancing()).andReturn(initialized);
        EasyMock.expect(threadMock.tasks()).andStubReturn(tasks);
        EasyMock.replay(threadMock);
    }

    private void configureRestoreConsumer(final MockClientSupplier clientSupplier,
                                          final String topic) {
        final List<PartitionInfo> partitions = Arrays.asList(
            new PartitionInfo(topic, 0, null, null, null),
            new PartitionInfo(topic, 1, null, null, null)
        );
        clientSupplier.restoreConsumer.updatePartitions(topic, partitions);
        final TopicPartition tp1 = new TopicPartition(topic, 0);
        final TopicPartition tp2 = new TopicPartition(topic, 1);

        clientSupplier.restoreConsumer.assign(Arrays.asList(tp1, tp2));

        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp1, 0L);
        offsets.put(tp2, 0L);

        clientSupplier.restoreConsumer.updateBeginningOffsets(offsets);
        clientSupplier.restoreConsumer.updateEndOffsets(offsets);
    }
}
