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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsProducer;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockStandbyUpdateListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class StreamThreadStateStoreProviderTest {

    private StreamTask taskOne;
    private StreamThreadStateStoreProvider provider;
    private StateDirectory stateDirectory;
    private File stateDir;
    private final String topicName = "topic";
    @Mock
    private StreamThread threadMock;
    private Map<TaskId, Task> tasks;

    @Before
    public void before() {
        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("the-source", topicName);
        topology.addProcessor("the-processor", new MockApiProcessorSupplier<>(), "the-source");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("kv-store"),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("timestamped-kv-store"),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore(
                    "window-store",
                    Duration.ofMillis(10L),
                    Duration.ofMillis(2L),
                    false),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.inMemoryWindowStore(
                    "timestamped-window-store",
                    Duration.ofMillis(10L),
                    Duration.ofMillis(2L),
                    false),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.sessionStoreBuilder(
                Stores.inMemorySessionStore(
                    "session-store",
                    Duration.ofMillis(10L)),
                Serdes.String(),
                Serdes.String()),
            "the-processor");

        final Properties properties = new Properties();
        final String applicationId = "applicationId";
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        stateDir = TestUtils.tempDirectory();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());

        final StreamsConfig streamsConfig = new StreamsConfig(properties);
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        configureClients(clientSupplier, "applicationId-kv-store-changelog");
        configureClients(clientSupplier, "applicationId-window-store-changelog");

        final InternalTopologyBuilder internalTopologyBuilder = topology.getInternalBuilder(applicationId);
        final ProcessorTopology processorTopology = internalTopologyBuilder.buildTopology();

        tasks = new HashMap<>();
        stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true, false);

        taskOne = createStreamsTask(
            streamsConfig,
            clientSupplier,
            processorTopology,
            new TaskId(0, 0));
        taskOne.initializeIfNeeded();
        tasks.put(new TaskId(0, 0), taskOne);

        final StreamTask taskTwo = createStreamsTask(
            streamsConfig,
            clientSupplier,
            processorTopology,
            new TaskId(0, 1));
        taskTwo.initializeIfNeeded();
        tasks.put(new TaskId(0, 1), taskTwo);

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
            provider.stores(StoreQueryParameters.fromNameAndType("kv-store", QueryableStoreTypes.keyValueStore()));
        assertEquals(2, kvStores.size());
        for (final ReadOnlyKeyValueStore<String, String> store: kvStores) {
            assertThat(store, instanceOf(ReadOnlyKeyValueStore.class));
            assertThat(store, not(instanceOf(TimestampedKeyValueStore.class)));
        }
    }

    @Test
    public void shouldFindTimestampedKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> tkvStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store", QueryableStoreTypes.timestampedKeyValueStore()));
        assertEquals(2, tkvStores.size());
        for (final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store: tkvStores) {
            assertThat(store, instanceOf(ReadOnlyKeyValueStore.class));
            assertThat(store, instanceOf(TimestampedKeyValueStore.class));
        }
    }

    @Test
    public void shouldNotFindKeyValueStoresAsTimestampedStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store", QueryableStoreTypes.timestampedKeyValueStore()))
        );
        assertThat(
            exception.getMessage(),
            is(
                "Cannot get state store kv-store because the queryable store type " +
                    "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedKeyValueStoreType] " +
                    "does not accept the actual store type " +
                    "[class org.apache.kafka.streams.state.internals.MeteredKeyValueStore]."
            )
        );
    }

    @Test
    public void shouldFindTimestampedKeyValueStoresAsKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> tkvStores =
                provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store", QueryableStoreTypes.keyValueStore()));
        assertEquals(2, tkvStores.size());
        for (final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store: tkvStores) {
            assertThat(store, instanceOf(ReadOnlyKeyValueStore.class));
            assertThat(store, not(instanceOf(TimestampedKeyValueStore.class)));
        }
    }

    @Test
    public void shouldFindWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, String>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.windowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, String> store: windowStores) {
            assertThat(store, instanceOf(ReadOnlyWindowStore.class));
            assertThat(store, not(instanceOf(TimestampedWindowStore.class)));
        }
    }

    @Test
    public void shouldFindTimestampedWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store", QueryableStoreTypes.timestampedWindowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> store: windowStores) {
            assertThat(store, instanceOf(ReadOnlyWindowStore.class));
            assertThat(store, instanceOf(TimestampedWindowStore.class));
        }
    }

    @Test
    public void shouldNotFindWindowStoresAsTimestampedStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.timestampedWindowStore()))
        );
        assertThat(
            exception.getMessage(),
            is(
                "Cannot get state store window-store because the queryable store type " +
                    "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedWindowStoreType] " +
                    "does not accept the actual store type " +
                    "[class org.apache.kafka.streams.state.internals.MeteredWindowStore]."
            )
        );
    }

    @Test
    public void shouldFindTimestampedWindowStoresAsWindowStore() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store", QueryableStoreTypes.windowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> store: windowStores) {
            assertThat(store, instanceOf(ReadOnlyWindowStore.class));
            assertThat(store, not(instanceOf(TimestampedWindowStore.class)));
        }
    }

    @Test
    public void shouldFindSessionStores() {
        mockThread(true);
        final List<ReadOnlySessionStore<String, String>> sessionStores =
            provider.stores(StoreQueryParameters.fromNameAndType("session-store", QueryableStoreTypes.sessionStore()));
        assertEquals(2, sessionStores.size());
        for (final ReadOnlySessionStore<String, String> store: sessionStores) {
            assertThat(store, instanceOf(ReadOnlySessionStore.class));
        }
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfKVStoreClosed() {
        mockThread(true);
        taskOne.getStore("kv-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store",
                QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfTsKVStoreClosed() {
        mockThread(true);
        taskOne.getStore("timestamped-kv-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store",
                QueryableStoreTypes.timestampedKeyValueStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfWindowStoreClosed() {
        mockThread(true);
        taskOne.getStore("window-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("window-store",
                QueryableStoreTypes.windowStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfTsWindowStoreClosed() {
        mockThread(true);
        taskOne.getStore("timestamped-window-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store",
                QueryableStoreTypes.timestampedWindowStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfSessionStoreClosed() {
        mockThread(true);
        taskOne.getStore("session-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("session-store",
                QueryableStoreTypes.sessionStore())));
    }

    @Test
    public void shouldReturnEmptyListIfNoStoresFoundWithName() {
        mockThread(true);
        assertEquals(
            Collections.emptyList(),
            provider.stores(StoreQueryParameters.fromNameAndType("not-a-store", QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldReturnSingleStoreForPartition() {
        mockThread(true);
        {
            final List<ReadOnlyKeyValueStore<String, String>> kvStores =
                provider.stores(
                    StoreQueryParameters
                        .fromNameAndType("kv-store", QueryableStoreTypes.keyValueStore())
                        .withPartition(0));
            assertEquals(1, kvStores.size());
            for (final ReadOnlyKeyValueStore<String, String> store : kvStores) {
                assertThat(store, instanceOf(ReadOnlyKeyValueStore.class));
                assertThat(store, not(instanceOf(TimestampedKeyValueStore.class)));
            }
        }
        {
            final List<ReadOnlyKeyValueStore<String, String>> kvStores =
                provider.stores(
                    StoreQueryParameters
                        .fromNameAndType("kv-store", QueryableStoreTypes.keyValueStore())
                        .withPartition(1));
            assertEquals(1, kvStores.size());
            for (final ReadOnlyKeyValueStore<String, String> store : kvStores) {
                assertThat(store, instanceOf(ReadOnlyKeyValueStore.class));
                assertThat(store, not(instanceOf(TimestampedKeyValueStore.class)));
            }
        }
    }

    @Test
    public void shouldReturnEmptyListForInvalidPartitions() {
        mockThread(true);
        assertEquals(
                Collections.emptyList(),
                provider.stores(StoreQueryParameters.fromNameAndType("kv-store", QueryableStoreTypes.keyValueStore()).withPartition(2))
        );
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfNotAllStoresAvailable() {
        mockThread(false);
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store",
                QueryableStoreTypes.keyValueStore())));
    }

    private StreamTask createStreamsTask(final StreamsConfig streamsConfig,
                                         final MockClientSupplier clientSupplier,
                                         final ProcessorTopology topology,
                                         final TaskId taskId) {
        final Metrics metrics = new Metrics();
        final LogContext logContext = new LogContext("test-stream-task ");
        final Set<TopicPartition> partitions = Collections.singleton(new TopicPartition(topicName, taskId.partition()));
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Task.TaskType.ACTIVE,
            StreamsConfigUtils.eosEnabled(streamsConfig),
            logContext,
            stateDirectory,
            new StoreChangelogReader(
                new MockTime(),
                streamsConfig,
                logContext,
                clientSupplier.adminClient,
                clientSupplier.restoreConsumer,
                new MockStateRestoreListener(),
                new MockStandbyUpdateListener()),
            topology.storeToChangelogTopic(),
            partitions,
            false);
        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            taskId,
            new StreamsProducer(
                streamsConfig,
                "threadId",
                clientSupplier,
                new TaskId(0, 0),
                UUID.randomUUID(),
                logContext,
                Time.SYSTEM
            ),
            streamsConfig.defaultProductionExceptionHandler(),
            new MockStreamsMetrics(metrics),
            topology
        );
        final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            streamsConfig,
            stateManager,
            streamsMetrics,
            null
        );
        return new StreamTask(
            taskId,
            partitions,
            topology,
            clientSupplier.consumer,
            new TopologyConfig(null, streamsConfig, new Properties()).getTaskConfig(),
            streamsMetrics,
            stateDirectory,
            mock(ThreadCache.class),
            new MockTime(),
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
    }

    private void mockThread(final boolean initialized) {
        when(threadMock.readOnlyActiveTasks()).thenReturn(new HashSet<>(tasks.values()));
        when(threadMock.state()).thenReturn(
            initialized ? StreamThread.State.RUNNING : StreamThread.State.PARTITIONS_ASSIGNED
        );
    }

    private void configureClients(final MockClientSupplier clientSupplier, final String topic) {
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

        clientSupplier.adminClient.updateBeginningOffsets(offsets);
        clientSupplier.adminClient.updateEndOffsets(offsets);
    }
}
