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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
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
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsProducer;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StreamThreadStateStoreProviderTest {

    private StreamTask taskOne;
    private StreamThreadStateStoreProvider provider;
    private StateDirectory stateDirectory;
    private File stateDir;
    private final String topicName = "topic";
    @Mock
    private StreamThread threadMock;
    private Map<TaskId, Task> tasks;

    @BeforeEach
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
        topology.addStateStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.inMemoryKeyValueStore("timestamped-kv-store-with-headers"),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "timestamped-window-store-with-headers",
                    Duration.ofMillis(10L),
                    Duration.ofMillis(2L),
                    false),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.sessionStoreWithHeadersBuilder(
                Stores.inMemorySessionStore(
                    "session-store-with-headers",
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
        final MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>();
        final MockAdminClient mockAdminClient = MockAdminClient.create().build();
        configureClients(mockRestoreConsumer, mockAdminClient, "applicationId-kv-store-changelog");
        configureClients(mockRestoreConsumer, mockAdminClient, "applicationId-window-store-changelog");

        final InternalTopologyBuilder internalTopologyBuilder = topology.getInternalBuilder(applicationId);
        final ProcessorTopology processorTopology = internalTopologyBuilder.buildTopology();

        tasks = new HashMap<>();
        stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true, false);

        taskOne = createStreamsTask(
            streamsConfig,
            mockConsumer,
            mockProducer,
            processorTopology,
            new TaskId(0, 0));
        taskOne.initializeIfNeeded();
        tasks.put(new TaskId(0, 0), taskOne);

        final StreamTask taskTwo = createStreamsTask(
            streamsConfig,
            mockConsumer,
            mockProducer,
            processorTopology,
            new TaskId(0, 1));
        taskTwo.initializeIfNeeded();
        tasks.put(new TaskId(0, 1), taskTwo);

        provider = new StreamThreadStateStoreProvider(threadMock);

    }

    @AfterEach
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
            assertInstanceOf(ReadOnlyKeyValueStore.class, store);
            assertFalse(store instanceof TimestampedKeyValueStore);
            assertFalse(store instanceof TimestampedKeyValueStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindTimestampedKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> tkvStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store", QueryableStoreTypes.timestampedKeyValueStore()));
        assertEquals(2, tkvStores.size());
        for (final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store: tkvStores) {
            assertInstanceOf(ReadOnlyKeyValueStore.class, store);
            assertInstanceOf(TimestampedKeyValueStore.class, store);
            assertFalse(store instanceof TimestampedKeyValueStoreWithHeaders);
        }
    }

    @Test
    public void shouldNotFindKeyValueStoresAsTimestampedStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store", QueryableStoreTypes.timestampedKeyValueStore()))
        );
        assertEquals(
            "Cannot get state store kv-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedKeyValueStoreType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredKeyValueStore].",
            exception.getMessage());
    }

    @Test
    public void shouldFindTimestampedKeyValueStoresAsKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, String>> tkvStores =
                provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store", QueryableStoreTypes.keyValueStore()));
        assertEquals(2, tkvStores.size());
        for (final ReadOnlyKeyValueStore<String, String> store: tkvStores) {
            assertInstanceOf(ReadOnlyKeyValueStore.class, store);
            assertFalse(store instanceof TimestampedKeyValueStore);
            assertFalse(store instanceof TimestampedKeyValueStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, String>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.windowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, String> store: windowStores) {
            assertInstanceOf(ReadOnlyWindowStore.class, store);
            assertFalse(store instanceof TimestampedWindowStore);
            assertFalse(store instanceof TimestampedWindowStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindTimestampedWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store", QueryableStoreTypes.timestampedWindowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> store: windowStores) {
            assertInstanceOf(ReadOnlyWindowStore.class, store);
            assertInstanceOf(TimestampedWindowStore.class, store);
            assertFalse(store instanceof TimestampedWindowStoreWithHeaders);
        }
    }

    @Test
    public void shouldNotFindWindowStoresAsTimestampedStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.timestampedWindowStore()))
        );
        assertEquals(
            "Cannot get state store window-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedWindowStoreType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredWindowStore].",
            exception.getMessage());
    }

    @Test
    public void shouldFindTimestampedWindowStoresAsWindowStore() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, String>> windowStores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store", QueryableStoreTypes.windowStore()));
        assertEquals(2, windowStores.size());
        for (final ReadOnlyWindowStore<String, String> store: windowStores) {
            assertInstanceOf(ReadOnlyWindowStore.class, store);
            assertFalse(store instanceof TimestampedWindowStore);
            assertFalse(store instanceof TimestampedWindowStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindSessionStores() {
        mockThread(true);
        final List<ReadOnlySessionStore<String, String>> sessionStores =
            provider.stores(StoreQueryParameters.fromNameAndType("session-store", QueryableStoreTypes.sessionStore()));
        assertEquals(2, sessionStores.size());
        for (final ReadOnlySessionStore<String, String> store: sessionStores) {
            assertInstanceOf(ReadOnlySessionStore.class, store);
            assertFalse(store instanceof SessionStoreWithHeaders);
        }
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfKVStoreClosed() {
        mockThread(true);
        taskOne.store("kv-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store",
                QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfTsKVStoreClosed() {
        mockThread(true);
        taskOne.store("timestamped-kv-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store",
                QueryableStoreTypes.timestampedKeyValueStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfWindowStoreClosed() {
        mockThread(true);
        taskOne.store("window-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("window-store",
                QueryableStoreTypes.windowStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfTsWindowStoreClosed() {
        mockThread(true);
        taskOne.store("timestamped-window-store").close();
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store",
                QueryableStoreTypes.timestampedWindowStore())));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfSessionStoreClosed() {
        mockThread(true);
        taskOne.store("session-store").close();
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
                assertInstanceOf(ReadOnlyKeyValueStore.class, store);
                assertFalse(store instanceof TimestampedKeyValueStore);
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
                assertInstanceOf(ReadOnlyKeyValueStore.class, store);
                assertFalse(store instanceof TimestampedKeyValueStore);
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
        when(threadMock.state()).thenReturn(StreamThread.State.PARTITIONS_ASSIGNED);
        assertThrows(InvalidStateStoreException.class, () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store",
                QueryableStoreTypes.keyValueStore())));
    }

    @Test
    public void shouldFindTimestampedKeyValueStoresWithHeaders() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store-with-headers",
                QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
        assertEquals(2, stores.size());
        for (final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store : stores) {
            assertInstanceOf(ReadOnlyKeyValueStore.class, store);
            assertInstanceOf(TimestampedKeyValueStoreWithHeaders.class, store);
        }
    }

    @Test
    public void shouldFindTimestampedKeyValueStoresWithHeadersAsTimestampedKeyValueStore() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store-with-headers",
                QueryableStoreTypes.timestampedKeyValueStore()));
        assertEquals(2, stores.size());
        for (final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store : stores) {
            assertInstanceOf(GenericReadOnlyKeyValueStoreFacade.class, store);
            assertFalse(store instanceof TimestampedKeyValueStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindTimestampedKeyValueStoresWithHeadersAsKeyValueStores() {
        mockThread(true);
        final List<ReadOnlyKeyValueStore<String, String>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store-with-headers",
                QueryableStoreTypes.keyValueStore()));
        assertEquals(2, stores.size());
        for (final ReadOnlyKeyValueStore<String, String> store : stores) {
            assertInstanceOf(GenericReadOnlyKeyValueStoreFacade.class, store);
            assertFalse(store instanceof TimestampedKeyValueStore);
            assertFalse(store instanceof TimestampedKeyValueStoreWithHeaders);
        }
    }

    @Test
    public void shouldNotFindKeyValueStoresAsHeadersStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("kv-store", QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()))
        );
        assertEquals(
            "Cannot get state store kv-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedKeyValueStoreWithHeadersType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredKeyValueStore].",
            exception.getMessage());
    }

    @Test
    public void shouldNotFindTimestampedKeyValueStoresAsHeadersStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-kv-store", QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()))
        );
        assertEquals(
            "Cannot get state store timestamped-kv-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedKeyValueStoreWithHeadersType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore].",
            exception.getMessage());
    }

    @Test
    public void shouldFindTimestampedWindowStoresWithHeaders() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, ValueTimestampHeaders<String>>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store-with-headers",
                QueryableStoreTypes.timestampedWindowStoreWithHeaders()));
        assertEquals(2, stores.size());
        for (final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> store : stores) {
            assertInstanceOf(ReadOnlyWindowStore.class, store);
            assertInstanceOf(TimestampedWindowStoreWithHeaders.class, store);
        }
    }

    @Test
    public void shouldFindTimestampedWindowStoresWithHeadersAsTimestampedWindowStore() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store-with-headers",
                QueryableStoreTypes.timestampedWindowStore()));
        assertEquals(2, stores.size());
        for (final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> store : stores) {
            assertInstanceOf(GenericReadOnlyWindowStoreFacade.class, store);
            assertFalse(store instanceof TimestampedWindowStoreWithHeaders);
        }
    }

    @Test
    public void shouldFindTimestampedWindowStoresWithHeadersAsWindowStores() {
        mockThread(true);
        final List<ReadOnlyWindowStore<String, String>> stores =
            provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store-with-headers",
                QueryableStoreTypes.windowStore()));
        assertEquals(2, stores.size());
        for (final ReadOnlyWindowStore<String, String> store : stores) {
            assertInstanceOf(GenericReadOnlyWindowStoreFacade.class, store);
            assertFalse(store instanceof TimestampedWindowStore);
            assertFalse(store instanceof TimestampedWindowStoreWithHeaders);
        }
    }

    @Test
    public void shouldNotFindWindowStoresAsHeadersStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.timestampedWindowStoreWithHeaders()))
        );
        assertEquals(
            "Cannot get state store window-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedWindowStoreWithHeadersType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredWindowStore].",
            exception.getMessage());
    }

    @Test
    public void shouldNotFindTimestampedWindowStoresAsHeadersStore() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("timestamped-window-store", QueryableStoreTypes.timestampedWindowStoreWithHeaders()))
        );
        assertEquals(
            "Cannot get state store timestamped-window-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$TimestampedWindowStoreWithHeadersType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredTimestampedWindowStore].",
            exception.getMessage());
    }

    @Test
    public void shouldFindSessionStoresWithHeaders() {
        mockThread(true);
        final List<ReadOnlySessionStore<String, AggregationWithHeaders<String>>> sessionStores =
            provider.stores(StoreQueryParameters.fromNameAndType("session-store-with-headers", QueryableStoreTypes.sessionStoreWithHeaders()));
        assertEquals(2, sessionStores.size());
        for (final ReadOnlySessionStore<String, AggregationWithHeaders<String>> store: sessionStores) {
            assertInstanceOf(ReadOnlySessionStore.class, store);
            assertInstanceOf(SessionStoreWithHeaders.class, store);
        }
    }

    @Test
    public void shouldFindSessionStoresWithHeadersAsSessionStore() {
        mockThread(true);
        final List<ReadOnlySessionStore<String, String>> sessionStores =
            provider.stores(StoreQueryParameters.fromNameAndType("session-store-with-headers", QueryableStoreTypes.sessionStore()));
        assertEquals(2, sessionStores.size());
        for (final ReadOnlySessionStore<String, String> store: sessionStores) {
            assertInstanceOf(ReadOnlySessionStoreFacade.class, store);
            assertFalse(store instanceof SessionStoreWithHeaders);
        }
    }

    @Test
    public void shouldNotFindSessionStoresAsSessionStoreWithHeaders() {
        mockThread(true);
        final InvalidStateStoreException exception = assertThrows(
            InvalidStateStoreException.class,
            () -> provider.stores(StoreQueryParameters.fromNameAndType("session-store", QueryableStoreTypes.sessionStoreWithHeaders()))
        );
        assertEquals(
            "Cannot get state store session-store because the queryable store type " +
                "[class org.apache.kafka.streams.state.QueryableStoreTypes$SessionStoreWithHeadersType] " +
                "does not accept the actual store type " +
                "[class org.apache.kafka.streams.state.internals.MeteredSessionStore].",
            exception.getMessage());
    }

    private StreamTask createStreamsTask(final StreamsConfig streamsConfig,
                                         final Consumer<byte[], byte[]> consumer,
                                         final Producer<byte[], byte[]> producer,
                                         final ProcessorTopology topology,
                                         final TaskId taskId) {
        final Metrics metrics = new Metrics();
        final LogContext logContext = new LogContext("test-stream-task ");
        final Set<TopicPartition> partitions = Collections.singleton(new TopicPartition(topicName, taskId.partition()));
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Task.TaskType.ACTIVE,
            StreamsConfigUtils.eosEnabled(streamsConfig),
            false,
            logContext,
            stateDirectory,
            new MockTime(),
            topology.storeToChangelogTopic(),
            partitions);
        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            taskId,
            new StreamsProducer(
                producer,
                AT_LEAST_ONCE,
                Time.SYSTEM,
                logContext
            ),
            streamsConfig.productionExceptionHandler(),
            new MockStreamsMetrics(metrics),
            topology
        );
        final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
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
            consumer,
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

    private void configureClients(final MockConsumer<byte[], byte[]> restoreConsumer,
                                  final MockAdminClient adminClient,
                                  final String topic) {
        final List<PartitionInfo> partitions = Arrays.asList(
            new PartitionInfo(topic, 0, null, null, null),
            new PartitionInfo(topic, 1, null, null, null)
        );
        restoreConsumer.updatePartitions(topic, partitions);
        final TopicPartition tp1 = new TopicPartition(topic, 0);
        final TopicPartition tp2 = new TopicPartition(topic, 1);

        restoreConsumer.assign(Arrays.asList(tp1, tp2));

        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp1, 0L);
        offsets.put(tp2, 0L);

        restoreConsumer.updateBeginningOffsets(offsets);
        restoreConsumer.updateEndOffsets(offsets);

        adminClient.updateBeginningOffsets(offsets);
        adminClient.updateEndOffsets(offsets);
    }
}
