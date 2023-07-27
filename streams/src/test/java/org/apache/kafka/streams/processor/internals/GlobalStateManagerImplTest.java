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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStateManagerImplTest {


    private final MockTime time = new MockTime();
    private final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
    private final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final String storeName1 = "t1-store";
    private final String storeName2 = "t2-store";
    private final String storeName3 = "t3-store";
    private final String storeName4 = "t4-store";
    private final TopicPartition t1 = new TopicPartition("t1", 1);
    private final TopicPartition t2 = new TopicPartition("t2", 1);
    private final TopicPartition t3 = new TopicPartition("t3", 1);
    private final TopicPartition t4 = new TopicPartition("t4", 1);
    private GlobalStateManagerImpl stateManager;
    private StateDirectory stateDirectory;
    private StreamsConfig streamsConfig;
    private NoOpReadOnlyStore<Object, Object> store1, store2, store3, store4;
    private MockConsumer<byte[], byte[]> consumer;
    private File checkpointFile;
    private ProcessorTopology topology;
    private InternalMockProcessorContext processorContext;

    static ProcessorTopology withGlobalStores(final List<StateStore> stateStores,
                                              final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     stateStores,
                                     storeToChangelogTopic,
                                     Collections.emptySet());
    }

    @Before
    public void before() {
        final Map<String, String> storeToTopic = new HashMap<>();

        storeToTopic.put(storeName1, t1.topic());
        storeToTopic.put(storeName2, t2.topic());
        storeToTopic.put(storeName3, t3.topic());
        storeToTopic.put(storeName4, t4.topic());

        store1 = new NoOpReadOnlyStore<>(storeName1, true);
        store2 = new ConverterStore<>(storeName2, true);
        store3 = new NoOpReadOnlyStore<>(storeName3);
        store4 = new NoOpReadOnlyStore<>(storeName4);

        topology = withGlobalStores(asList(store1, store2, store3, store4), storeToTopic);

        streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
            }
        });
        stateDirectory = new StateDirectory(streamsConfig, time, true, false);
        consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        stateManager = new GlobalStateManagerImpl(
            new LogContext("test"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext = new InternalMockProcessorContext(stateDirectory.globalStateDir(), streamsConfig);
        stateManager.setGlobalProcessorContext(processorContext);
        checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    @Test
    public void shouldReadCheckpointOffsets() throws IOException {
        final Map<TopicPartition, Long> expected = writeCheckpoint();

        stateManager.initialize();
        final Map<TopicPartition, Long> offsets = stateManager.changelogOffsets();
        assertEquals(expected, offsets);
    }

    @Test
    public void shouldLogWarningMessageWhenIOExceptionInCheckPoint() throws IOException {
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(t1, 25L);
        stateManager.initialize();
        stateManager.updateChangelogOffsets(offsets);

        // set readonly to the CHECKPOINT_FILE_NAME.tmp file because we will write data to the .tmp file first
        // and then swap to CHECKPOINT_FILE_NAME by replacing it
        final File file = new File(stateDirectory.globalStateDir(), StateManagerUtil.CHECKPOINT_FILE_NAME + ".tmp");
        Files.createFile(file.toPath());
        file.setWritable(false);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(GlobalStateManagerImpl.class)) {
            stateManager.checkpoint();
            assertThat(appender.getMessages(), hasItem(containsString(
                "Failed to write offset checkpoint file to " + checkpointFile.getPath() + " for global stores")));
        }
    }

    @Test
    public void shouldThrowStreamsExceptionForOldTopicPartitions() throws IOException {
        final HashMap<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 1L);
        expectedOffsets.put(t2, 1L);
        expectedOffsets.put(t3, 1L);
        expectedOffsets.put(t4, 1L);

        // add an old topic (a topic not associated with any global state store)
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>(expectedOffsets);
        final TopicPartition tOld = new TopicPartition("oldTopic", 1);
        startOffsets.put(tOld, 1L);

        // start with a checkpoint file will all topic-partitions: expected and old (not
        // associated with any global state store).
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
        checkpoint.write(startOffsets);

        // initialize will throw exception
        final StreamsException e = assertThrows(StreamsException.class, () -> stateManager.initialize());
        assertThat(e.getMessage(), equalTo("Encountered a topic-partition not associated with any global state store"));
    }

    @Test
    public void shouldNotDeleteCheckpointFileAfterLoaded() throws IOException {
        writeCheckpoint();
        stateManager.initialize();
        assertTrue(checkpointFile.exists());
    }

    @Test
    public void shouldThrowStreamsExceptionIfFailedToReadCheckpointedOffsets() throws IOException {
        writeCorruptCheckpoint();
        assertThrows(StreamsException.class, stateManager::initialize);
    }

    @Test
    public void shouldInitializeStateStores() {
        stateManager.initialize();
        assertTrue(store1.initialized);
        assertTrue(store2.initialized);
    }

    @Test
    public void shouldReturnInitializedStoreNames() {
        final Set<String> storeNames = stateManager.initialize();
        assertEquals(Utils.mkSet(storeName1, storeName2, storeName3, storeName4), storeNames);
    }

    @Test
    public void shouldThrowIllegalArgumentIfTryingToRegisterStoreThatIsNotGlobal() {
        stateManager.initialize();

        try {
            stateManager.registerStore(new NoOpReadOnlyStore<>("not-in-topology"), stateRestoreCallback, null);
            fail("should have raised an illegal argument exception as store is not in the topology");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAttemptingToRegisterStoreTwice() {
        stateManager.initialize();
        initializeConsumer(2, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback, null);
        try {
            stateManager.registerStore(store1, stateRestoreCallback, null);
            fail("should have raised an illegal argument exception as store has already been registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowStreamsExceptionIfNoPartitionsFoundForStore() {
        stateManager.initialize();
        try {
            stateManager.registerStore(store1, stateRestoreCallback, null);
            fail("Should have raised a StreamsException as there are no partition for the store");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldNotConvertValuesIfStoreDoesNotImplementTimestampedBytesStore() {
        initializeConsumer(1, 0, t1);

        stateManager.initialize();
        stateManager.registerStore(store1, stateRestoreCallback, null);

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(5, restoredRecord.value.length);
    }

    @Test
    public void shouldNotConvertValuesIfInnerStoreDoesNotImplementTimestampedBytesStore() {
        initializeConsumer(1, 0, t1);

        stateManager.initialize();
        stateManager.registerStore(
            new WrappedStateStore<NoOpReadOnlyStore<Object, Object>, Object, Object>(store1) {
            },
            stateRestoreCallback,
                null);

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(5, restoredRecord.value.length);
    }

    @Test
    public void shouldConvertValuesIfStoreImplementsTimestampedBytesStore() {
        initializeConsumer(1, 0, t2);

        stateManager.initialize();
        stateManager.registerStore(store2, stateRestoreCallback, null);

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(13, restoredRecord.value.length);
    }

    @Test
    public void shouldConvertValuesIfInnerStoreImplementsTimestampedBytesStore() {
        initializeConsumer(1, 0, t2);

        stateManager.initialize();
        stateManager.registerStore(
            new WrappedStateStore<NoOpReadOnlyStore<Object, Object>, Object, Object>(store2) {
            },
            stateRestoreCallback,
            null);

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(13, restoredRecord.value.length);
    }

    @Test
    public void shouldRestoreRecordsUpToHighwatermark() {
        initializeConsumer(2, 0, t1);

        stateManager.initialize();

        stateManager.registerStore(store1, stateRestoreCallback, null);
        assertEquals(2, stateRestoreCallback.restored.size());
    }

    @Test
    public void shouldListenForRestoreEvents() {
        initializeConsumer(5, 1, t1);
        stateManager.initialize();

        stateManager.registerStore(store1, stateRestoreCallback, null);

        assertThat(stateRestoreListener.restoreStartOffset, equalTo(1L));
        assertThat(stateRestoreListener.restoreEndOffset, equalTo(6L));
        assertThat(stateRestoreListener.totalNumRestored, equalTo(5L));


        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_START), equalTo(store1.name()));
        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_BATCH), equalTo(store1.name()));
        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_END), equalTo(store1.name()));
    }

    @Test
    public void shouldRestoreRecordsFromCheckpointToHighWatermark() throws IOException {
        initializeConsumer(5, 5, t1);

        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
        offsetCheckpoint.write(Collections.singletonMap(t1, 5L));

        stateManager.initialize();
        stateManager.registerStore(store1, stateRestoreCallback, null);
        assertEquals(5, stateRestoreCallback.restored.size());
    }


    @Test
    public void shouldFlushStateStores() {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback, null);
        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback, null);

        stateManager.flush();
        assertTrue(store1.flushed);
        assertTrue(store2.flushed);
    }

    @Test
    public void shouldThrowProcessorStateStoreExceptionIfStoreFlushFailed() {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore<Object, Object>(store1.name()) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        }, stateRestoreCallback, null);
        assertThrows(StreamsException.class, stateManager::flush);
    }

    @Test
    public void shouldCloseStateStores() throws IOException {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback, null);
        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback, null);

        stateManager.close();
        assertFalse(store1.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test
    public void shouldThrowProcessorStateStoreExceptionIfStoreCloseFailed() {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore<Object, Object>(store1.name()) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        }, stateRestoreCallback, null);

        assertThrows(ProcessorStateException.class, stateManager::close);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallbackIsNull() {
        stateManager.initialize();
        try {
            stateManager.registerStore(store1, null, null);
            fail("should have thrown due to null callback");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldNotCloseStoresIfCloseAlreadyCalled() {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore<Object, Object>("t1-store") {
            @Override
            public void close() {
                if (!isOpen()) {
                    throw new RuntimeException("store already closed");
                }
                super.close();
            }
        }, stateRestoreCallback, null);
        stateManager.close();

        stateManager.close();
    }

    @Test
    public void shouldAttemptToCloseAllStoresEvenWhenSomeException() {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        final NoOpReadOnlyStore<Object, Object> store = new NoOpReadOnlyStore<Object, Object>("t1-store") {
            @Override
            public void close() {
                super.close();
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.registerStore(store, stateRestoreCallback, null);

        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback, null);

        try {
            stateManager.close();
        } catch (final ProcessorStateException e) {
            // expected
        }
        assertFalse(store.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test
    public void shouldCheckpointOffsets() throws IOException {
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(t1, 25L);
        stateManager.initialize();

        stateManager.updateChangelogOffsets(offsets);
        stateManager.checkpoint();

        final Map<TopicPartition, Long> result = readOffsetsCheckpoint();
        assertThat(result, equalTo(offsets));
        assertThat(stateManager.changelogOffsets(), equalTo(offsets));
    }

    @Test
    public void shouldNotRemoveOffsetsOfUnUpdatedTablesDuringCheckpoint() {
        stateManager.initialize();
        initializeConsumer(10, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback, null);
        initializeConsumer(20, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback, null);

        final Map<TopicPartition, Long> initialCheckpoint = stateManager.changelogOffsets();
        stateManager.updateChangelogOffsets(Collections.singletonMap(t1, 101L));
        stateManager.checkpoint();

        final Map<TopicPartition, Long> updatedCheckpoint = stateManager.changelogOffsets();
        assertThat(updatedCheckpoint.get(t2), equalTo(initialCheckpoint.get(t2)));
        assertThat(updatedCheckpoint.get(t1), equalTo(101L));
    }

    @Test
    public void shouldSkipNullKeysWhenRestoring() {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t1, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t1, 3L);
        consumer.updatePartitions(t1.topic(), Collections.singletonList(new PartitionInfo(t1.topic(), t1.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t1));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
        consumer.addRecord(new ConsumerRecord<>(t1.topic(), t1.partition(), 1, null, "null".getBytes()));
        final byte[] expectedKey = "key".getBytes();
        final byte[] expectedValue = "value".getBytes();
        consumer.addRecord(new ConsumerRecord<>(t1.topic(), t1.partition(), 2, expectedKey, expectedValue));

        stateManager.initialize();
        stateManager.registerStore(store1, stateRestoreCallback, null);
        final KeyValue<byte[], byte[]> restoredKv = stateRestoreCallback.restored.get(0);
        assertThat(stateRestoreCallback.restored, equalTo(Collections.singletonList(KeyValue.pair(restoredKv.key, restoredKv.value))));
    }

    @Test
    public void shouldCheckpointRestoredOffsetsToFile() throws IOException {
        stateManager.initialize();
        initializeConsumer(10, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback, null);
        stateManager.checkpoint();
        stateManager.close();

        final Map<TopicPartition, Long> checkpointMap = stateManager.changelogOffsets();
        assertThat(checkpointMap, equalTo(Collections.singletonMap(t1, 10L)));
        assertThat(readOffsetsCheckpoint(), equalTo(checkpointMap));
    }

    @Test
    public void shouldSkipGlobalInMemoryStoreOffsetsToFile() throws IOException {
        stateManager.initialize();
        initializeConsumer(10, 0, t3);
        stateManager.registerStore(store3, stateRestoreCallback, null);
        stateManager.close();

        assertThat(readOffsetsCheckpoint(), equalTo(Collections.emptyMap()));
    }

    private Map<TopicPartition, Long> readOffsetsCheckpoint() throws IOException {
        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
        return offsetCheckpoint.read();
    }

    @Test
    public void shouldNotRetryWhenEndOffsetsThrowsTimeoutExceptionAndTaskTimeoutIsZero() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 0L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> stateManager.initialize()
        );
        final Throwable cause = expected.getCause();
        assertThat(cause, instanceOf(TimeoutException.class));
        assertThat(cause.getMessage(), equalTo("KABOOM!"));

        assertEquals(numberOfCalls.get(), 1);
    }

    @Test
    public void shouldRetryAtLeastOnceWhenEndOffsetsThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 100 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 2);
    }

    @Test
    public void shouldRetryWhenEndOffsetsThrowsTimeoutExceptionUntilTaskTimeoutExpired() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1000L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 1000 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 11);
    }

    @Test
    public void shouldNotFailOnSlowProgressWhenEndOffsetsThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                time.sleep(1L);
                if (numberOfCalls.incrementAndGet() % 3 == 0) {
                    return super.endOffsets(partitions);
                }
                throw new TimeoutException("KABOOM!");
            }

            @Override
            public synchronized long position(final TopicPartition partition) {
                return numberOfCalls.incrementAndGet();
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 10L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        stateManager.initialize();
    }

    @Test
    public void shouldNotRetryWhenPartitionsForThrowsTimeoutExceptionAndTaskTimeoutIsZero() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 0L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> stateManager.initialize()
        );
        final Throwable cause = expected.getCause();
        assertThat(cause, instanceOf(TimeoutException.class));
        assertThat(cause.getMessage(), equalTo("KABOOM!"));

        assertEquals(numberOfCalls.get(), 1);
    }

    @Test
    public void shouldRetryAtLeastOnceWhenPartitionsForThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 100 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 2);
    }

    @Test
    public void shouldRetryWhenPartitionsForThrowsTimeoutExceptionUntilTaskTimeoutExpires() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1000L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 1000 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 11);
    }

    @Test
    public void shouldNotFailOnSlowProgressWhenPartitionForThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                time.sleep(1L);
                if (numberOfCalls.incrementAndGet() % 3 == 0) {
                    return super.partitionsFor(topic);
                }
                throw new TimeoutException("KABOOM!");
            }

            @Override
            public synchronized long position(final TopicPartition partition) {
                return numberOfCalls.incrementAndGet();
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 10L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        stateManager.initialize();
    }

    @Test
    public void shouldNotRetryWhenPositionThrowsTimeoutExceptionAndTaskTimeoutIsZero() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized long position(final TopicPartition partition) {
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 0L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> stateManager.initialize()
        );
        final Throwable cause = expected.getCause();
        assertThat(cause, instanceOf(TimeoutException.class));
        assertThat(cause.getMessage(), equalTo("KABOOM!"));

        assertEquals(numberOfCalls.get(), 1);
    }

    @Test
    public void shouldRetryAtLeastOnceWhenPositionThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized long position(final TopicPartition partition) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 100 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 2);
    }

    @Test
    public void shouldRetryWhenPositionThrowsTimeoutExceptionUntilTaskTimeoutExpired() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized long position(final TopicPartition partition) {
                time.sleep(100L);
                numberOfCalls.incrementAndGet();
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 1000L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final TimeoutException expected = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(expected.getMessage(), equalTo("Global task did not make progress to restore state within 1000 ms. Adjust `task.timeout.ms` if needed."));

        assertEquals(numberOfCalls.get(), 11);
    }

    @Test
    public void shouldNotFailOnSlowProgressWhenPositionThrowsTimeoutException() {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized long position(final TopicPartition partition) {
                time.sleep(1L);
                if (numberOfCalls.incrementAndGet() % 3 == 0) {
                    return numberOfCalls.incrementAndGet();
                }
                throw new TimeoutException("KABOOM!");
            }
        };
        initializeConsumer(0, 0, t1, t2, t3, t4);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 10L)
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        stateManager.initialize();
    }

    @Test
    public void shouldUsePollMsPlusRequestTimeoutInPollDuringRestoreAndTimeoutWhenNoProgressDuringRestore() {
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
                time.sleep(timeout.toMillis());
                return super.poll(timeout);
            }
        };

        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t1, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t1, 3L);
        consumer.updatePartitions(t1.topic(), Collections.singletonList(new PartitionInfo(t1.topic(), t1.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t1));
        consumer.updateBeginningOffsets(startOffsets);
        consumer.updateEndOffsets(endOffsets);

        streamsConfig = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));

        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            time,
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig
        );
        processorContext.setStateManger(stateManager);
        stateManager.setGlobalProcessorContext(processorContext);

        final long startTime = time.milliseconds();

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> stateManager.initialize()
        );
        assertThat(
            exception.getMessage(),
            equalTo("Global task did not make progress to restore state within 301000 ms. Adjust `task.timeout.ms` if needed.")
        );
        assertThat(time.milliseconds() - startTime, equalTo(331_100L));
    }

    private void writeCorruptCheckpoint() throws IOException {
        final File checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
        try (final OutputStream stream = Files.newOutputStream(checkpointFile.toPath())) {
            stream.write("0\n1\nfoo".getBytes());
        }
    }

    private void initializeConsumer(final long numRecords, final long startOffset, final TopicPartition... topicPartitions) {
        consumer.assign(Arrays.asList(topicPartitions));

        final Map<TopicPartition, Long> startOffsets = new HashMap<>();
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            startOffsets.put(topicPartition, startOffset);
            endOffsets.put(topicPartition, startOffset + numRecords);
            consumer.updatePartitions(topicPartition.topic(), Collections.singletonList(new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, null, null)));
            for (int i = 0; i < numRecords; i++) {
                consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), startOffset + i, "key".getBytes(), "value".getBytes()));
            }
        }
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
    }

    private Map<TopicPartition, Long> writeCheckpoint() throws IOException {
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
        final Map<TopicPartition, Long> expected = Collections.singletonMap(t1, 1L);
        checkpoint.write(expected);
        return expected;
    }

    private static class TheStateRestoreCallback implements StateRestoreCallback {
        private final List<KeyValue<byte[], byte[]>> restored = new ArrayList<>();

        @Override
        public void restore(final byte[] key, final byte[] value) {
            restored.add(KeyValue.pair(key, value));
        }
    }

    private static class ConverterStore<K, V> extends NoOpReadOnlyStore<K, V> implements TimestampedBytesStore {
        ConverterStore(final String name,
                       final boolean rocksdbStore) {
            super(name, rocksdbStore);
        }
    }

}
