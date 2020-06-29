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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GlobalStateManagerImplTest {


    protected final MockTime time = new MockTime();
    protected final TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
    protected final MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    protected final String storeName1 = "t1-store";
    protected final String storeName2 = "t2-store";
    protected final String storeName3 = "t3-store";
    protected final String storeName4 = "t4-store";
    protected final String storeName5 = "t5-store";
    protected final String storeName6 = "t6-store";
    protected final TopicPartition t1 = new TopicPartition("t1", 1);
    protected final TopicPartition t2 = new TopicPartition("t2", 1);
    protected final TopicPartition t3 = new TopicPartition("t3", 1);
    protected final TopicPartition t4 = new TopicPartition("t4", 1);
    protected final TopicPartition t5 = new TopicPartition("t5", 1);
    protected final TopicPartition t6 = new TopicPartition(ProcessorStateManager.storeChangelogTopic("appId", storeName6), 1);
    protected GlobalStateManagerImpl stateManager;
    protected StateDirectory stateDirectory;
    protected StreamsConfig streamsConfig;
    protected NoOpReadOnlyStore<Object, Object> store1, store2, store3, store4;
    protected NoOpReadOnlyStore<Long, Long> store5, store6;
    protected MockConsumer<byte[], byte[]> consumer;
    protected File checkpointFile;
    protected ProcessorTopology topology;
    protected InternalMockProcessorContext processorContext;

    static ProcessorTopology withGlobalStores(final Map<String, SourceNode> sourcesByTopic,
                                              final List<StateStore> stateStores,
                                              final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.emptyList(),
            sourcesByTopic,
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
        storeToTopic.put(storeName5, t5.topic());
        storeToTopic.put(storeName6, t6.topic());

        store1 = new NoOpReadOnlyStore<>(storeName1, true);
        store2 = new ConverterStore<>(storeName2, true);
        store3 = new NoOpReadOnlyStore<>(storeName3);
        store4 = new NoOpReadOnlyStore<>(storeName4);
        store5 = new NoOpReadOnlyStore<>(storeName5, true);
        store6 = new NoOpReadOnlyStore<>(storeName6, true);

        final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();
        final MockSourceNode<Long, Long> source1 = new MockSourceNode<>(new String[]{t5.topic()}, longDeserializer,
            longDeserializer);
        final MockSourceNode<Long, Long> source2 = new MockSourceNode<>(new String[]{t6.topic()}, longDeserializer,
            longDeserializer);

        topology = withGlobalStores(mkMap(mkEntry(t5.topic(), source1), mkEntry(t6.topic(), source2)),
            asList(store1, store2, store3, store4, store5, store6),
            storeToTopic);

        streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
            }
        });
        stateDirectory = new StateDirectory(streamsConfig, time, true);
        consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        stateManager = new GlobalStateManagerImpl(
            new LogContext("test"),
            topology,
            consumer,
            stateDirectory,
            stateRestoreListener,
            streamsConfig);
        processorContext = new InternalMockProcessorContext(stateDirectory.globalStateDir(), streamsConfig);
        stateManager.setGlobalProcessorContext(processorContext);
        checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    @After
    public void after() throws IOException {
        stateDirectory.unlockGlobalState();
    }

    public byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    @Test
    public void shouldThrowStreamsExceptionWhenRestoringWithLogAndFailExceptionHandler() {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t5, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t5, 3L);
        consumer.updatePartitions(t5.topic(), Collections.singletonList(new PartitionInfo(t5.topic(), t5.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t5));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
        final byte[] specialString = "specialKey".getBytes(StandardCharsets.UTF_8);
        final byte[] longValue = longToBytes(1);

        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 1, longValue, longValue));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 2, specialString, specialString));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 3, longValue, longValue));

        stateManager.initialize();

        try {
            stateManager.register(store5, stateRestoreCallback);
            fail("Should not get here as LogAndFailExceptionHandler used");
        } catch (final StreamsException e) {
            //expected ok to ignore
        }

    }

    @Test
    public void shouldRestoreForChangelogTopics() {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t6, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t6, 3L);
        consumer.updatePartitions(t6.topic(),
            Collections.singletonList(new PartitionInfo(t6.topic(), t6.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t6));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
        final byte[] specialString = "specialKey".getBytes(StandardCharsets.UTF_8);
        final byte[] longValue = longToBytes(1);

        consumer.addRecord(new ConsumerRecord<>(t6.topic(), t6.partition(), 1, longValue, longValue));
        consumer.addRecord(new ConsumerRecord<>(t6.topic(), t6.partition(), 2, specialString, specialString));
        consumer.addRecord(new ConsumerRecord<>(t6.topic(), t6.partition(), 3, longValue, longValue));

        stateManager.initialize();
        stateManager.register(store6, stateRestoreCallback);
        assertEquals(3, stateRestoreCallback.restored.size());

    }

    @Test
    public void shouldLockGlobalStateDirectory() {
        stateManager.initialize();
        assertTrue(new File(stateDirectory.globalStateDir(), ".lock").exists());
    }

    @Test(expected = LockException.class)
    public void shouldThrowLockExceptionIfCantGetLock() throws IOException {
        final StateDirectory stateDir = new StateDirectory(streamsConfig, time, true);
        try {
            stateDir.lockGlobalState();
            stateManager.initialize();
        } finally {
            stateDir.unlockGlobalState();
        }
    }

    @Test
    public void shouldReadCheckpointOffsets() throws IOException {
        final Map<TopicPartition, Long> expected = writeCheckpoint();

        stateManager.initialize();
        final Map<TopicPartition, Long> offsets = stateManager.changelogOffsets();
        assertEquals(expected, offsets);
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

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfFailedToReadCheckpointedOffsets() throws IOException {
        writeCorruptCheckpoint();
        stateManager.initialize();
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
        assertEquals(Utils.mkSet(storeName1, storeName2, storeName3, storeName4, storeName5, storeName6), storeNames);
    }

    @Test
    public void shouldThrowIllegalArgumentIfTryingToRegisterStoreThatIsNotGlobal() {
        stateManager.initialize();

        try {
            stateManager.registerStore(new NoOpReadOnlyStore<>("not-in-topology"), stateRestoreCallback);
            fail("should have raised an illegal argument exception as store is not in the topology");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAttemptingToRegisterStoreTwice() {
        stateManager.initialize();
        initializeConsumer(2, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback);
        try {
            stateManager.registerStore(store1, stateRestoreCallback);
            fail("should have raised an illegal argument exception as store has already been registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowStreamsExceptionIfNoPartitionsFoundForStore() {
        stateManager.initialize();
        try {
            stateManager.registerStore(store1, stateRestoreCallback);
            fail("Should have raised a StreamsException as there are no partition for the store");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldNotConvertValuesIfStoreDoesNotImplementTimestampedBytesStore() {
        initializeConsumer(1, 0, t1);

        stateManager.initialize();
        stateManager.registerStore(store1, stateRestoreCallback);

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
            stateRestoreCallback
        );

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(5, restoredRecord.value.length);
    }

    @Test
    public void shouldConvertValuesIfStoreImplementsTimestampedBytesStore() {
        initializeConsumer(1, 0, t2);

        stateManager.initialize();
        stateManager.registerStore(store2, stateRestoreCallback);

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
            stateRestoreCallback
        );

        final KeyValue<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.get(0);
        assertEquals(3, restoredRecord.key.length);
        assertEquals(13, restoredRecord.value.length);
    }

    @Test
    public void shouldRestoreRecordsUpToHighwatermark() {
        initializeConsumer(2, 0, t1);

        stateManager.initialize();

        stateManager.registerStore(store1, stateRestoreCallback);
        assertEquals(2, stateRestoreCallback.restored.size());
    }

    @Test
    public void shouldRecoverFromInvalidOffsetExceptionAndRestoreRecords() {
        initializeConsumer(2, 0, t1);
        consumer.setPollException(new InvalidOffsetException("Try Again!") {
            public Set<TopicPartition> partitions() {
                return Collections.singleton(t1);
            }
        });

        stateManager.initialize();

        stateManager.registerStore(store1, stateRestoreCallback);
        assertEquals(2, stateRestoreCallback.restored.size());
    }

    @Test
    public void shouldListenForRestoreEvents() {
        initializeConsumer(5, 1, t1);
        stateManager.initialize();

        stateManager.registerStore(store1, stateRestoreCallback);

        assertThat(stateRestoreListener.restoreStartOffset, equalTo(1L));
        assertThat(stateRestoreListener.restoreEndOffset, equalTo(6L));
        assertThat(stateRestoreListener.totalNumRestored, equalTo(5L));


        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_START), equalTo(store1.name()));
        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_BATCH), equalTo(store1.name()));
        assertThat(stateRestoreListener.storeNameCalledStates.get(RESTORE_END), equalTo(store1.name()));
    }

    @Test
    public void shouldRestoreRecordsFromCheckpointToHighwatermark() throws IOException {
        initializeConsumer(5, 5, t1);

        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
        offsetCheckpoint.write(Collections.singletonMap(t1, 5L));

        stateManager.initialize();
        stateManager.registerStore(store1, stateRestoreCallback);
        assertEquals(5, stateRestoreCallback.restored.size());
    }


    @Test
    public void shouldFlushStateStores() {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback);
        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback);

        stateManager.flush();
        assertTrue(store1.flushed);
        assertTrue(store2.flushed);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateStoreExceptionIfStoreFlushFailed() {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore(store1.name()) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        }, stateRestoreCallback);

        stateManager.flush();
    }

    @Test
    public void shouldCloseStateStores() throws IOException {
        stateManager.initialize();
        // register the stores
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback);
        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback);

        stateManager.close();
        assertFalse(store1.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateStoreExceptionIfStoreCloseFailed() throws IOException {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore(store1.name()) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        }, stateRestoreCallback);

        stateManager.close();
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallbackIsNull() {
        stateManager.initialize();
        try {
            stateManager.registerStore(store1, null);
            fail("should have thrown due to null callback");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldUnlockGlobalStateDirectoryOnClose() throws IOException {
        stateManager.initialize();
        stateManager.close();
        final StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
        try {
            // should be able to get the lock now as it should've been released in close
            assertTrue(stateDir.lockGlobalState());
        } finally {
            stateDir.unlockGlobalState();
        }
    }

    @Test
    public void shouldNotCloseStoresIfCloseAlreadyCalled() throws IOException {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        stateManager.registerStore(new NoOpReadOnlyStore("t1-store") {
            @Override
            public void close() {
                if (!isOpen()) {
                    throw new RuntimeException("store already closed");
                }
                super.close();
            }
        }, stateRestoreCallback);
        stateManager.close();

        stateManager.close();
    }

    @Test
    public void shouldAttemptToCloseAllStoresEvenWhenSomeException() throws IOException {
        stateManager.initialize();
        initializeConsumer(1, 0, t1);
        final NoOpReadOnlyStore store = new NoOpReadOnlyStore("t1-store") {
            @Override
            public void close() {
                super.close();
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.registerStore(store, stateRestoreCallback);

        initializeConsumer(1, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback);

        try {
            stateManager.close();
        } catch (final ProcessorStateException e) {
            // expected
        }
        assertFalse(store.isOpen());
        assertFalse(store2.isOpen());
    }

    @Test
    public void shouldReleaseLockIfExceptionWhenLoadingCheckpoints() throws IOException {
        writeCorruptCheckpoint();
        try {
            stateManager.initialize();
        } catch (final StreamsException e) {
            // expected
        }
        final StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
        try {
            // should be able to get the lock now as it should've been released
            assertTrue(stateDir.lockGlobalState());
        } finally {
            stateDir.unlockGlobalState();
        }
    }

    @Test
    public void shouldCheckpointOffsets() throws IOException {
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(t1, 25L);
        stateManager.initialize();

        stateManager.checkpoint(offsets);

        final Map<TopicPartition, Long> result = readOffsetsCheckpoint();
        assertThat(result, equalTo(offsets));
        assertThat(stateManager.changelogOffsets(), equalTo(offsets));
    }

    @Test
    public void shouldNotRemoveOffsetsOfUnUpdatedTablesDuringCheckpoint() {
        stateManager.initialize();
        initializeConsumer(10, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback);
        initializeConsumer(20, 0, t2);
        stateManager.registerStore(store2, stateRestoreCallback);

        final Map<TopicPartition, Long> initialCheckpoint = stateManager.changelogOffsets();
        stateManager.checkpoint(Collections.singletonMap(t1, 101L));

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
        stateManager.registerStore(store1, stateRestoreCallback);
        final KeyValue<byte[], byte[]> restoredKv = stateRestoreCallback.restored.get(0);
        assertThat(stateRestoreCallback.restored, equalTo(Collections.singletonList(KeyValue.pair(restoredKv.key, restoredKv.value))));
    }

    @Test
    public void shouldCheckpointRestoredOffsetsToFile() throws IOException {
        stateManager.initialize();
        initializeConsumer(10, 0, t1);
        stateManager.registerStore(store1, stateRestoreCallback);
        stateManager.checkpoint(Collections.emptyMap());
        stateManager.close();

        final Map<TopicPartition, Long> checkpointMap = stateManager.changelogOffsets();
        assertThat(checkpointMap, equalTo(Collections.singletonMap(t1, 10L)));
        assertThat(readOffsetsCheckpoint(), equalTo(checkpointMap));
    }

    @Test
    public void shouldSkipGlobalInMemoryStoreOffsetsToFile() throws IOException {
        stateManager.initialize();
        initializeConsumer(10, 0, t3);
        stateManager.registerStore(store3, stateRestoreCallback);
        stateManager.close();

        assertThat(readOffsetsCheckpoint(), equalTo(Collections.emptyMap()));
    }

    private Map<TopicPartition, Long> readOffsetsCheckpoint() throws IOException {
        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
        return offsetCheckpoint.read();
    }

    @Test
    public void shouldThrowLockExceptionIfIOExceptionCaughtWhenTryingToLockStateDir() {
        stateManager = new GlobalStateManagerImpl(
            new LogContext("mock"),
            topology,
            consumer,
            new StateDirectory(streamsConfig, time, true) {
                @Override
                public boolean lockGlobalState() throws IOException {
                    throw new IOException("KABOOM!");
                }
            },
            stateRestoreListener,
            streamsConfig
        );

        try {
            stateManager.initialize();
            fail("Should have thrown LockException");
        } catch (final LockException e) {
            // pass
        }
    }

    @Test
    public void shouldRetryWhenEndOffsetsThrowsTimeoutException() {
        final int retries = 2;
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<org.apache.kafka.common.TopicPartition> partitions) {
                numberOfCalls.incrementAndGet();
                throw new TimeoutException();
            }
        };
        streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
                put(StreamsConfig.RETRIES_CONFIG, retries);
            }
        });

        try {
            new GlobalStateManagerImpl(
                new LogContext("mock"),
                topology,
                consumer,
                stateDirectory,
                stateRestoreListener,
                streamsConfig);
        } catch (final StreamsException expected) {
            assertEquals(numberOfCalls.get(), retries);
        }
    }

    @Test
    public void shouldRetryWhenPartitionsForThrowsTimeoutException() {
        final int retries = 2;
        final AtomicInteger numberOfCalls = new AtomicInteger(0);
        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(final String topic) {
                numberOfCalls.incrementAndGet();
                throw new TimeoutException();
            }
        };
        streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
                put(StreamsConfig.RETRIES_CONFIG, retries);
            }
        });

        try {
            new GlobalStateManagerImpl(
                new LogContext("mock"),
                topology,
                consumer,
                stateDirectory,
                stateRestoreListener,
                streamsConfig);
        } catch (final StreamsException expected) {
            assertEquals(numberOfCalls.get(), retries);
        }
    }

    private void writeCorruptCheckpoint() throws IOException {
        final File checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
        try (final OutputStream stream = Files.newOutputStream(checkpointFile.toPath())) {
            stream.write("0\n1\nfoo".getBytes());
        }
    }

    private void initializeConsumer(final long numRecords, final long startOffset, final TopicPartition topicPartition) {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(topicPartition, startOffset);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, startOffset + numRecords);
        consumer.updatePartitions(topicPartition.topic(), Collections.singletonList(new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);

        for (int i = 0; i < numRecords; i++) {
            consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), startOffset + i, "key".getBytes(), "value".getBytes()));
        }
    }

    private Map<TopicPartition, Long> writeCheckpoint() throws IOException {
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
        final Map<TopicPartition, Long> expected = Collections.singletonMap(t1, 1L);
        checkpoint.write(expected);
        return expected;
    }

    static class TheStateRestoreCallback implements StateRestoreCallback {
        final List<KeyValue<byte[], byte[]>> restored = new ArrayList<>();

        @Override
        public void restore(final byte[] key, final byte[] value) {
            restored.add(KeyValue.pair(key, value));
        }
    }

    protected class ConverterStore<K, V> extends NoOpReadOnlyStore<K, V> implements TimestampedBytesStore {
        ConverterStore(final String name,
                       final boolean rocksdbStore) {
            super(name, rocksdbStore);
        }
    }

}
