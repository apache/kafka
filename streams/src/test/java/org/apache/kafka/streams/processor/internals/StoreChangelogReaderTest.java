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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

// TODO K9113: fix tests
@RunWith(EasyMockRunner.class)
public class StoreChangelogReaderTest {

    @Mock(type = MockType.NICE)
    private ProcessorStateManager activeStateManager;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager standbyStateManager;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadata;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadataOne;
    @Mock(type = MockType.NICE)
    private StateStoreMetadata storeMetadataTwo;
    @Mock(type = MockType.NICE)
    private StateStore store;

    private final String storeName = "store";
    private final String topicName = "topic";
    private final LogContext logContext = new LogContext("test-reader ");
    private final TopicPartition tp = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition("one", 0);
    private final TopicPartition tp2 = new TopicPartition("two", 0);
    private final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader"));
    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

    @Before
    public void setUp() {
        EasyMock.expect(activeStateManager.storeMetadata(tp)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(activeStateManager.taskType()).andReturn(AbstractTask.TaskType.ACTIVE).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(standbyStateManager.taskType()).andReturn(AbstractTask.TaskType.STANDBY).anyTimes();

        EasyMock.expect(storeMetadata.changelogPartition()).andReturn(tp).anyTimes();
        EasyMock.expect(storeMetadata.store()).andReturn(store).anyTimes();
        EasyMock.expect(store.name()).andReturn(storeName).anyTimes();
    }

    @Test
    public void shouldNotRegisterSameStoreMultipleTimes() {
        EasyMock.replay(activeStateManager, storeMetadata);

        changelogReader.register(tp, activeStateManager);

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        assertThrows(IllegalStateException.class, () -> changelogReader.register(tp, activeStateManager));
    }

    @Test
    public void shouldInitializeChangelogAndCheckForCompletion() {
        EasyMock.expect(storeMetadata.offset()).andReturn(9L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(10L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRestoreFromPositionAndCheckForCompletion() {
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertTrue(changelogReader.completedChangelogs().isEmpty());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(6L, consumer.position(tp));
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        // beyond end records should be skipped even when there's gap at the end offset
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(1, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(12L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_BATCH));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
    }

    @Test
    public void shouldRestoreFromBeginningAndCheckCompletion() {
        EasyMock.expect(storeMetadata.offset()).andReturn(null).andReturn(9L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 11L));
            }
        };
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(11L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(5L, consumer.position(tp));
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        // consumer position bypassing the gap in the next poll
        consumer.seek(tp, 11L);

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(3L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(11L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());
    }

    @Test
    public void shouldCheckCompletionIfPositionLargerThanEndOffset() {
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 0L));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(0L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(6L, consumer.position(tp));
        assertEquals(Collections.singleton(tp), consumer.paused());
        assertEquals(tp, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRequestPositionAndHandleTimeoutException() {
        EasyMock.expect(storeMetadata.offset()).andReturn(10L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final AtomicBoolean clearException = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public long position(final TopicPartition partition) {
                if (clearException.get()) {
                    return 10L;
                } else {
                    throw new TimeoutException("KABOOM!");
                }
            }

            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertTrue(changelogReader.completedChangelogs().isEmpty());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());

        clearException.set(true);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(Collections.singleton(tp), changelogReader.completedChangelogs());
        assertEquals(10L, consumer.position(tp));
    }

    @Test
    public void shouldRequestEndOffsetsAndHandleTimeoutException() {
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                if (functionCalled.get()) {
                    return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
                } else {
                    functionCalled.set(true);
                    throw new TimeoutException("KABOOM!");
                }
            }

            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                throw new AssertionError("Should not trigger this function");
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertTrue(functionCalled.get());

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(6L, consumer.position(tp));
    }

    @Test
    public void shouldRequestCommittedOffsetsAndHandleTimeoutException() {
        EasyMock.expect(activeStateManager.changelogAsSource(tp)).andReturn(true).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                if (functionCalled.get()) {
                    return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> new OffsetAndMetadata(10L)));
                } else {
                    functionCalled.set(true);
                    throw new TimeoutException("KABOOM!");
                }
            }

            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 20L));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);
        changelogReader.setMainConsumer(consumer);

        changelogReader.register(tp, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertTrue(functionCalled.get());

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(6L, consumer.position(tp));
    }

    @Test
    public void shouldRestoreToEndOffsetInStandbyState() {
        EasyMock.replay(standbyStateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        changelogReader.register(tp, standbyStateManager);
        changelogReader.restore();

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertNull(changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(5L, changelogReader.changelogMetadata(tp).totalRestored());
        assertTrue(changelogReader.changelogMetadata(tp).bufferedRecords().isEmpty());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRestoreToLimitInStandbyState() {
        EasyMock.expect(standbyStateManager.changelogAsSource(tp)).andReturn(true).anyTimes();
        EasyMock.replay(standbyStateManager, storeMetadata, store);

        final AtomicLong offset = new AtomicLong(7L);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> new OffsetAndMetadata(offset.get())));
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);
        changelogReader.setMainConsumer(consumer);
        changelogReader.transitToUpdateStandby();

        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 5L));
        changelogReader.register(tp, standbyStateManager);
        changelogReader.restore();

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(7L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(tp).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 5L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertNull(changelogReader.changelogMetadata(tp).endOffset());
        assertEquals(7L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(2L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        offset.set(10L);
        changelogReader.updateLimitOffsets();
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(2L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        changelogReader.restore();
        assertEquals(10L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        offset.set(15L);
        changelogReader.updateLimitOffsets();
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(4L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        changelogReader.restore();
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(6L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 12L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 13L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 14L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 15L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(15L, (long) changelogReader.changelogMetadata(tp).limitOffset());
        assertEquals(9L, changelogReader.changelogMetadata(tp).totalRestored());
        assertEquals(1, changelogReader.changelogMetadata(tp).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(tp).bufferedLimitIndex());
    }

    @Test
    public void shouldRestoreMultipleChangelogs() {
        EasyMock.expect(storeMetadataOne.changelogPartition()).andReturn(tp1).anyTimes();
        EasyMock.expect(storeMetadataOne.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadataTwo.changelogPartition()).andReturn(tp2).anyTimes();
        EasyMock.expect(storeMetadataTwo.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(storeMetadataOne.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(storeMetadataTwo.offset()).andReturn(0L).anyTimes();
        EasyMock.expect(activeStateManager.storeMetadata(tp1)).andReturn(storeMetadataOne).anyTimes();
        EasyMock.expect(activeStateManager.storeMetadata(tp2)).andReturn(storeMetadataTwo).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store, storeMetadataOne, storeMetadataTwo);

        setupConsumer(10, tp);
        setupConsumer(5, tp1);
        setupConsumer(3, tp2);

        changelogReader.register(tp, activeStateManager);
        changelogReader.register(tp1, activeStateManager);
        changelogReader.register(tp2, activeStateManager);

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());

        // should support removing and clearing changelogs
        changelogReader.remove(Collections.singletonList(tp));
        assertNull(changelogReader.changelogMetadata(tp));
        assertFalse(changelogReader.isEmpty());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());

        changelogReader.clear();
        assertTrue(changelogReader.isEmpty());
        assertNull(changelogReader.changelogMetadata(tp1));
        assertNull(changelogReader.changelogMetadata(tp2));
    }

    @Test
    public void shouldTransitState() {
        EasyMock.expect(storeMetadataOne.changelogPartition()).andReturn(tp1).anyTimes();
        EasyMock.expect(storeMetadataOne.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadataTwo.changelogPartition()).andReturn(tp2).anyTimes();
        EasyMock.expect(storeMetadataTwo.store()).andReturn(store).anyTimes();
        EasyMock.expect(storeMetadata.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(storeMetadataOne.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(storeMetadataTwo.offset()).andReturn(5L).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp1)).andReturn(storeMetadataOne).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(tp2)).andReturn(storeMetadataTwo).anyTimes();
        EasyMock.replay(activeStateManager, standbyStateManager, storeMetadata, store, storeMetadataOne, storeMetadataTwo);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
            }
        };
        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(tp, activeStateManager);
        changelogReader.register(tp1, standbyStateManager);
        changelogReader.register(tp2, standbyStateManager);
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(tp2).state());

        assertEquals(Collections.emptySet(), consumer.assignment());

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(mkSet(tp1, tp2), consumer.paused());

        // transition to restore active is idempotent
        changelogReader.transitToRestoreActive();

        changelogReader.transitToUpdateStandby();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(Collections.emptySet(), consumer.paused());

        // transition to update standby is NOT idempotent
        assertThrows(IllegalStateException.class, changelogReader::transitToUpdateStandby);

        changelogReader.remove(Collections.singletonList(tp));
        changelogReader.register(tp, activeStateManager);

        // if a new active is registered, we should immediately transit to standby updating
        assertThrows(IllegalStateException.class, changelogReader::restore);

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp1).state());
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(tp2).state());
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(Collections.emptySet(), consumer.paused());

        changelogReader.transitToRestoreActive();
        assertEquals(mkSet(tp, tp1, tp2), consumer.assignment());
        assertEquals(mkSet(tp1, tp2), consumer.paused());
    }

    @Test
    public void shouldHandleExceptionFromRestoreCallback() {

    }

    private void setupConsumer(final long messages, final TopicPartition topicPartition) {
        assignPartition(messages, topicPartition);
        addRecords(messages, topicPartition);
        consumer.assign(Collections.emptyList());
    }

    private void addRecords(final long messages, final TopicPartition topicPartition) {
        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                i,
                new byte[0],
                new byte[0]));
        }
    }

    private void assignPartition(final long messages,
                                 final TopicPartition topicPartition) {
        consumer.updatePartitions(
            topicPartition.topic(),
            Collections.singletonList(new PartitionInfo(
                topicPartition.topic(),
                topicPartition.partition(),
                null,
                null,
                null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages) + 1));
        consumer.assign(Collections.singletonList(topicPartition));
    }
}
