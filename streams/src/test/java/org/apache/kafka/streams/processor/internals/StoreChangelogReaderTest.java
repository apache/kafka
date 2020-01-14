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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.MockRestoreCallback;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
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

import static java.util.Arrays.asList;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// TODO K9113: fix tests
@RunWith(EasyMockRunner.class)
public class StoreChangelogReaderTest {

    @Mock(type = MockType.NICE)
    private RestoringTasks active;
    @Mock(type = MockType.NICE)
    private StreamTask task;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager activeStateManager;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager standbyStateManager;
    @Mock(type = MockType.NICE)
    private ProcessorStateManager.StateStoreMetadata storeMetadata;
    @Mock(type = MockType.NICE)
    private StateStore store;

    private final String storeName = "store";
    private final String topicName = "topic";
    private final LogContext logContext = new LogContext("test-reader ");
    private final TopicPartition topicPartition = new TopicPartition(topicName, 0);
    private final StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test-reader"));
    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

    @Before
    public void setUp() {
        EasyMock.expect(activeStateManager.storeMetadata(topicPartition)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(activeStateManager.taskType()).andReturn(AbstractTask.TaskType.ACTIVE).anyTimes();
        EasyMock.expect(standbyStateManager.storeMetadata(topicPartition)).andReturn(storeMetadata).anyTimes();
        EasyMock.expect(standbyStateManager.taskType()).andReturn(AbstractTask.TaskType.STANDBY).anyTimes();

        EasyMock.expect(storeMetadata.changelogPartition()).andReturn(topicPartition).anyTimes();
        EasyMock.expect(storeMetadata.store()).andReturn(store).anyTimes();
        EasyMock.expect(store.name()).andReturn(storeName).anyTimes();
    }

    @Test
    public void shouldNotRegisterSameStoreMultipleTimes() {
        EasyMock.replay(activeStateManager, storeMetadata);

        changelogReader.register(topicPartition, activeStateManager);

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());

        assertThrows(IllegalStateException.class, () -> changelogReader.register(topicPartition, activeStateManager));
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

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(10L, consumer.position(topicPartition));
        assertEquals(Collections.singleton(topicPartition), consumer.paused());
        assertEquals(topicPartition, callback.restoreTopicPartition);
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

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(6L, consumer.position(topicPartition));
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(topicPartition, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        // beyond end records should be skipped
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(3L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());
        assertEquals(12L, consumer.position(topicPartition));
        assertEquals(Collections.singleton(topicPartition), consumer.paused());
        assertEquals(topicPartition, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_BATCH));
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_END));
    }

    @Test
    public void shouldRestoreFromBeginningAndCheckCompletion() {
        EasyMock.expect(storeMetadata.offset()).andReturn(null).anyTimes();
        EasyMock.replay(activeStateManager, storeMetadata, store);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> 10L));
            }
        };
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 5L));

        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, callback);

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(5L, consumer.position(topicPartition));
        assertEquals(Collections.emptySet(), consumer.paused());
        assertEquals(topicPartition, callback.restoreTopicPartition);
        assertEquals(storeName, callback.storeNameCalledStates.get(RESTORE_START));
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
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

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());

        clearException.set(true);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.COMPLETED, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(10L, consumer.position(topicPartition));
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

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertTrue(functionCalled.get());

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(6L, consumer.position(topicPartition));
    }

    @Test
    public void shouldRequestCommittedOffsetsAndHandleTimeoutException() {
        EasyMock.expect(activeStateManager.changelogAsSource(topicPartition)).andReturn(true).anyTimes();
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

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.REGISTERED, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertTrue(functionCalled.get());

        changelogReader.restore();

        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(6L, consumer.position(topicPartition));
    }

    @Test
    public void shouldRestoreToEndOffsetInStandbyState() {
        EasyMock.replay(standbyStateManager, storeMetadata, store);

        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 5L));
        changelogReader.register(topicPartition, standbyStateManager);
        changelogReader.restore();

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertNull(changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(5L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertTrue(changelogReader.changelogMetadata(topicPartition).bufferedRecords().isEmpty());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));
    }

    @Test
    public void shouldRestoreToLimitInStandbyState() {
        EasyMock.expect(standbyStateManager.changelogAsSource(topicPartition)).andReturn(true).anyTimes();
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

        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 5L));
        changelogReader.register(topicPartition, standbyStateManager);
        changelogReader.restore();

        assertNull(callback.restoreTopicPartition);
        assertNull(callback.storeNameCalledStates.get(RESTORE_START));
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(7L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(0L, changelogReader.changelogMetadata(topicPartition).totalRestored());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 5L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 6L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 7L, "key".getBytes(), "value".getBytes()));
        // null key should be ignored
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 8L, null, "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 9L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 10L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 11L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(StoreChangelogReader.ChangelogState.RESTORING, changelogReader.changelogMetadata(topicPartition).state());
        assertNull(changelogReader.changelogMetadata(topicPartition).endOffset());
        assertEquals(7L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(2L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());
        assertNull(callback.storeNameCalledStates.get(RESTORE_END));
        assertNull(callback.storeNameCalledStates.get(RESTORE_BATCH));

        offset.set(10L);
        changelogReader.updateLimitOffsets();
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(2L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(4, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());

        changelogReader.restore();
        assertEquals(10L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(4L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());

        offset.set(15L);
        changelogReader.updateLimitOffsets();
        assertEquals(15L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(4L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(2, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(2, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());

        changelogReader.restore();
        assertEquals(15L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(6L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());

        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 12L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 13L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 14L, "key".getBytes(), "value".getBytes()));
        consumer.addRecord(new ConsumerRecord<>(topicName, 0, 15L, "key".getBytes(), "value".getBytes()));

        changelogReader.restore();
        assertEquals(15L, (long) changelogReader.changelogMetadata(topicPartition).limitOffset());
        assertEquals(9L, changelogReader.changelogMetadata(topicPartition).totalRestored());
        assertEquals(1, changelogReader.changelogMetadata(topicPartition).bufferedRecords().size());
        assertEquals(0, changelogReader.changelogMetadata(topicPartition).bufferedLimitIndex());
    }

    @Test
    public void shouldRestoreMultipleStores() {
        final TopicPartition one = new TopicPartition("one", 0);
        final TopicPartition two = new TopicPartition("two", 0);
        final MockRestoreCallback callbackOne = new MockRestoreCallback();
        final MockRestoreCallback callbackTwo = new MockRestoreCallback();
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.register(one, activeStateManager);
        changelogReader.register(two, activeStateManager);

        expect(active.restoringTaskFor(one)).andStubReturn(task);
        expect(active.restoringTaskFor(two)).andStubReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.restored.size(), equalTo(10));
        assertThat(callbackOne.restored.size(), equalTo(5));
        assertThat(callbackTwo.restored.size(), equalTo(3));
    }

    @Test
    public void shouldRestoreAndNotifyMultipleStores() {
        final TopicPartition one = new TopicPartition("one", 0);
        final TopicPartition two = new TopicPartition("two", 0);
        final MockStateRestoreListener callbackOne = new MockStateRestoreListener();
        final MockStateRestoreListener callbackTwo = new MockStateRestoreListener();
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader.register(topicPartition, activeStateManager);
        changelogReader.register(one, activeStateManager);
        changelogReader.register(two, activeStateManager);

        expect(active.restoringTaskFor(one)).andReturn(task);
        expect(active.restoringTaskFor(two)).andReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();
        changelogReader.restore();

        assertThat(callback.restored.size(), equalTo(10));
        assertThat(callbackOne.restored.size(), equalTo(5));
        assertThat(callbackTwo.restored.size(), equalTo(3));

        assertAllCallbackStatesExecuted(callback, "storeName1");
        assertCorrectOffsetsReportedByListener(callback, 0L, 9L, 10L);

        assertAllCallbackStatesExecuted(callbackOne, "storeName2");
        assertCorrectOffsetsReportedByListener(callbackOne, 0L, 4L, 5L);

        assertAllCallbackStatesExecuted(callbackTwo, "storeName3");
        assertCorrectOffsetsReportedByListener(callbackTwo, 0L, 2L, 3L);
    }

    @Test
    public void shouldOnlyReportTheLastRestoredOffset() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.restored.size(), equalTo(5));
        assertAllCallbackStatesExecuted(callback, "storeName1");
        assertCorrectOffsetsReportedByListener(callback, 0L, 4L, 5L);
    }

    private void assertAllCallbackStatesExecuted(final MockStateRestoreListener restoreListener,
                                                 final String storeName) {
        assertThat(restoreListener.storeNameCalledStates.get(RESTORE_START), equalTo(storeName));
        assertThat(restoreListener.storeNameCalledStates.get(RESTORE_BATCH), equalTo(storeName));
        assertThat(restoreListener.storeNameCalledStates.get(RESTORE_END), equalTo(storeName));
    }

    private void assertCorrectOffsetsReportedByListener(final MockStateRestoreListener restoreListener,
                                                        final long startOffset,
                                                        final long batchOffset,
                                                        final long totalRestored) {

        assertThat(restoreListener.restoreStartOffset, equalTo(startOffset));
        assertThat(restoreListener.restoredBatchOffset, equalTo(batchOffset));
        assertThat(restoreListener.totalNumRestored, equalTo(totalRestored));
    }

    @Test
    public void shouldReturnRestoredOffsetsForPersistentStores() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.singletonMap(topicPartition, 10L)));
    }

    @Test
    public void shouldNotReturnRestoredOffsetsForNonPersistentStore() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();
        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.emptyMap()));
    }

    @Test
    public void shouldIgnoreNullKeysWhenRestoring() {
        assignPartition(3, topicPartition);
        final byte[] bytes = new byte[0];
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 0, bytes, bytes));
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 1, null, bytes));
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 2, bytes, bytes));
        consumer.assign(Collections.singletonList(topicPartition));
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.restored, CoreMatchers.equalTo(asList(KeyValue.pair(bytes, bytes), KeyValue.pair(bytes, bytes))));
    }

    @Test
    public void shouldCompleteImmediatelyWhenEndOffsetIs0() {
        final Collection<TopicPartition> expected = Collections.singleton(topicPartition);
        setupConsumer(0, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);

        changelogReader.restore();
        //assertThat(restored, equalTo(expected));
    }

    @Test
    public void shouldRestorePartitionsRegisteredPostInitialization() {
        final MockRestoreCallback callbackTwo = new MockRestoreCallback();

        setupConsumer(1, topicPartition);
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 10L));
        changelogReader.register(topicPartition, activeStateManager);


        final TopicPartition postInitialization = new TopicPartition("other", 0);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        expect(active.restoringTaskFor(postInitialization)).andStubReturn(task);
        replay(active, task);

        changelogReader.restore();
        //assertTrue();

        addRecords(9, topicPartition, 1);

        setupConsumer(3, postInitialization);
        consumer.updateBeginningOffsets(Collections.singletonMap(postInitialization, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(postInitialization, 3L));

        changelogReader.register(topicPartition, activeStateManager);

        final Collection<TopicPartition> expected = Utils.mkSet(topicPartition, postInitialization);
        consumer.assign(expected);

        //assertThat(changelogReader.restore(), equalTo(expected));
        assertThat(callback.restored.size(), equalTo(10));
        assertThat(callbackTwo.restored.size(), equalTo(3));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfSourceTopicUpdatedDuringRestoreProcess() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        // in this case first call to endOffsets returns correct value, but a second thread has updated the source topic
        // but since it's a source topic, the second check should not fire hence no exception
        consumer.addEndOffsets(Collections.singletonMap(topicPartition, 15L));
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSEnabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // EOS enabled simulated by setting offsets with commit marker
        // records have offsets of 0..9 10 is commit marker so 11 is end offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(10));
    }


    @Test
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSDisabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);

        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(10));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopic() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(5));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopic() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);

        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(10));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopicEOSEnabled() {
        final int totalMessages = 10;
        assignPartition(totalMessages, topicPartition);
        // records 0..4 last offset before commit is 4
        addRecords(5, topicPartition, 0);
        // EOS enabled simulated by setting offsets with commit marker
        // EOS enabled so commit marker at offset 5 so records start at 6
        addRecords(5, topicPartition, 6);
        consumer.assign(Collections.emptyList());
        // commit marker is 5 so ending offset is 12
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 12L));

        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(5));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopicEOSEnabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // EOS enabled simulated by setting offsets with commit marker
        // records have offsets 0..9 10 is commit marker so 11 is ending offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(topicPartition, activeStateManager);

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore();
        assertThat(callback.restored.size(), equalTo(10));
    }

    @Test
    public void shouldRestoreUpToOffsetLimit() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.restored.size(), equalTo(3));
        assertAllCallbackStatesExecuted(callback, "storeName1");
        assertCorrectOffsetsReportedByListener(callback, 2L, 4L, 3L);
    }

    @Test
    public void shouldNotRestoreIfCheckpointIsEqualToOffsetLimit() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.storeNameCalledStates.size(), equalTo(0));
        assertThat(callback.restored.size(), equalTo(0));
    }

    @Test
    public void shouldNotRestoreIfCheckpointIsGreaterThanOffsetLimit() {
        setupConsumer(10, topicPartition);
        changelogReader.register(topicPartition, activeStateManager);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore();

        assertThat(callback.storeNameCalledStates.size(), equalTo(0));
        assertThat(callback.restored.size(), equalTo(0));
    }

    private void setupConsumer(final long messages,
                               final TopicPartition topicPartition) {
        assignPartition(messages, topicPartition);
        addRecords(messages, topicPartition, 0);
        consumer.assign(Collections.emptyList());
    }

    private void addRecords(final long messages,
                            final TopicPartition topicPartition,
                            final int startingOffset) {
        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                startingOffset + i,
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
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages)));
        consumer.assign(Collections.singletonList(topicPartition));
    }

}
