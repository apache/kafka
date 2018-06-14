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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.test.MockRestoreCallback;
import org.apache.kafka.test.MockStateRestoreListener;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class StoreChangelogReaderTest {

    @Mock(type = MockType.NICE)
    private RestoringTasks active;
    @Mock(type = MockType.NICE)
    private StreamTask task;

    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final CompositeRestoreListener restoreListener = new CompositeRestoreListener(callback);
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final LogContext logContext = new LogContext("test-reader ");
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, Duration.ZERO, stateRestoreListener, logContext);

    @Before
    public void setUp() {
        restoreListener.setUserRestoreListener(stateRestoreListener);
    }

    @Test
    public void shouldRequestTopicsAndHandleTimeoutException() {
        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<String, List<PartitionInfo>> listTopics() {
                functionCalled.set(true);
                throw new TimeoutException("KABOOM!");
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, Duration.ZERO, stateRestoreListener, logContext);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName"));
        changelogReader.restore(active);
        assertTrue(functionCalled.get());
    }

    @Test
    public void shouldThrowExceptionIfConsumerHasCurrentSubscription() {
        final StateRestorer mockRestorer = EasyMock.mock(StateRestorer.class);
        mockRestorer.setUserRestoreListener(stateRestoreListener);
        expect(mockRestorer.partition()).andReturn(new TopicPartition("sometopic", 0)).andReturn(new TopicPartition("sometopic", 0));
        EasyMock.replay(mockRestorer);
        changelogReader.register(mockRestorer);

        consumer.subscribe(Collections.singleton("sometopic"));

        try {
            changelogReader.restore(active);
            fail("Should have thrown IllegalStateException");
        } catch (final StreamsException expected) {
            // ok
        }
    }

    @Test
    public void shouldRestoreAllMessagesFromBeginningWhenCheckpointNull() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName"));
        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(messages));
    }

    @Test
    public void shouldRecoverFromInvalidOffsetExceptionAndFinishRestore() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        consumer.setException(new InvalidOffsetException("Try Again!") {
            @Override
            public Set<TopicPartition> partitions() {
                return Collections.singleton(topicPartition);
            }
        });
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true,
                                                   "storeName"));

        EasyMock.expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        EasyMock.replay(active);

        // first restore call "fails" but we should not die with an exception
        assertEquals(0, changelogReader.restore(active).size());
        // retry restore should succeed
        assertEquals(1, changelogReader.restore(active).size());
        assertThat(callback.restored.size(), equalTo(messages));
    }


    @Test
    public void shouldRestoreMessagesFromCheckpoint() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, 5L, Long.MAX_VALUE, true,
                                                   "storeName"));

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(5));
    }

    @Test
    public void shouldClearAssignmentAtEndOfRestore() {
        final int messages = 1;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true,
                                                   "storeName"));

        changelogReader.restore(active);
        assertThat(consumer.assignment(), equalTo(Collections.<TopicPartition>emptySet()));
    }

    @Test
    public void shouldRestoreToLimitWhenSupplied() {
        setupConsumer(10, topicPartition);
        final StateRestorer restorer = new StateRestorer(topicPartition, restoreListener, null, 3, true,
                                                         "storeName");
        changelogReader.register(restorer);
        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(3));
        assertThat(restorer.restoredOffset(), equalTo(3L));
    }

    @Test
    public void shouldRestoreMultipleStores() {
        final TopicPartition one = new TopicPartition("one", 0);
        final TopicPartition two = new TopicPartition("two", 0);
        final MockRestoreCallback callbackOne = new MockRestoreCallback();
        final MockRestoreCallback callbackTwo = new MockRestoreCallback();
        final CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
        final CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader
            .register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName1"));
        changelogReader.register(new StateRestorer(one, restoreListener1, null, Long.MAX_VALUE, true, "storeName2"));
        changelogReader.register(new StateRestorer(two, restoreListener2, null, Long.MAX_VALUE, true, "storeName3"));

        expect(active.restoringTaskFor(one)).andReturn(null);
        expect(active.restoringTaskFor(two)).andReturn(null);
        replay(active);
        changelogReader.restore(active);

        assertThat(callback.restored.size(), equalTo(10));
        assertThat(callbackOne.restored.size(), equalTo(5));
        assertThat(callbackTwo.restored.size(), equalTo(3));
    }

    @Test
    public void shouldRestoreAndNotifyMultipleStores() throws Exception {
        final TopicPartition one = new TopicPartition("one", 0);
        final TopicPartition two = new TopicPartition("two", 0);
        final MockStateRestoreListener callbackOne = new MockStateRestoreListener();
        final MockStateRestoreListener callbackTwo = new MockStateRestoreListener();
        final CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
        final CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader
            .register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName1"));
        changelogReader.register(new StateRestorer(one, restoreListener1, null, Long.MAX_VALUE, true, "storeName2"));
        changelogReader.register(new StateRestorer(two, restoreListener2, null, Long.MAX_VALUE, true, "storeName3"));

        expect(active.restoringTaskFor(one)).andReturn(null);
        expect(active.restoringTaskFor(two)).andReturn(null);
        replay(active);
        changelogReader.restore(active);

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
        changelogReader
            .register(new StateRestorer(topicPartition, restoreListener, null, 5, true, "storeName1"));
        changelogReader.restore(active);

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
    public void shouldNotRestoreAnythingWhenPartitionIsEmpty() {
        final StateRestorer
            restorer =
            new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName");
        setupConsumer(0, topicPartition);
        changelogReader.register(restorer);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(0));
        assertThat(restorer.restoredOffset(), equalTo(0L));
    }

    @Test
    public void shouldNotRestoreAnythingWhenCheckpointAtEndOffset() {
        final Long endOffset = 10L;
        setupConsumer(endOffset, topicPartition);
        final StateRestorer
            restorer =
            new StateRestorer(topicPartition, restoreListener, endOffset, Long.MAX_VALUE, true, "storeName");

        changelogReader.register(restorer);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(0));
        assertThat(restorer.restoredOffset(), equalTo(endOffset));
    }

    @Test
    public void shouldReturnRestoredOffsetsForPersistentStores() {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName"));
        changelogReader.restore(active);
        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.singletonMap(topicPartition, 10L)));
    }

    @Test
    public void shouldNotReturnRestoredOffsetsForNonPersistentStore() {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, false, "storeName"));
        changelogReader.restore(active);
        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.<TopicPartition, Long>emptyMap()));
    }

    @Test
    public void shouldIgnoreNullKeysWhenRestoring() {
        assignPartition(3, topicPartition);
        final byte[] bytes = new byte[0];
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 0, bytes, bytes));
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 1, (byte[]) null, bytes));
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 2, bytes, bytes));
        consumer.assign(Collections.singletonList(topicPartition));
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, false,
                                                   "storeName"));
        changelogReader.restore(active);

        assertThat(callback.restored, CoreMatchers.equalTo(Utils.mkList(KeyValue.pair(bytes, bytes), KeyValue.pair(bytes, bytes))));
    }

    @Test
    public void shouldCompleteImmediatelyWhenEndOffsetIs0() {
        final Collection<TopicPartition> expected = Collections.singleton(topicPartition);
        setupConsumer(0, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "store"));
        final Collection<TopicPartition> restored = changelogReader.restore(active);
        assertThat(restored, equalTo(expected));
    }

    @Test
    public void shouldRestorePartitionsRegisteredPostInitialization() {
        final MockRestoreCallback callbackTwo = new MockRestoreCallback();
        final CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);

        setupConsumer(1, topicPartition);
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 10L));
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, false, "storeName"));

        final TopicPartition postInitialization = new TopicPartition("other", 0);
        expect(active.restoringTaskFor(topicPartition)).andReturn(null);
        expect(active.restoringTaskFor(topicPartition)).andReturn(null);
        expect(active.restoringTaskFor(postInitialization)).andReturn(null);
        replay(active);

        assertTrue(changelogReader.restore(active).isEmpty());

        addRecords(9, topicPartition, 1);

        setupConsumer(3, postInitialization);
        consumer.updateBeginningOffsets(Collections.singletonMap(postInitialization, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(postInitialization, 3L));

        changelogReader.register(new StateRestorer(postInitialization, restoreListener2, null, Long.MAX_VALUE, false, "otherStore"));

        final Collection<TopicPartition> expected = Utils.mkSet(topicPartition, postInitialization);
        consumer.assign(expected);

        assertThat(changelogReader.restore(active), equalTo(expected));
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
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, 9L, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
    }


    @Test
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSEnabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // records have offsets of 0..9 10 is commit marker so 11 is end offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(10));
    }


    @Test
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSDisabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);

        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, Long.MAX_VALUE, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(10));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopic() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, 5, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(5));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopic() {
        final int messages = 10;
        setupConsumer(messages, topicPartition);

        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, 10, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(10));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopicEOSEnabled() {
        final int totalMessages = 10;
        assignPartition(totalMessages, topicPartition);
        // records 0..4 last offset before commit is 4
        addRecords(5, topicPartition, 0);
        //EOS enabled so commit marker at offset 5 so records start at 6
        addRecords(5, topicPartition, 6);
        consumer.assign(Collections.<TopicPartition>emptyList());
        // commit marker is 5 so ending offset is 12
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 12L));

        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, 6, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(5));
    }

    @Test
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopicEOSEnabled() {
        final int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // records have offsets 0..9 10 is commit marker so 11 is ending offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(new StateRestorer(topicPartition, restoreListener, null, 11, true, "storeName"));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        assertThat(callback.restored.size(), equalTo(10));
    }

    private void setupConsumer(final long messages,
                               final TopicPartition topicPartition) {
        assignPartition(messages, topicPartition);
        addRecords(messages, topicPartition, 0);
        consumer.assign(Collections.<TopicPartition>emptyList());
    }

    private void addRecords(final long messages,
                            final TopicPartition topicPartition,
                            final int startingOffset) {
        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), startingOffset + i, new byte[0], new byte[0]));
        }
    }

    private void assignPartition(final long messages,
                                 final TopicPartition topicPartition) {
        consumer.updatePartitions(topicPartition.topic(),
                                  Collections.singletonList(
                                      new PartitionInfo(topicPartition.topic(),
                                                        topicPartition.partition(),
                                                        null,
                                                        null,
                                                        null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages)));
        consumer.assign(Collections.singletonList(topicPartition));
    }

}