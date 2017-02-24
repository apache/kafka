/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.test.MockRestoreCallback;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;

public class StoreChangelogReaderTest {

    private final MockRestoreCallback callback = new MockRestoreCallback();
    private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, new MockTime(), 0);
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final PartitionInfo partitionInfo = new PartitionInfo(topicPartition.topic(), 0, null, null, null);

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionWhenTimeoutExceptionThrown() throws Exception {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<String, List<PartitionInfo>> listTopics() {
                throw new TimeoutException("KABOOM!");
            }
        };
        final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, new MockTime(), 0);
        try {
            changelogReader.validatePartitionExists(topicPartition, "store");
            fail("Should have thrown streams exception");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfPartitionDoesntExistAfterMaxWait() throws Exception {
        changelogReader.validatePartitionExists(topicPartition, "store");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFallbackToPartitionsForIfPartitionNotInAllPartitionsList() throws Exception {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.singletonList(partitionInfo);
            }
        };

        final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, new MockTime(), 10);
        changelogReader.validatePartitionExists(topicPartition, "store");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionIfTimeoutOccursDuringPartitionsFor() throws Exception {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                throw new TimeoutException("KABOOM!");
            }
        };
        final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, new MockTime(), 5);
        try {
            changelogReader.validatePartitionExists(topicPartition, "store");
            fail("Should have thrown streams exception");
        } catch (final StreamsException e) {
            // pass
        }
    }

    @Test
    public void shouldPassIfTopicPartitionExists() throws Exception {
        consumer.updatePartitions(topicPartition.topic(), Collections.singletonList(partitionInfo));
        changelogReader.validatePartitionExists(topicPartition, "store");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRequestPartitionInfoIfItDoesntExist() throws Exception {
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<String, List<PartitionInfo>> listTopics() {
                return Collections.emptyMap();
            }
        };

        consumer.updatePartitions(topicPartition.topic(), Collections.singletonList(partitionInfo));
        final StoreChangelogReader changelogReader = new StoreChangelogReader(consumer, Time.SYSTEM, 5000);
        changelogReader.validatePartitionExists(topicPartition, "store");
    }


    @Test
    public void shouldThrowExceptionIfConsumerHasCurrentSubscription() throws Exception {
        consumer.subscribe(Collections.singleton("sometopic"));
        try {
            changelogReader.restore();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void shouldRestoreAllMessagesFromBeginningWhenCheckpointNull() throws Exception {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, true));

        changelogReader.restore();
        assertThat(callback.restoreCount, equalTo(messages));
    }

    @Test
    public void shouldRestoreMessagesFromCheckpoint() throws Exception {
        final int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, callback, 5L, Long.MAX_VALUE, true));

        changelogReader.restore();
        assertThat(callback.restoreCount, equalTo(5));
    }

    @Test
    public void shouldClearAssignmentAtEndOfRestore() throws Exception {
        final int messages = 1;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, true));

        changelogReader.restore();
        assertThat(consumer.assignment(), equalTo(Collections.<TopicPartition>emptySet()));
    }

    @Test
    public void shouldRestoreToLimitWhenSupplied() throws Exception {
        setupConsumer(10, topicPartition);
        final StateRestorer restorer = new StateRestorer(topicPartition, callback, null, 3, true);
        changelogReader.register(restorer);

        changelogReader.restore();
        assertThat(callback.restoreCount, equalTo(3));
        assertThat(restorer.restoredOffset(), equalTo(3L));
    }

    @Test
    public void shouldRestoreMultipleStores() throws Exception {
        final TopicPartition one = new TopicPartition("one", 0);
        final TopicPartition two = new TopicPartition("two", 0);
        final MockRestoreCallback callbackOne = new MockRestoreCallback();
        final MockRestoreCallback callbackTwo = new MockRestoreCallback();
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader.register(new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, true));
        changelogReader.register(new StateRestorer(one, callbackOne, null, Long.MAX_VALUE, true));
        changelogReader.register(new StateRestorer(two, callbackTwo, null, Long.MAX_VALUE, true));

        changelogReader.restore();

        assertThat(callback.restoreCount, equalTo(10));
        assertThat(callbackOne.restoreCount, equalTo(5));
        assertThat(callbackTwo.restoreCount, equalTo(3));
    }

    @Test
    public void shouldNotRestoreAnythingWhenPartitionIsEmpty() throws Exception {
        final StateRestorer restorer = new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, true);
        setupConsumer(0, topicPartition);
        changelogReader.register(restorer);

        changelogReader.restore();
        assertThat(callback.restoreCount, equalTo(0));
        assertThat(restorer.restoredOffset(), equalTo(0L));
    }

    @Test
    public void shouldNotRestoreAnythingWhenCheckpointAtEndOffset() throws Exception {
        final Long endOffset = 10L;
        setupConsumer(endOffset, topicPartition);
        final StateRestorer restorer = new StateRestorer(topicPartition, callback, endOffset, Long.MAX_VALUE, true);

        changelogReader.register(restorer);

        changelogReader.restore();
        assertThat(callback.restoreCount, equalTo(0));
        assertThat(restorer.restoredOffset(), equalTo(endOffset));
    }

    @Test
    public void shouldReturnRestoredOffsetsForPersistentStores() throws Exception {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, true));
        changelogReader.restore();
        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.singletonMap(topicPartition, 10L)));
    }

    @Test
    public void shouldNotReturnRestoredOffsetsForNonPersistentStore() throws Exception {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(topicPartition, callback, null, Long.MAX_VALUE, false));
        changelogReader.restore();
        final Map<TopicPartition, Long> restoredOffsets = changelogReader.restoredOffsets();
        assertThat(restoredOffsets, equalTo(Collections.<TopicPartition, Long>emptyMap()));
    }

    private void setupConsumer(final long messages, final TopicPartition topicPartition) {
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

        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0]));
        }
        consumer.assign(Collections.<TopicPartition>emptyList());
    }

}