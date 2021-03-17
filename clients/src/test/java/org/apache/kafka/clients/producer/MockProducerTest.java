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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MockProducerTest {

    private final String topic = "topic";
    private MockProducer<byte[], byte[]> producer;
    private final ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, "key1".getBytes(), "value1".getBytes());
    private final ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, "key2".getBytes(), "value2".getBytes());
    private final String groupId = "group";

    private void buildMockProducer(boolean autoComplete) {
        this.producer = new MockProducer<>(autoComplete, new MockSerializer(), new MockSerializer());
    }

    @AfterEach
    public void cleanup() {
        if (this.producer != null && !this.producer.closed())
            this.producer.close();
    }

    @Test
    public void testAutoCompleteMock() throws Exception {
        buildMockProducer(true);
        Future<RecordMetadata> metadata = producer.send(record1);
        assertTrue(metadata.isDone(), "Send should be immediately complete");
        assertFalse(isError(metadata), "Send should be successful");
        assertEquals(0L, metadata.get().offset(), "Offset should be 0");
        assertEquals(topic, metadata.get().topic());
        assertEquals(singletonList(record1), producer.history(), "We should have the record in our history");
        producer.clear();
        assertEquals(0, producer.history().size(), "Clear should erase our history");
    }

    @Test
    public void testPartitioner() throws Exception {
        PartitionInfo partitionInfo0 = new PartitionInfo(topic, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(topic, 1, null, null, null);
        Cluster cluster = new Cluster(null, new ArrayList<>(0), asList(partitionInfo0, partitionInfo1),
                Collections.emptySet(), Collections.emptySet());
        MockProducer<String, String> producer = new MockProducer<>(cluster, true, new DefaultPartitioner(), new StringSerializer(), new StringSerializer());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", "value");
        Future<RecordMetadata> metadata = producer.send(record);
        assertEquals(1, metadata.get().partition(), "Partition should be correct");
        producer.clear();
        assertEquals(0, producer.history().size(), "Clear should erase our history");
        producer.close();
    }

    @Test
    public void testManualCompletion() throws Exception {
        buildMockProducer(false);
        Future<RecordMetadata> md1 = producer.send(record1);
        assertFalse(md1.isDone(), "Send shouldn't have completed");
        Future<RecordMetadata> md2 = producer.send(record2);
        assertFalse(md2.isDone(), "Send shouldn't have completed");
        assertTrue(producer.completeNext(), "Complete the first request");
        assertFalse(isError(md1), "Requst should be successful");
        assertFalse(md2.isDone(), "Second request still incomplete");
        IllegalArgumentException e = new IllegalArgumentException("blah");
        assertTrue(producer.errorNext(e), "Complete the second request with an error");
        try {
            md2.get();
            fail("Expected error to be thrown");
        } catch (ExecutionException err) {
            assertEquals(e, err.getCause());
        }
        assertFalse(producer.completeNext(), "No more requests to complete");

        Future<RecordMetadata> md3 = producer.send(record1);
        Future<RecordMetadata> md4 = producer.send(record2);
        assertTrue(!md3.isDone() && !md4.isDone(), "Requests should not be completed.");
        producer.flush();
        assertTrue(md3.isDone() && md4.isDone(), "Requests should be completed.");
    }

    @Test
    public void shouldInitTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        assertTrue(producer.transactionInitialized());
    }

    @Test
    public void shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        assertThrows(IllegalStateException.class, producer::initTransactions);
    }

    @Test
    public void shouldThrowOnBeginTransactionIfTransactionsNotInitialized() {
        buildMockProducer(true);
        assertThrows(IllegalStateException.class, producer::beginTransaction);
    }

    @Test
    public void shouldBeginTransactions() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldThrowOnBeginTransactionsIfTransactionInflight() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        assertThrows(IllegalStateException.class, () -> producer.beginTransaction());
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized() {
        buildMockProducer(true);
        assertThrows(IllegalStateException.class, () -> producer.sendOffsetsToTransaction(null, groupId));
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true);
        producer.initTransactions();
        assertThrows(IllegalStateException.class, () -> producer.sendOffsetsToTransaction(null, groupId));
    }

    @Test
    public void shouldThrowOnCommitIfTransactionsNotInitialized() {
        buildMockProducer(true);
        assertThrows(IllegalStateException.class, producer::commitTransaction);
    }

    @Test
    public void shouldThrowOnCommitTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true);
        producer.initTransactions();
        assertThrows(IllegalStateException.class, producer::commitTransaction);
    }

    @Test
    public void shouldCommitEmptyTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        producer.commitTransaction();
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldCountCommittedTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        assertEquals(0L, producer.commitCount());
        producer.commitTransaction();
        assertEquals(1L, producer.commitCount());
    }

    @Test
    public void shouldNotCountAbortedTransaction() {
        buildMockProducer(true);
        producer.initTransactions();

        producer.beginTransaction();
        producer.abortTransaction();

        producer.beginTransaction();
        producer.commitTransaction();
        assertEquals(1L, producer.commitCount());
    }

    @Test
    public void shouldThrowOnAbortIfTransactionsNotInitialized() {
        buildMockProducer(true);
        assertThrows(IllegalStateException.class, () -> producer.abortTransaction());
    }

    @Test
    public void shouldThrowOnAbortTransactionIfNoTransactionGotStarted() {
        buildMockProducer(true);
        producer.initTransactions();
        assertThrows(IllegalStateException.class, producer::abortTransaction);
    }

    @Test
    public void shouldAbortEmptyTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        producer.abortTransaction();
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
    }

    @Test
    public void shouldThrowFenceProducerIfTransactionsNotInitialized() {
        buildMockProducer(true);
        assertThrows(IllegalStateException.class, () -> producer.fenceProducer());
    }

    @Test
    public void shouldThrowOnBeginTransactionsIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        assertThrows(ProducerFencedException.class, producer::beginTransaction);
    }

    @Test
    public void shouldThrowOnSendIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        Throwable e = assertThrows(KafkaException.class, () -> producer.send(null));
        assertTrue(e.getCause() instanceof ProducerFencedException, "The root cause of the exception should be ProducerFenced");
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        assertThrows(ProducerFencedException.class, () -> producer.sendOffsetsToTransaction(null, groupId));
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        assertThrows(ProducerFencedException.class, () -> producer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata(groupId)));
    }

    @Test
    public void shouldThrowOnCommitTransactionIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        assertThrows(ProducerFencedException.class, producer::commitTransaction);
    }

    @Test
    public void shouldThrowOnAbortTransactionIfProducerGotFenced() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.fenceProducer();
        assertThrows(ProducerFencedException.class, producer::abortTransaction);
    }

    @Test
    public void shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        producer.send(record1);
        producer.send(record2);

        assertTrue(producer.history().isEmpty());

        producer.commitTransaction();

        List<ProducerRecord<byte[], byte[]>> expectedResult = new ArrayList<>();
        expectedResult.add(record1);
        expectedResult.add(record2);

        assertEquals(expectedResult, producer.history());
    }

    @Test
    public void shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled() {
        buildMockProducer(false);
        producer.initTransactions();
        producer.beginTransaction();

        Future<RecordMetadata> md1 = producer.send(record1);
        Future<RecordMetadata> md2 = producer.send(record2);

        assertFalse(md1.isDone());
        assertFalse(md2.isDone());

        producer.commitTransaction();

        assertTrue(md1.isDone());
        assertTrue(md2.isDone());
    }

    @Test
    public void shouldDropMessagesOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();

        producer.beginTransaction();
        producer.send(record1);
        producer.send(record2);
        producer.abortTransaction();
        assertTrue(producer.history().isEmpty());

        producer.beginTransaction();
        producer.commitTransaction();
        assertTrue(producer.history().isEmpty());
    }

    @Test
    public void shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled() {
        buildMockProducer(false);
        producer.initTransactions();
        producer.beginTransaction();

        Future<RecordMetadata> md1 = producer.send(record1);
        assertFalse(md1.isDone());

        producer.abortTransaction();
        assertTrue(md1.isDone());
    }

    @Test
    public void shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();

        producer.beginTransaction();
        producer.send(record1);
        producer.send(record2);
        producer.commitTransaction();

        producer.beginTransaction();
        producer.abortTransaction();

        List<ProducerRecord<byte[], byte[]>> expectedResult = new ArrayList<>();
        expectedResult.add(record1);
        expectedResult.add(record2);

        assertEquals(expectedResult, producer.history());
    }

    @Test
    public void shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        String group1 = "g1";
        Map<TopicPartition, OffsetAndMetadata> group1Commit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(73L, null));
            }
        };
        String group2 = "g2";
        Map<TopicPartition, OffsetAndMetadata> group2Commit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(101L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(21L, null));
            }
        };
        producer.sendOffsetsToTransaction(group1Commit, group1);
        producer.sendOffsetsToTransaction(group2Commit, group2);

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty());

        Map<String, Map<TopicPartition, OffsetAndMetadata>> expectedResult = new HashMap<>();
        expectedResult.put(group1, group1Commit);
        expectedResult.put(group2, group2Commit);

        producer.commitTransaction();
        assertEquals(Collections.singletonList(expectedResult), producer.consumerGroupOffsetsHistory());
    }

    @Test
    public void shouldThrowOnNullConsumerGroupIdWhenSendOffsetsToTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        assertThrows(NullPointerException.class, () -> producer.sendOffsetsToTransaction(Collections.emptyMap(), (String) null));
    }

    @Test
    public void shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        assertThrows(NullPointerException.class, () -> producer.sendOffsetsToTransaction(Collections.emptyMap(), new ConsumerGroupMetadata(null)));
    }

    @Test
    public void shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupId() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(Collections.emptyMap(), "groupId");
        assertFalse(producer.sentOffsets());
    }

    @Test
    public void shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(Collections.emptyMap(), new ConsumerGroupMetadata("groupId"));
        assertFalse(producer.sentOffsets());
    }

    @Test
    public void shouldAddOffsetsWhenSendOffsetsToTransactionByGroupId() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        assertFalse(producer.sentOffsets());

        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, "groupId");
        assertTrue(producer.sentOffsets());
    }

    @Test
    public void shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        assertFalse(producer.sentOffsets());

        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, new ConsumerGroupMetadata("groupId"));
        assertTrue(producer.sentOffsets());
    }

    @Test
    public void shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        assertFalse(producer.sentOffsets());

        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, "groupId");
        producer.commitTransaction(); // commit should not reset "sentOffsets" flag
        assertTrue(producer.sentOffsets());

        producer.beginTransaction();
        assertFalse(producer.sentOffsets());

        producer.sendOffsetsToTransaction(groupCommit, new ConsumerGroupMetadata("groupId"));
        producer.commitTransaction(); // commit should not reset "sentOffsets" flag
        assertTrue(producer.sentOffsets());

        producer.beginTransaction();
        assertFalse(producer.sentOffsets());
    }

    @Test
    public void shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        String group = "g";
        Map<TopicPartition, OffsetAndMetadata> groupCommit1 = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(73L, null));
            }
        };
        Map<TopicPartition, OffsetAndMetadata> groupCommit2 = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(101L, null));
                put(new TopicPartition(topic, 2), new OffsetAndMetadata(21L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit1, group);
        producer.sendOffsetsToTransaction(groupCommit2, new ConsumerGroupMetadata(group));

        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty());

        Map<String, Map<TopicPartition, OffsetAndMetadata>> expectedResult = new HashMap<>();
        expectedResult.put(group, new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(101L, null));
                put(new TopicPartition(topic, 2), new OffsetAndMetadata(21L, null));
            }
        });

        producer.commitTransaction();
        assertEquals(Collections.singletonList(expectedResult), producer.consumerGroupOffsetsHistory());
    }

    @Test
    public void shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        String group = "g";
        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(73L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, group);
        producer.abortTransaction();

        producer.beginTransaction();
        producer.commitTransaction();
        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty());

        producer.beginTransaction();
        producer.sendOffsetsToTransaction(groupCommit, new ConsumerGroupMetadata(group));
        producer.abortTransaction();

        producer.beginTransaction();
        producer.commitTransaction();
        assertTrue(producer.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    public void shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        String group = "g";
        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(73L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, group);
        producer.commitTransaction();

        producer.beginTransaction();
        producer.abortTransaction();

        Map<String, Map<TopicPartition, OffsetAndMetadata>> expectedResult = new HashMap<>();
        expectedResult.put(group, groupCommit);

        assertEquals(Collections.singletonList(expectedResult), producer.consumerGroupOffsetsHistory());
    }

    @Test
    public void shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled() {
        buildMockProducer(true);
        producer.initTransactions();
        producer.beginTransaction();

        String group = "g";
        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 0), new OffsetAndMetadata(42L, null));
                put(new TopicPartition(topic, 1), new OffsetAndMetadata(73L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit, new ConsumerGroupMetadata(group));
        producer.commitTransaction();

        producer.beginTransaction();

        String group2 = "g2";
        Map<TopicPartition, OffsetAndMetadata> groupCommit2 = new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(new TopicPartition(topic, 2), new OffsetAndMetadata(53L, null));
                put(new TopicPartition(topic, 3), new OffsetAndMetadata(84L, null));
            }
        };
        producer.sendOffsetsToTransaction(groupCommit2, new ConsumerGroupMetadata(group2));
        producer.abortTransaction();

        Map<String, Map<TopicPartition, OffsetAndMetadata>> expectedResult = new HashMap<>();
        expectedResult.put(group, groupCommit);

        assertEquals(Collections.singletonList(expectedResult), producer.consumerGroupOffsetsHistory());
    }

    @Test
    public void shouldThrowOnInitTransactionIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::initTransactions);
    }

    @Test
    public void shouldThrowOnSendIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, () -> producer.send(null));
    }

    @Test
    public void shouldThrowOnBeginTransactionIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::beginTransaction);
    }

    @Test
    public void shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, () -> producer.sendOffsetsToTransaction(null, groupId));
    }

    @Test
    public void shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, () -> producer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata(groupId)));
    }

    @Test
    public void shouldThrowOnCommitTransactionIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::commitTransaction);
    }

    @Test
    public void shouldThrowOnAbortTransactionIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::abortTransaction);
    }

    @Test
    public void shouldThrowOnFenceProducerIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::fenceProducer);
    }

    @Test
    public void shouldThrowOnFlushProducerIfProducerIsClosed() {
        buildMockProducer(true);
        producer.close();
        assertThrows(IllegalStateException.class, producer::flush);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowClassCastException() {
        try (MockProducer<Integer, String> customProducer = new MockProducer<>(true, new IntegerSerializer(), new StringSerializer())) {
            assertThrows(ClassCastException.class, () -> customProducer.send(new ProducerRecord(topic, "key1", "value1")));
        }
    }

    @Test
    public void shouldBeFlushedIfNoBufferedRecords() {
        buildMockProducer(true);
        assertTrue(producer.flushed());
    }

    @Test
    public void shouldBeFlushedWithAutoCompleteIfBufferedRecords() {
        buildMockProducer(true);
        producer.send(record1);
        assertTrue(producer.flushed());
    }

    @Test
    public void shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords() {
        buildMockProducer(false);
        producer.send(record1);
        assertFalse(producer.flushed());
    }

    @Test
    public void shouldNotBeFlushedAfterFlush() {
        buildMockProducer(false);
        producer.send(record1);
        producer.flush();
        assertTrue(producer.flushed());
    }

    @Test
    public void testMetadataOnException() throws InterruptedException {
        buildMockProducer(false);
        Future<RecordMetadata> metadata = producer.send(record2, (md, exception) -> {
            assertNotNull(md);
            assertEquals(md.offset(), -1L, "Invalid offset");
            assertEquals(md.timestamp(), RecordBatch.NO_TIMESTAMP, "Invalid timestamp");
            assertEquals(md.serializedKeySize(), -1L, "Invalid Serialized Key size");
            assertEquals(md.serializedValueSize(), -1L, "Invalid Serialized value size");
        });
        IllegalArgumentException e = new IllegalArgumentException("dummy exception");
        assertTrue(producer.errorNext(e), "Complete the second request with an error");
        try {
            metadata.get();
            fail("Something went wrong, expected an error");
        } catch (ExecutionException err) {
            assertEquals(e, err.getCause());
        }
    }

    private boolean isError(Future<?> future) {
        try {
            future.get();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
