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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MockProducerTest {

    private final String topic = "topic";
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new MockSerializer(), new MockSerializer());
    private final ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, "key1".getBytes(), "value1".getBytes());
    private final ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, "key2".getBytes(), "value2".getBytes());


    @Test
    @SuppressWarnings("unchecked")
    public void testAutoCompleteMock() throws Exception {
        Future<RecordMetadata> metadata = producer.send(record1);
        assertTrue("Send should be immediately complete", metadata.isDone());
        assertFalse("Send should be successful", isError(metadata));
        assertEquals("Offset should be 0", 0L, metadata.get().offset());
        assertEquals(topic, metadata.get().topic());
        assertEquals("We should have the record in our history", singletonList(record1), producer.history());
        producer.clear();
        assertEquals("Clear should erase our history", 0, producer.history().size());
    }

    @Test
    public void testPartitioner() throws Exception {
        PartitionInfo partitionInfo0 = new PartitionInfo(topic, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(topic, 1, null, null, null);
        Cluster cluster = new Cluster(null, new ArrayList<Node>(0), asList(partitionInfo0, partitionInfo1),
                Collections.<String>emptySet(), Collections.<String>emptySet());
        MockProducer<String, String> producer = new MockProducer<>(cluster, true, new DefaultPartitioner(), new StringSerializer(), new StringSerializer());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", "value");
        Future<RecordMetadata> metadata = producer.send(record);
        assertEquals("Partition should be correct", 1, metadata.get().partition());
        producer.clear();
        assertEquals("Clear should erase our history", 0, producer.history().size());
    }

    @Test
    public void testManualCompletion() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new MockSerializer(), new MockSerializer());
        Future<RecordMetadata> md1 = producer.send(record1);
        assertFalse("Send shouldn't have completed", md1.isDone());
        Future<RecordMetadata> md2 = producer.send(record2);
        assertFalse("Send shouldn't have completed", md2.isDone());
        assertTrue("Complete the first request", producer.completeNext());
        assertFalse("Requst should be successful", isError(md1));
        assertFalse("Second request still incomplete", md2.isDone());
        IllegalArgumentException e = new IllegalArgumentException("blah");
        assertTrue("Complete the second request with an error", producer.errorNext(e));
        try {
            md2.get();
            fail("Expected error to be thrown");
        } catch (ExecutionException err) {
            assertEquals(e, err.getCause());
        }
        assertFalse("No more requests to complete", producer.completeNext());
        
        Future<RecordMetadata> md3 = producer.send(record1);
        Future<RecordMetadata> md4 = producer.send(record2);
        assertTrue("Requests should not be completed.", !md3.isDone() && !md4.isDone());
        producer.flush();
        assertTrue("Requests should be completed.", md3.isDone() && md4.isDone());
    }

    @Test
    public void shouldInitTransactions() {
        producer.initTransactions();
        assertTrue(producer.transactionInitialized());
    }

    @Test
    public void shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions() {
        producer.initTransactions();
        try {
            producer.initTransactions();
            fail("Should have thrown as producer is already initialized");
        } catch (IllegalStateException e) { }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowOnBeginTransactionIfTransactionsNotInitialized() {
        producer.beginTransaction();
    }

    @Test
    public void shouldBeginTransactions() {
        producer.initTransactions();
        producer.beginTransaction();
        assertTrue(producer.transactionInFlight());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized() {
        producer.sendOffsetsToTransaction(null, null);
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted() {
        producer.initTransactions();
        try {
            producer.sendOffsetsToTransaction(null, null);
            fail("Should have thrown as producer has no open transaction");
        } catch (IllegalStateException e) { }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowOnCommitIfTransactionsNotInitialized() {
        producer.commitTransaction();
    }

    @Test
    public void shouldThrowOnCommitTransactionIfNoTransactionGotStarted() {
        producer.initTransactions();
        try {
            producer.commitTransaction();
            fail("Should have thrown as producer has no open transaction");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldCommitEmptyTransaction() {
        producer.initTransactions();
        producer.beginTransaction();
        producer.commitTransaction();
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionAborted());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowOnAbortIfTransactionsNotInitialized() {
        producer.abortTransaction();
    }

    @Test
    public void shouldThrowOnAbortTransactionIfNoTransactionGotStarted() {
        producer.initTransactions();
        try {
            producer.abortTransaction();
            fail("Should have thrown as producer has no open transaction");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldAbortEmptyTransaction() {
        producer.initTransactions();
        producer.beginTransaction();
        producer.abortTransaction();
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
    }

    @Test
    public void shouldAbortInFlightTransactionOnClose() {
        producer.initTransactions();
        producer.beginTransaction();
        producer.close();
        assertFalse(producer.transactionInFlight());
        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowFenceProducerIfTransactionsNotInitialized() {
        producer.fenceProducer();
    }

    @Test
    public void shouldThrowOnBeginTransactionsIfProducerGotFenced() {
        producer.initTransactions();
        producer.fenceProducer();
        try {
            producer.beginTransaction();
            fail("Should have thrown as producer is fenced off");
        } catch (ProducerFencedException e) { }
    }

    @Test
    public void shouldThrowOnSendIfProducerGotFenced() {
        producer.initTransactions();
        producer.fenceProducer();
        try {
            producer.send(null);
            fail("Should have thrown as producer is fenced off");
        } catch (ProducerFencedException e) { }
    }

    @Test
    public void shouldThrowOnSendOffsetsToTransactionIfProducerGotFenced() {
        producer.initTransactions();
        producer.fenceProducer();
        try {
            producer.sendOffsetsToTransaction(null, null);
            fail("Should have thrown as producer is fenced off");
        } catch (ProducerFencedException e) { }
    }

    @Test
    public void shouldThrowOnCommitTransactionIfProducerGotFenced() {
        producer.initTransactions();
        producer.fenceProducer();
        try {
            producer.commitTransaction();
            fail("Should have thrown as producer is fenced off");
        } catch (ProducerFencedException e) { }
    }

    @Test
    public void shouldThrowOnAbortTransactionIfProducerGotFenced() {
        producer.initTransactions();
        producer.fenceProducer();
        try {
            producer.abortTransaction();
            fail("Should have thrown as producer is fenced off");
        } catch (ProducerFencedException e) { }
    }

    @Test
    public void shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled() {
        producer.initTransactions();
        producer.beginTransaction();

        producer.send(record1);
        producer.send(record2);

        assertTrue(producer.history().isEmpty());

        producer.commitTransaction();

        List<ProducerRecord<byte[], byte[]>> expectedResult = new ArrayList<>();
        expectedResult.add(record1);
        expectedResult.add(record2);

        assertThat(producer.history(), equalTo(expectedResult));
    }

    @Test
    public void shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled() {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new MockSerializer(), new MockSerializer());
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
    public void shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new MockSerializer(), new MockSerializer());
        producer.initTransactions();
        producer.beginTransaction();

        Future<RecordMetadata> md1 = producer.send(record1);
        assertFalse(md1.isDone());

        producer.abortTransaction();
        assertTrue(md1.isDone());
    }

    @Test
    public void shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled() {
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

        assertThat(producer.history(), equalTo(expectedResult));
    }

    @Test
    public void shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
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
        assertThat(producer.consumerGroupOffsetsHistory(), equalTo(Collections.singletonList(expectedResult)));
    }

    @Test
    public void shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled() {
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
        producer.sendOffsetsToTransaction(groupCommit2, group);

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
        assertThat(producer.consumerGroupOffsetsHistory(), equalTo(Collections.singletonList(expectedResult)));
    }

    @Test
    public void shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled() {
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
    }

    @Test
    public void shouldPreserveCommittedConsumerGroupsOffsetsOnAbortIfTransactionsAreEnabled() {
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

        assertThat(producer.consumerGroupOffsetsHistory(), equalTo(Collections.singletonList(expectedResult)));
    }

    @Test
    public void shouldThrowOnInitTransactionIfProducerIsClosed() {
        producer.close();
        try {
            producer.initTransactions();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnSendIfProducerIsClosed() {
        producer.close();
        try {
            producer.send(null);
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnBeginTransactionIfProducerIsClosed() {
        producer.close();
        try {
            producer.beginTransaction();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowSendOffsetsToTransactionIfProducerIsClosed() {
        producer.close();
        try {
            producer.sendOffsetsToTransaction(null, null);
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnCommitTransactionIfProducerIsClosed() {
        producer.close();
        try {
            producer.commitTransaction();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnAbortTransactionIfProducerIsClosed() {
        producer.close();
        try {
            producer.abortTransaction();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnCloseIfProducerIsClosed() {
        producer.close();
        try {
            producer.close();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
    }

    @Test
    public void shouldThrowOnFenceProducerIfProducerIsClosed() {
        producer.close();
        try {
            producer.fenceProducer();
            fail("Should have thrown as producer is already closed");
        } catch (IllegalStateException e) { }
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
