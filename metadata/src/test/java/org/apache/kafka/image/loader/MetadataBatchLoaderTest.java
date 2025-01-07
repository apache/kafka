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

package org.apache.kafka.image.loader;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.MockFaultHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataBatchLoaderTest {

    static final Uuid TOPIC_FOO = Uuid.fromString("c6uHMgPkRp2Urjlh-RxMNQ");
    static final Uuid TOPIC_BAR = Uuid.fromString("tUWOOPvzQhmZZ_eXmTCcig");
    static final List<ApiMessageAndVersion> TOPIC_TXN_BATCH_1;
    static final List<ApiMessageAndVersion> TOPIC_TXN_BATCH_2;
    static final List<ApiMessageAndVersion> TOPIC_NO_TXN_BATCH;
    static final List<ApiMessageAndVersion> TXN_BEGIN_SINGLETON;
    static final List<ApiMessageAndVersion> TXN_END_SINGLETON;
    static final List<ApiMessageAndVersion> TXN_ABORT_SINGLETON;
    static final LeaderAndEpoch LEADER_AND_EPOCH = new LeaderAndEpoch(OptionalInt.of(1), 42);

    static {
        {
            TOPIC_TXN_BATCH_1 = Arrays.asList(
                new ApiMessageAndVersion(new BeginTransactionRecord().setName("txn-1"), (short) 0),
                new ApiMessageAndVersion(new TopicRecord()
                    .setName("foo")
                    .setTopicId(TOPIC_FOO), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord()
                    .setPartitionId(0)
                    .setTopicId(TOPIC_FOO), (short) 0)
            );

            TOPIC_TXN_BATCH_2 = Arrays.asList(
                new ApiMessageAndVersion(new PartitionRecord()
                    .setPartitionId(1)
                    .setTopicId(TOPIC_FOO), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord()
                    .setPartitionId(2)
                    .setTopicId(TOPIC_FOO), (short) 0),
                new ApiMessageAndVersion(new EndTransactionRecord(), (short) 0)
            );

            TOPIC_NO_TXN_BATCH = Arrays.asList(
                new ApiMessageAndVersion(new TopicRecord()
                    .setName("bar")
                    .setTopicId(TOPIC_BAR), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord()
                    .setPartitionId(0)
                    .setTopicId(TOPIC_BAR), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord()
                    .setPartitionId(1)
                    .setTopicId(TOPIC_BAR), (short) 0)
            );

            TXN_BEGIN_SINGLETON = Collections.singletonList(
                new ApiMessageAndVersion(new BeginTransactionRecord().setName("txn-1"), (short) 0));

            TXN_END_SINGLETON = Collections.singletonList(
                new ApiMessageAndVersion(new EndTransactionRecord(), (short) 0));

            TXN_ABORT_SINGLETON = Collections.singletonList(
                new ApiMessageAndVersion(new AbortTransactionRecord(), (short) 0));
        }
    }

    static List<ApiMessageAndVersion> noOpRecords(int n) {
        return IntStream.range(0, n)
                .mapToObj(__ -> new ApiMessageAndVersion(new NoOpRecord(), (short) 0))
                .collect(Collectors.toList());
    }


    static class MockMetadataUpdater implements MetadataBatchLoader.MetadataUpdater {
        MetadataImage latestImage = null;
        MetadataDelta latestDelta = null;
        LogDeltaManifest latestManifest = null;
        int updates = 0;

        @Override
        public void update(MetadataDelta delta, MetadataImage image, LogDeltaManifest manifest) {
            latestDelta = delta;
            latestImage = image;
            latestManifest = manifest;
            updates++;
        }

        public void reset() {
            latestImage = null;
            latestDelta = null;
            latestManifest = null;
            updates = 0;
        }
    }

    @Test
    public void testAlignedTransactionBatches() {
        Batch<ApiMessageAndVersion> batch1 = Batch.data(
            10, 1, 0, 10, TOPIC_TXN_BATCH_1);
        Batch<ApiMessageAndVersion> batch2 = Batch.data(
            13, 2, 0, 10, noOpRecords(3));
        Batch<ApiMessageAndVersion> batch3 = Batch.data(
            16, 2, 0, 30, TOPIC_TXN_BATCH_2);

        MockMetadataUpdater updater = new MockMetadataUpdater();
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            new MockFaultHandler("testAlignedTransactionBatches"),
            updater
        );

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        batchLoader.loadBatch(batch3, LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);
        assertNotNull(updater.latestImage.topics().getTopic("foo"));
        assertEquals(18, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(2, updater.latestImage.provenance().lastContainedEpoch());
        assertTrue(updater.latestImage.provenance().isOffsetBatchAligned());
    }

    @Test
    public void testSingletonBeginAndEnd() {
        Batch<ApiMessageAndVersion> batch1 = Batch.data(
            13, 1, 0, 30, noOpRecords(3));

        Batch<ApiMessageAndVersion> batch2 = Batch.data(
            16, 2, 0, 30, TXN_BEGIN_SINGLETON);

        Batch<ApiMessageAndVersion> batch3 = Batch.data(
            17, 3, 0, 10, TOPIC_NO_TXN_BATCH);

        Batch<ApiMessageAndVersion> batch4 = Batch.data(
            20, 4, 0, 10, TXN_END_SINGLETON);
        MockMetadataUpdater updater = new MockMetadataUpdater();
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            new MockFaultHandler("testSingletonBeginAndEnd"),
            updater
        );

        // All in one commit
        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        // batch1 is flushed in this loadBatch call
        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        assertEquals(1, updater.updates);
        assertTrue(updater.latestImage.provenance().isOffsetBatchAligned());
        assertNull(updater.latestImage.topics().getTopic("bar"));
        batchLoader.loadBatch(batch3, LEADER_AND_EPOCH);
        assertEquals(1, updater.updates);
        batchLoader.loadBatch(batch4, LEADER_AND_EPOCH);
        assertEquals(1, updater.updates);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertNotNull(updater.latestImage.topics().getTopic("bar"));
        assertEquals(20, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(4, updater.latestImage.provenance().lastContainedEpoch());

        // Each batch in a separate commit
        updater.reset();
        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);

        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);

        batchLoader.loadBatch(batch3, LEADER_AND_EPOCH);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);

        batchLoader.loadBatch(batch4, LEADER_AND_EPOCH);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(2, updater.updates);
    }

    @Test
    public void testUnexpectedBeginTransaction() {
        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testUnexpectedBeginTransaction");
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            faultHandler,
            updater
        );

        Batch<ApiMessageAndVersion> batch1 = Batch.data(
            10, 2, 0, 30, TOPIC_TXN_BATCH_1);

        Batch<ApiMessageAndVersion> batch2 = Batch.data(
            13, 2, 0, 30, TXN_BEGIN_SINGLETON);

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        assertNull(faultHandler.firstException());
        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        assertEquals(RuntimeException.class, faultHandler.firstException().getCause().getClass());
        assertEquals(
            "Encountered BeginTransactionRecord while already in a transaction",
            faultHandler.firstException().getCause().getMessage()
        );
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(0, updater.updates);
    }

    @Test
    public void testUnexpectedEndTransaction() {
        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testUnexpectedAbortTransaction");
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
                new LogContext(),
                new MockTime(),
                faultHandler,
                updater
        );

        // First batch gets loaded fine
        Batch<ApiMessageAndVersion> batch1 = Batch.data(
            10, 2, 0, 30, TOPIC_NO_TXN_BATCH);

        // Second batch throws an error, but shouldn't interfere with prior batches
        Batch<ApiMessageAndVersion> batch2 = Batch.data(
            13, 2, 0, 30, TXN_END_SINGLETON);

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        assertNull(faultHandler.firstException());
        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        assertEquals(RuntimeException.class, faultHandler.firstException().getCause().getClass());
        assertEquals(
            "Encountered EndTransactionRecord without having seen a BeginTransactionRecord",
            faultHandler.firstException().getCause().getMessage()
        );
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);
        assertNotNull(updater.latestImage.topics().getTopic("bar"));
    }

    @Test
    public void testUnexpectedAbortTransaction() {
        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testUnexpectedAbortTransaction");
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            faultHandler,
            updater
        );

        // First batch gets loaded fine
        Batch<ApiMessageAndVersion> batch1 = Batch.data(
            10, 2, 0, 30, TOPIC_NO_TXN_BATCH);

        // Second batch throws an error, but shouldn't interfere with prior batches
        Batch<ApiMessageAndVersion> batch2 = Batch.data(
            13, 2, 0, 30, TXN_ABORT_SINGLETON);

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch1, LEADER_AND_EPOCH);
        assertNull(faultHandler.firstException());
        batchLoader.loadBatch(batch2, LEADER_AND_EPOCH);
        assertEquals(RuntimeException.class, faultHandler.firstException().getCause().getClass());
        assertEquals(
            "Encountered AbortTransactionRecord without having seen a BeginTransactionRecord",
            faultHandler.firstException().getCause().getMessage()
        );
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(1, updater.updates);
        assertNotNull(updater.latestImage.topics().getTopic("bar"));
    }

    private MetadataBatchLoader loadSingleBatch(
        MockMetadataUpdater updater,
        MockFaultHandler faultHandler,
        List<ApiMessageAndVersion> batchRecords
    ) {
        Batch<ApiMessageAndVersion> batch = Batch.data(
            10, 42, 0, 100, batchRecords);

        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            faultHandler,
            updater
        );

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(batch, LEADER_AND_EPOCH);
        return batchLoader;
    }

    @Test
    public void testMultipleTransactionsInOneBatch() {
        List<ApiMessageAndVersion> batchRecords = new ArrayList<>();
        batchRecords.addAll(TOPIC_TXN_BATCH_1);
        batchRecords.addAll(TOPIC_TXN_BATCH_2);
        batchRecords.addAll(TXN_BEGIN_SINGLETON);
        batchRecords.addAll(TOPIC_NO_TXN_BATCH);
        batchRecords.addAll(TXN_END_SINGLETON);

        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testMultipleTransactionsInOneBatch");
        MetadataBatchLoader batchLoader = loadSingleBatch(updater, faultHandler, batchRecords);

        assertEquals(1, updater.updates);
        assertEquals(0, updater.latestManifest.numBytes());
        assertEquals(15, updater.latestImage.provenance().lastContainedOffset());
        // The first transaction is flushed in the middle of the batch, the offset flushed is not batch-aligned
        assertFalse(updater.latestImage.provenance().isOffsetBatchAligned());
        assertEquals(42, updater.latestImage.provenance().lastContainedEpoch());

        assertNotNull(updater.latestImage.topics().getTopic("foo"));
        assertNull(updater.latestImage.topics().getTopic("bar"));
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(2, updater.updates);
        assertEquals(100, updater.latestManifest.numBytes());
        assertEquals(20, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(42, updater.latestImage.provenance().lastContainedEpoch());
        assertTrue(updater.latestImage.provenance().isOffsetBatchAligned());
        assertNotNull(updater.latestImage.topics().getTopic("foo"));
        assertNotNull(updater.latestImage.topics().getTopic("bar"));
    }

    @Test
    public void testMultipleTransactionsInOneBatchesWithNoOp() {
        List<ApiMessageAndVersion> batchRecords = new ArrayList<>();
        batchRecords.addAll(noOpRecords(1));
        batchRecords.addAll(TOPIC_TXN_BATCH_1);
        batchRecords.addAll(noOpRecords(1));
        batchRecords.addAll(TOPIC_TXN_BATCH_2);
        // A batch with non-transactional records between two transactions causes a delta to get published
        batchRecords.addAll(noOpRecords(1));
        batchRecords.addAll(TXN_BEGIN_SINGLETON);
        batchRecords.addAll(noOpRecords(1));
        batchRecords.addAll(TOPIC_NO_TXN_BATCH);
        batchRecords.addAll(noOpRecords(1));
        batchRecords.addAll(TXN_END_SINGLETON);
        batchRecords.addAll(noOpRecords(1));

        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testMultipleTransactionsInOneBatches");
        MetadataBatchLoader batchLoader = loadSingleBatch(updater, faultHandler, batchRecords);

        assertEquals(2, updater.updates);
        assertEquals(0, updater.latestManifest.numBytes());
        assertEquals(18, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(42, updater.latestImage.provenance().lastContainedEpoch());
        assertFalse(updater.latestImage.provenance().isOffsetBatchAligned());
        assertNotNull(updater.latestImage.topics().getTopic("foo"));
        assertNull(updater.latestImage.topics().getTopic("bar"));
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);
        assertEquals(3, updater.updates);
        assertEquals(100, updater.latestManifest.numBytes());
        assertEquals(26, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(42, updater.latestImage.provenance().lastContainedEpoch());
        assertNotNull(updater.latestImage.topics().getTopic("foo"));
        assertNotNull(updater.latestImage.topics().getTopic("bar"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOneTransactionInMultipleBatches(boolean abortTxn) {
        MockMetadataUpdater updater = new MockMetadataUpdater();
        MetadataBatchLoader batchLoader = new MetadataBatchLoader(
            new LogContext(),
            new MockTime(),
            new MockFaultHandler("testOneTransactionInMultipleBatches"),
            updater
        );

        batchLoader.resetToImage(MetadataImage.EMPTY);
        batchLoader.loadBatch(Batch.data(
            16, 2, 0, 10, TXN_BEGIN_SINGLETON), LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        batchLoader.loadBatch(Batch.data(
            17, 3, 0, 30, TOPIC_NO_TXN_BATCH), LEADER_AND_EPOCH);
        assertEquals(0, updater.updates);
        if (abortTxn) {
            batchLoader.loadBatch(Batch.data(
                20, 4, 0, 10, TXN_ABORT_SINGLETON), LEADER_AND_EPOCH);
        } else {
            batchLoader.loadBatch(Batch.data(
                20, 4, 0, 10, TXN_END_SINGLETON), LEADER_AND_EPOCH);
        }
        assertEquals(0, updater.updates);
        batchLoader.maybeFlushBatches(LEADER_AND_EPOCH, true);

        // Regardless of end/abort, we should publish an updated MetadataProvenance and manifest
        assertEquals(50, updater.latestManifest.numBytes());
        assertEquals(3, updater.latestManifest.numBatches());
        assertEquals(20, updater.latestImage.provenance().lastContainedOffset());
        assertEquals(4, updater.latestImage.provenance().lastContainedEpoch());
        if (abortTxn) {
            assertNull(updater.latestImage.topics().getTopic("bar"));
        } else {
            assertNotNull(updater.latestImage.topics().getTopic("bar"));
        }
    }

    @Test
    public void testTransactionAlignmentOnBatchBoundary() {
        List<ApiMessageAndVersion> batchRecords = new ArrayList<>();
        batchRecords.addAll(noOpRecords(3));
        batchRecords.addAll(TOPIC_TXN_BATCH_1);
        batchRecords.addAll(TOPIC_TXN_BATCH_2);
        batchRecords.addAll(noOpRecords(3));

        MockMetadataUpdater updater = new MockMetadataUpdater();
        MockFaultHandler faultHandler = new MockFaultHandler("testMultipleTransactionsInOneBatch");
        MetadataBatchLoader batchLoader = loadSingleBatch(updater, faultHandler, batchRecords);

        assertEquals(1, updater.updates);
        assertEquals(0, updater.latestManifest.numBytes());
        assertEquals(12, updater.latestImage.provenance().lastContainedOffset());
        assertFalse(updater.latestImage.provenance().isOffsetBatchAligned());

        batchLoader.loadBatch(Batch.data(
                22, 42, 0, 10, TXN_BEGIN_SINGLETON), LEADER_AND_EPOCH);
        assertEquals(2, updater.updates);
        assertEquals(100, updater.latestManifest.numBytes());
        assertEquals(21, updater.latestImage.provenance().lastContainedOffset());
        assertTrue(updater.latestImage.provenance().isOffsetBatchAligned());
    }
}
