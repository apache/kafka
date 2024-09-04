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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT;
import static org.apache.kafka.storage.internals.log.ProducerStateManager.LATE_TRANSACTION_BUFFER_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProducerStateManagerTest {

    private final File logDir;
    private final ProducerStateManager stateManager;
    private final TopicPartition partition;
    private final ProducerStateManagerConfig producerStateManagerConfig;
    private final MockTime time;

    private final long producerId = 1;
    private final short epoch = 0;
    private final int defaultSequence = 0;
    private final int maxTransactionTimeoutMs = 5 * 60 * 1000;
    private final long lateTransactionTimeoutMs = maxTransactionTimeoutMs + LATE_TRANSACTION_BUFFER_MS;

    public ProducerStateManagerTest() throws IOException {
        logDir = TestUtils.tempDirectory();
        partition = new TopicPartition("test", 0);
        producerStateManagerConfig = new ProducerStateManagerConfig(PRODUCER_ID_EXPIRATION_MS_DEFAULT, true);
        time = new MockTime();
        stateManager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs,
                producerStateManagerConfig, time);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(logDir);
    }

    @Test
    public void testBasicIdMapping() {
        // First entry for id 0 added
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0L, false);
        // Second entry for id 0 added
        appendClientEntry(stateManager, producerId, epoch, 1, 0L, 1L, false);

        // Duplicates are checked separately and should result in OutOfOrderSequence if appended
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, 1, 0L, 1L, false));
        // Invalid sequence number (greater than next expected sequence number)
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, 5, 0L, 2L, false));

        // Change epoch
        appendClientEntry(stateManager, producerId, (short) (epoch + 1), defaultSequence, 0L, 3L, false);
        // Incorrect epoch
        assertThrows(InvalidProducerEpochException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 4L, false));
    }

    @Test
    public void testAppendTxnMarkerWithNoProducerState() {
        short producerEpoch = 2;

        appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, 27L);
        ProducerStateEntry firstEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(producerEpoch, firstEntry.producerEpoch());
        assertEquals(producerId, firstEntry.producerId());
        assertEquals(RecordBatch.NO_SEQUENCE, firstEntry.lastSeq());

        // Fencing should continue to work even if the marker is the only thing left
        assertThrows(InvalidProducerEpochException.class,
                () -> appendClientEntry(stateManager, producerId, (short) 0, defaultSequence, 0L, 4L, false));
        // If the transaction marker is the only thing left in the log, then an attempt to write using a
        // non-zero sequence number should cause an OutOfOrderSequenceException, so that the producer can reset its state
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, producerEpoch, 17, 0L, 4L, false));

        // The broker should accept the request if the sequence number is reset to 0
        appendClientEntry(stateManager, producerId, producerEpoch, defaultSequence, 39L, 4L, false);

        ProducerStateEntry secondEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);
        assertEquals(producerEpoch, secondEntry.producerEpoch());
        assertEquals(producerId, secondEntry.producerId());
        assertEquals(0, secondEntry.lastSeq());
    }

    @Test
    public void testProducerSequenceWrapAround() {
        short epoch = 15;
        int sequence = Integer.MAX_VALUE;
        long offset = 735L;
        
        appendReplicationEntry(stateManager, epoch, sequence, offset);
        appendClientEntry(stateManager, producerId, epoch, 0, offset + 500, false);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(epoch, lastEntry.producerEpoch());
        assertEquals(sequence, lastEntry.firstSeq());
        assertEquals(0, lastEntry.lastSeq());
    }

    @Test
    public void testProducerSequenceWithWrapAroundBatchRecord() {
        short epoch = 15;
        int firstSequence = Integer.MAX_VALUE - 10;
        int lastSequence = 9;
        long firstOffset = 2000L;
        long lastOffset = 2020L;

        ProducerAppendInfo appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION);
        // Sequence number wrap around
        appendInfo.appendDataBatch(epoch, firstSequence, lastSequence, time.milliseconds(),
                new LogOffsetMetadata(firstOffset), lastOffset, false);
        assertEquals(Optional.empty(), stateManager.lastEntry(producerId));
        stateManager.update(appendInfo);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(firstSequence, lastEntry.firstSeq());
        assertEquals(lastSequence, lastEntry.lastSeq());
        assertEquals(firstOffset, lastEntry.firstDataOffset());
        assertEquals(lastOffset, lastEntry.lastDataOffset());
    }

    @Test
    public void testProducerSequenceInvalidWrapAround() {
        short epoch = 15;
        int sequence = Integer.MAX_VALUE;
        long offset = 735L;

        appendReplicationEntry(stateManager, epoch, sequence, offset);
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, 1, offset + 500, false));
    }

    @Test
    public void testNoValidationOnFirstEntryWhenLoadingLog() {
        short epoch = 5;
        int sequence = 16;
        long offset = 735L;
        appendReplicationEntry(stateManager, epoch, sequence, offset);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(epoch, lastEntry.producerEpoch());
        assertEquals(sequence, lastEntry.firstSeq());
        assertEquals(sequence, lastEntry.lastSeq());
        assertEquals(offset, lastEntry.lastDataOffset());
        assertEquals(offset, lastEntry.firstDataOffset());
    }

    @Test
    public void testControlRecordBumpsProducerEpoch() {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, false);

        short bumpedProducerEpoch = 1;
        appendEndTxnMarker(stateManager, producerId, bumpedProducerEpoch, ControlRecordType.ABORT, 1L);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(bumpedProducerEpoch, lastEntry.producerEpoch());
        assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset());
        assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq());
        assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq());

        // should be able to append with the new epoch if we start at sequence 0
        appendClientEntry(stateManager, producerId, bumpedProducerEpoch, 0, 2L, false);
        assertEquals(defaultSequence, getLastEntryOrElseThrownByProducerId(stateManager, producerId).firstSeq());
    }

    @Test
    public void testTxnFirstOffsetMetadataCached() {
        long offset = 992342L;
        ProducerAppendInfo appendInfo = new ProducerAppendInfo(partition, producerId,
                ProducerStateEntry.empty(producerId), AppendOrigin.CLIENT,
                stateManager.maybeCreateVerificationStateEntry(producerId, defaultSequence, epoch));

        LogOffsetMetadata firstOffsetMetadata = new LogOffsetMetadata(offset, 990000L, 234224);
        appendInfo.appendDataBatch(epoch, defaultSequence, defaultSequence, 
                time.milliseconds(), firstOffsetMetadata, offset, true);
        stateManager.update(appendInfo);

        assertEquals(Optional.of(firstOffsetMetadata), stateManager.firstUnstableOffset());
    }

    @Test
    public void testHasLateTransaction() {
        long producerId1 = 39L;
        short epoch1 = 2;
        long producerId2 = 57L;
        short epoch2 = 9;

        // Start two transactions with a delay between them
        appendClientEntry(stateManager, producerId1, epoch1, defaultSequence, 100, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));

        time.sleep(500);
        appendClientEntry(stateManager, producerId2, epoch2, defaultSequence, 150, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));

        // Only the first transaction is late
        time.sleep(lateTransactionTimeoutMs - 500 + 1);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // Both transactions are now late
        time.sleep(500);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // Finish the first transaction
        appendEndTxnMarker(stateManager, producerId1, epoch1, ControlRecordType.COMMIT, 200);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // Now finish the second transaction
        appendEndTxnMarker(stateManager, producerId2, epoch2, ControlRecordType.COMMIT, 250);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));
    }

    @Test
    public void testHasLateTransactionInitializedAfterReload() throws IOException {
        long producerId1 = 39L;
        short epoch1 = 2;
        long producerId2 = 57L;
        short epoch2 = 9;

        // Start two transactions with a delay between them
        appendClientEntry(stateManager, producerId1, epoch1, defaultSequence, 100, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));

        time.sleep(500);
        appendClientEntry(stateManager, producerId2, epoch2, defaultSequence, 150, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));

        // Take a snapshot and reload the state
        stateManager.takeSnapshot();
        time.sleep(lateTransactionTimeoutMs - 500 + 1);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // After reloading from the snapshot, the transaction should still be considered late
        ProducerStateManager reloadedStateManager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs,
                producerStateManagerConfig, time);
        reloadedStateManager.truncateAndReload(0L, stateManager.mapEndOffset(), time.milliseconds());
        assertTrue(reloadedStateManager.hasLateTransaction(time.milliseconds()));
    }

    @Test
    public void testHasLateTransactionUpdatedAfterPartialTruncation() throws IOException {
        long producerId = 39L;
        short epoch = 2;

        // Start one transaction and sleep until it is late
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 100, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));
        time.sleep(lateTransactionTimeoutMs + 1);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // After truncation, the ongoing transaction will be cleared
        stateManager.truncateAndReload(0, 80, time.milliseconds());
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));
    }

    @Test
    public void testHasLateTransactionUpdatedAfterFullTruncation() throws IOException {
        long producerId = 39L;
        short epoch = 2;

        // Start one transaction and sleep until it is late
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 100, true);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));
        time.sleep(lateTransactionTimeoutMs + 1);
        assertTrue(stateManager.hasLateTransaction(time.milliseconds()));

        // After truncation, the ongoing transaction will be cleared
        stateManager.truncateFullyAndStartAt(150L);
        assertFalse(stateManager.hasLateTransaction(time.milliseconds()));
    }

    @Test
    public void testPrepareUpdateDoesNotMutate() {
        ProducerAppendInfo appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        appendInfo.appendDataBatch(epoch, 0, 5, time.milliseconds(),
                new LogOffsetMetadata(15L), 20L, false);
        assertEquals(Optional.empty(), stateManager.lastEntry(producerId));
        stateManager.update(appendInfo);

        getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        ProducerAppendInfo nextAppendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        nextAppendInfo.appendDataBatch(epoch, 6, 10, time.milliseconds(),
                new LogOffsetMetadata(26L), 30L, false);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);

        assertEquals(0, lastEntry.firstSeq());
        assertEquals(5, lastEntry.lastSeq());
        assertEquals(20L, lastEntry.lastDataOffset());

        stateManager.update(nextAppendInfo);
        lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);
        assertEquals(0, lastEntry.firstSeq());
        assertEquals(10, lastEntry.lastSeq());
        assertEquals(30L, lastEntry.lastDataOffset());
    }

    @Test
    public void updateProducerTransactionState() {
        int coordinatorEpoch = 15;
        long offset = 9L;
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, offset, false);

        ProducerAppendInfo appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        appendInfo.appendDataBatch(epoch, 1, 5, time.milliseconds(),
                new LogOffsetMetadata(16L), 20L, true);
        verifyLastEntryWithTxnData(appendInfo.toEntry(), 1, 5,
                16L, 20L, OptionalLong.of(16), appendInfo);

        appendInfo.appendDataBatch(epoch, 6, 10, time.milliseconds(),
                new LogOffsetMetadata(26L), 30L, true);
        verifyLastEntryWithTxnData(appendInfo.toEntry(), 1, 10,
                16L, 30L, OptionalLong.of(16), appendInfo);

        EndTransactionMarker endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        CompletedTxn completedTxn = appendInfo.appendEndTxnMarker(endTxnMarker, epoch, 40L, time.milliseconds())
                .orElseThrow(() -> new RuntimeException("The transaction should be completed"));

        assertEquals(producerId, completedTxn.producerId);
        assertEquals(16L, completedTxn.firstOffset);
        assertEquals(40L, completedTxn.lastOffset);
        assertFalse(completedTxn.isAborted);

        ProducerStateEntry lastEntry = appendInfo.toEntry();
        // verify that appending the transaction marker doesn't affect the metadata of the cached record batches.
        verifyLastEntryWithTxnData(lastEntry, 1, 10,
                16L, 30L, OptionalLong.empty(), appendInfo);
        assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset());
    }

    @Test
    public void testOutOfSequenceAfterControlRecordEpochBump() {

        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, true);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, true);

        short bumpedEpoch = 1;
        appendEndTxnMarker(stateManager, producerId, bumpedEpoch, ControlRecordType.ABORT, 1L);

        // next append is invalid since we expect the sequence to be reset
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, bumpedEpoch, 2, 2L, true));

        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, (short) (bumpedEpoch + 1), 2, 2L, true));

        // Append with the bumped epoch should be fine if starting from sequence 0
        appendClientEntry(stateManager, producerId, bumpedEpoch, defaultSequence, 0L, true);
        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);
        assertEquals(bumpedEpoch, lastEntry.producerEpoch());
        assertEquals(0, lastEntry.lastSeq());
    }

    @Test
    public void testNonTransactionalAppendWithOngoingTransaction() {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, true);
        assertThrows(InvalidTxnStateException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, 1, 1L, false));
    }

    @Test
    public void testTruncateAndReloadRemovesOutOfRangeSnapshots() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, false);
        stateManager.takeSnapshot();
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, false);
        stateManager.takeSnapshot();
        appendClientEntry(stateManager, producerId, epoch, 2, 2L, false);
        stateManager.takeSnapshot();
        appendClientEntry(stateManager, producerId, epoch, 3, 3L, false);
        stateManager.takeSnapshot();
        appendClientEntry(stateManager, producerId, epoch, 4, 4L, false);
        stateManager.takeSnapshot();

        stateManager.truncateAndReload(1L, 3L, time.milliseconds());

        assertEquals(OptionalLong.of(2L), stateManager.oldestSnapshotOffset());
        assertEquals(OptionalLong.of(3L), stateManager.latestSnapshotOffset());
    }

    @Test
    public void testTakeSnapshot() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0L, false);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, 1L, false);

        // Take snapshot
        stateManager.takeSnapshot();

        // Check that file exists and it is not empty
        String[] logDirs = Objects.requireNonNull(logDir.list());
        assertEquals(1, logDirs.length, "Directory doesn't contain a single file as expected");
        assertFalse(logDirs[0].isEmpty(), "Snapshot file is empty");
    }

    @Test
    public void testFetchSnapshotEmptySnapShot() {
        int offset = 1;
        assertEquals(Optional.empty(), stateManager.fetchSnapshot(offset));
    }

    @Test
    public void testRecoverFromSnapshotUnfinishedTransaction() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, 0, 0L, true);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, true);

        stateManager.takeSnapshot();
        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds());

        // The snapshot only persists the last appended batch metadata
        ProducerStateEntry loadedEntry = getLastEntryOrElseThrownByProducerId(recoveredMapping, producerId);
        assertEquals(1, loadedEntry.firstDataOffset());
        assertEquals(1, loadedEntry.firstSeq());
        assertEquals(1, loadedEntry.lastDataOffset());
        assertEquals(1, loadedEntry.lastSeq());
        assertEquals(OptionalLong.of(0), loadedEntry.currentTxnFirstOffset());

        // entry added after recovery
        assertDoesNotThrow(() -> appendClientEntry(recoveredMapping, producerId, epoch, 2, 2L, true));
    }

    @Test
    public void testRecoverFromSnapshotFinishedTransaction() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, 0, 0L, true);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, true);
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.ABORT, 2L);

        stateManager.takeSnapshot();
        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds());

        // The snapshot only persists the last appended batch metadata
        ProducerStateEntry loadedEntry = getLastEntryOrElseThrownByProducerId(recoveredMapping, producerId);
        assertEquals(1, loadedEntry.firstDataOffset());
        assertEquals(1, loadedEntry.firstSeq());
        assertEquals(1, loadedEntry.lastDataOffset());
        assertEquals(1, loadedEntry.lastSeq());
        assertEquals(OptionalLong.empty(), loadedEntry.currentTxnFirstOffset());
    }

    @Test
    public void testRecoverFromSnapshotEmptyTransaction() throws IOException {
        long appendTimestamp = time.milliseconds();
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.ABORT, 0L, 0, appendTimestamp);
        stateManager.takeSnapshot();

        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 1L, time.milliseconds());

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(recoveredMapping, producerId);
        assertEquals(appendTimestamp, lastEntry.lastTimestamp());
        assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset());
    }

    @Test
    public void testProducerStateAfterFencingAbortMarker() {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, true);
        appendEndTxnMarker(stateManager, producerId, (short) (epoch + 1), ControlRecordType.ABORT, 1L);

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);
        assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset());
        assertEquals(-1, lastEntry.lastDataOffset());
        assertEquals(-1, lastEntry.firstDataOffset());

        // The producer should not be expired because we want to preserve fencing epochs
        stateManager.removeExpiredProducers(time.milliseconds());
        assertDoesNotThrow(() -> getLastEntryOrElseThrownByProducerId(stateManager, producerId));
    }

    @Test
    public void testRemoveExpiredPidsOnReload() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0, false);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, 1, false);

        stateManager.takeSnapshot();
        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 1L, 70000);

        // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence,
        // we should accept the append and add the pid back in
        appendClientEntry(recoveredMapping, producerId, epoch, 2, 2L, 70001, false);

        assertEquals(1, recoveredMapping.activeProducers().size());
        assertEquals(2, recoveredMapping.activeProducers().values().iterator().next().lastSeq());
        assertEquals(3L, recoveredMapping.mapEndOffset());
    }

    @Test
    public void testAcceptAppendWithoutProducerStateOnReplica() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0, false);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, 1, false);

        stateManager.takeSnapshot();
        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 1L, 70000);

        int sequence = 2;
        // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Nonetheless,
        // to append on a replica should be accepted with the local producer state updated to the appended value.
        assertFalse(recoveredMapping.activeProducers().containsKey(producerId));
        appendReplicationEntry(recoveredMapping, epoch, sequence, 2L, 70001);
        assertTrue(recoveredMapping.activeProducers().containsKey(producerId));
        ProducerStateEntry producerStateEntry = recoveredMapping.activeProducers().get(producerId);
        assertEquals(epoch, producerStateEntry.producerEpoch());
        assertEquals(sequence, producerStateEntry.firstSeq());
        assertEquals(sequence, producerStateEntry.lastSeq());
    }

    @Test
    public void testAcceptAppendWithSequenceGapsOnReplica() {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0, false);
        int outOfOrderSequence = 3;

        // First we ensure that we raise an OutOfOrderSequenceException is raised when to append comes from a client.
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, outOfOrderSequence, 1L, 1, false));
        assertTrue(stateManager.activeProducers().containsKey(producerId));
        ProducerStateEntry producerStateEntry = stateManager.activeProducers().get(producerId);
        assertNotNull(producerStateEntry);
        assertEquals(0L, producerStateEntry.lastSeq());

        appendReplicationEntry(stateManager, epoch, outOfOrderSequence, 1L, 1);
        ProducerStateEntry producerStateEntryForReplication = stateManager.activeProducers().get(producerId);
        assertNotNull(producerStateEntryForReplication);
        assertEquals(outOfOrderSequence, producerStateEntryForReplication.lastSeq());
    }

    @Test
    public void testDeleteSnapshotsBefore() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, false);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, false);
        stateManager.takeSnapshot();
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(2L), currentSnapshotOffsets());

        appendClientEntry(stateManager, producerId, epoch, 2, 2L, false);
        stateManager.takeSnapshot();
        assertEquals(2, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(new HashSet<>(asList(2L, 3L)), currentSnapshotOffsets());

        stateManager.deleteSnapshotsBefore(3L);
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(3L), currentSnapshotOffsets());

        stateManager.deleteSnapshotsBefore(4L);
        assertEquals(0, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(emptySet(), currentSnapshotOffsets());
    }

    @Test
    public void testTruncateFullyAndStartAt() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, false);
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, false);
        stateManager.takeSnapshot();
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(2L), currentSnapshotOffsets());

        appendClientEntry(stateManager, producerId, epoch, 2, 2L, false);
        stateManager.takeSnapshot();
        assertEquals(2, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(new HashSet<>(asList(2L, 3L)), currentSnapshotOffsets());

        stateManager.truncateFullyAndStartAt(0L);

        assertEquals(0, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(emptySet(), currentSnapshotOffsets());

        appendClientEntry(stateManager, producerId, epoch, 0, 0L, false);
        stateManager.takeSnapshot();
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(1L), currentSnapshotOffsets());
    }

    @Test
    public void testReloadSnapshots() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, 1, 1L, false);
        appendClientEntry(stateManager, producerId, epoch, 2, 2L, false);
        stateManager.takeSnapshot();
        Map<Path, byte[]> pathAndDataList = Arrays.stream(Objects.requireNonNull(logDir.listFiles()))
                .map(File::toPath)
                .collect(Collectors.toMap(path -> path, path -> assertDoesNotThrow(() -> Files.readAllBytes(path))));

        appendClientEntry(stateManager, producerId, epoch, 3, 3L, false);
        appendClientEntry(stateManager, producerId, epoch, 4, 4L, false);
        stateManager.takeSnapshot();
        assertEquals(2, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(new HashSet<>(asList(3L, 5L)), currentSnapshotOffsets());

        // Truncate to the range (3, 5), this will delete the earlier snapshot until offset 3.
        stateManager.truncateAndReload(3, 5, time.milliseconds());
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(5L), currentSnapshotOffsets());

        // Add the snapshot files until offset 3 to the log dir.
        pathAndDataList.forEach((path, data) -> assertDoesNotThrow(() -> Files.write(path, data)));
        // Cleanup the in-memory snapshots and reload the snapshots from log dir.
        // It loads the earlier written snapshot files from log dir.
        stateManager.truncateFullyAndReloadSnapshots();

        assertEquals(OptionalLong.of(3), stateManager.latestSnapshotOffset());
        assertEquals(singleton(3L), currentSnapshotOffsets());
    }

    @Test
    public void testFirstUnstableOffsetAfterTruncation() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        assertEquals(99L, stateManager.firstUnstableOffset()
                .map(offset -> offset.messageOffset)
                .orElseThrow(() -> new RuntimeException("First unstable offset should be present")));
        stateManager.takeSnapshot();

        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 105);
        stateManager.onHighWatermarkUpdated(106);
        assertEquals(Optional.empty(), stateManager.firstUnstableOffset().map(offset -> offset.messageOffset));
        stateManager.takeSnapshot();

        appendClientEntry(stateManager, producerId, epoch, defaultSequence + 1, 106, false);
        stateManager.truncateAndReload(0L, 106, time.milliseconds());
        assertEquals(Optional.empty(), stateManager.firstUnstableOffset().map(offset -> offset.messageOffset));

        stateManager.truncateAndReload(0L, 100L, time.milliseconds());
        assertEquals(99L, stateManager.firstUnstableOffset()
                .map(offset -> offset.messageOffset)
                .orElseThrow(() -> new RuntimeException("First unstable offset should be present")));
    }

    @Test
    public void testLoadFromSnapshotRetainsNonExpiredProducers() throws IOException {
        long pid1 = 1L;
        long pid2 = 2L;

        appendClientEntry(stateManager, pid1, epoch, defaultSequence, 0L, false);
        appendClientEntry(stateManager, pid2, epoch, defaultSequence, 1L, false);
        stateManager.takeSnapshot();
        assertEquals(2, stateManager.activeProducers().size());

        stateManager.truncateAndReload(1L, 2L, time.milliseconds());
        assertEquals(2, stateManager.activeProducers().size());

        ProducerStateEntry entry1 = getLastEntryOrElseThrownByProducerId(stateManager, pid1);
        assertEquals(0, entry1.lastSeq());
        assertEquals(0L, entry1.lastDataOffset());

        ProducerStateEntry entry2 = getLastEntryOrElseThrownByProducerId(stateManager, pid2);
        assertEquals(0, entry2.lastSeq());
        assertEquals(1L, entry2.lastDataOffset());
    }

    @Test
    public void testSkipSnapshotIfOffsetUnchanged() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 0L, 0L, false);

        stateManager.takeSnapshot();
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(1L), currentSnapshotOffsets());

        // nothing changed so there should be no new snapshot
        stateManager.takeSnapshot();
        assertEquals(1, Objects.requireNonNull(logDir.listFiles()).length);
        assertEquals(singleton(1L), currentSnapshotOffsets());
    }

    @Test
    public void testPidExpirationTimeout() {
        short epoch = 5;
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 1L, false);
        time.sleep(producerStateManagerConfig.producerIdExpirationMs() + 1);
        stateManager.removeExpiredProducers(time.milliseconds());
        appendClientEntry(stateManager, producerId, epoch, defaultSequence + 1, 2L, false);
        assertEquals(1, stateManager.activeProducers().size());
        assertEquals(defaultSequence + 1, stateManager.activeProducers().values().iterator().next().lastSeq());
        assertEquals(3L, stateManager.mapEndOffset());
    }

    @Test
    public void testFirstUnstableOffset() {
        short epoch = 5;

        assertEquals(OptionalLong.empty(), stateManager.firstUndecidedOffset());

        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset());
        assertEquals(Optional.of(99L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        long anotherPid = 2L;
        appendClientEntry(stateManager, anotherPid, epoch, defaultSequence, 105, true);
        assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset());
        assertEquals(Optional.of(99L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 109);
        assertEquals(OptionalLong.of(105L), stateManager.firstUndecidedOffset());
        assertEquals(Optional.of(99L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        stateManager.onHighWatermarkUpdated(100L);
        assertEquals(Optional.of(99L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        stateManager.onHighWatermarkUpdated(110L);
        assertEquals(Optional.of(105L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        appendEndTxnMarker(stateManager, anotherPid, epoch, ControlRecordType.ABORT, 112);
        assertFalse(stateManager.firstUndecidedOffset().isPresent());
        assertEquals(Optional.of(105L), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        stateManager.onHighWatermarkUpdated(113L);
        assertFalse(stateManager.firstUnstableOffset().map(x -> x.messageOffset).isPresent());
    }

    @Test
    public void testProducersWithOngoingTransactionsDontExpire() {
        short epoch = 5;
        
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset());

        time.sleep(producerStateManagerConfig.producerIdExpirationMs() + 1);
        stateManager.removeExpiredProducers(time.milliseconds());

        assertTrue(stateManager.lastEntry(producerId).isPresent());
        assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset());

        stateManager.removeExpiredProducers(time.milliseconds());
        assertTrue(stateManager.lastEntry(producerId).isPresent());
    }

    @Test
    public void testSequenceNotValidatedForGroupMetadataTopic() throws IOException {
        TopicPartition partition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);
        ProducerStateManager stateManager = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);

        appendEntry(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, 99,
                time.milliseconds(), AppendOrigin.COORDINATOR, true);
        appendEntry(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, 100,
                time.milliseconds(), AppendOrigin.COORDINATOR, true);
    }

    @Test
    public void testOldEpochForControlRecord() {
        short epoch = 5;

        assertFalse(stateManager.firstUndecidedOffset().isPresent());

        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        assertThrows(InvalidProducerEpochException.class,
                () -> appendEndTxnMarker(stateManager, producerId, (short) 3,
                        ControlRecordType.COMMIT, 100));
    }

    @Test
    public void testCoordinatorFencing() {
        short epoch = 5;

        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 100, 1, time.milliseconds());

        ProducerStateEntry lastEntry = getLastEntryOrElseThrownByProducerId(stateManager, producerId);
        assertEquals(1, lastEntry.coordinatorEpoch());

        // writing with the current epoch is allowed
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 101, 1, time.milliseconds());

        // bumping the epoch is allowed
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 102, 2, time.milliseconds());

        // old epochs are not allowed
        assertThrows(TransactionCoordinatorFencedException.class,
                () -> appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT,
                        103, 1, time.milliseconds()));
    }

    @Test
    public void testCoordinatorFencedAfterReload() throws IOException {
        appendClientEntry(stateManager, producerId, epoch, defaultSequence, 99, true);
        appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, 100, 1, time.milliseconds());
        stateManager.takeSnapshot();

        ProducerStateManager recoveredMapping = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        recoveredMapping.truncateAndReload(0L, 2L, 70000);

        // append from old coordinator should be rejected
        assertThrows(TransactionCoordinatorFencedException.class, () -> appendEndTxnMarker(stateManager, producerId,
                epoch, ControlRecordType.COMMIT, 100));
    }

    @Test
    public void testLoadFromEmptySnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(file -> {
            try {
                file.truncate(0L);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadFromTruncatedSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(file -> {
            try {
                // truncate to some arbitrary point in the middle of the snapshot
                assertTrue(file.size() > 2);
                file.truncate(file.size() / 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadFromCorruptSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(file -> {
            try {
                // write some garbage somewhere in the file
                assertTrue(file.size() > 2);
                file.write(ByteBuffer.wrap(new byte[37]));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testAppendEmptyControlBatch() {
        long producerId = 23423L;
        int baseOffset = 15;

        RecordBatch batch = mock(RecordBatch.class);
        when(batch.isControlBatch()).thenReturn(true);
        when(batch.iterator()).thenReturn(Collections.emptyIterator());

        // Appending the empty control batch should not throw and a new transaction shouldn't be started
        append(stateManager, producerId, baseOffset, batch);
        assertEquals(OptionalLong.empty(), getLastEntryOrElseThrownByProducerId(stateManager, producerId).currentTxnFirstOffset());
    }

    @Test
    public void testRemoveStraySnapshotsKeepCleanShutdownSnapshot() throws IOException {
        // Test that when stray snapshots are removed, the largest stray snapshot is kept around. This covers the case where
        // the broker shutdown cleanly and emitted a snapshot file larger than the base offset of the active segment.

        // Create 3 snapshot files at different offsets.
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath()); // not stray
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 2).toPath()); // stray
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 42).toPath()); // not stray

        // claim that we only have one segment with a base offset of 5
        stateManager.removeStraySnapshots(singletonList(5L));

        // The snapshot file at offset 2 should be considered a stray, but the snapshot at 42 should be kept
        // around because it is the largest snapshot.
        assertEquals(OptionalLong.of(42), stateManager.latestSnapshotOffset());
        assertEquals(OptionalLong.of(5), stateManager.oldestSnapshotOffset());
        assertEquals(asList(5L, 42L), ProducerStateManager.listSnapshotFiles(logDir)
                .stream()
                .map(file -> file.offset)
                .sorted()
                .collect(Collectors.toList()));
    }

    @Test
    public void testRemoveAllStraySnapshots() throws IOException {
        // Test that when stray snapshots are removed, we remove only the stray snapshots below the largest segment base offset.
        // Snapshots associated with an offset in the list of segment base offsets should remain.

        // Create 3 snapshot files at different offsets.
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath()); // stray
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 2).toPath()); // stray
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 42).toPath()); // not stray

        stateManager.removeStraySnapshots(singletonList(42L));
        assertEquals(singletonList(42L), ProducerStateManager.listSnapshotFiles(logDir)
                .stream()
                .map(file -> file.offset)
                .sorted()
                .collect(Collectors.toList()));

    }

    /**
     * Test that removeAndMarkSnapshotForDeletion will rename the SnapshotFile with
     * the deletion suffix and remove it from the producer state.
     */
    @Test
    public void testRemoveAndMarkSnapshotForDeletion() throws IOException {
        Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath());
        ProducerStateManager manager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time);
        assertTrue(manager.latestSnapshotOffset().isPresent());
        Optional<SnapshotFile> snapshotFileOpt = manager.removeAndMarkSnapshotForDeletion(5);
        assertTrue(snapshotFileOpt.isPresent());
        assertTrue(snapshotFileOpt.get().file().toPath().toString().endsWith(LogFileUtils.DELETED_FILE_SUFFIX));
        assertFalse(manager.latestSnapshotOffset().isPresent());
    }

    /**
     * Test that marking a snapshot for deletion when the file has already been deleted
     * returns None instead of the SnapshotFile. The snapshot file should be removed from
     * the in-memory state of the ProducerStateManager. This scenario can occur during log
     * recovery when the intermediate ProducerStateManager instance deletes a file without
     * updating the state of the "real" ProducerStateManager instance which is passed to the Log.
     */
    @Test
    public void testRemoveAndMarkSnapshotForDeletionAlreadyDeleted() throws IOException {
        File file = LogFileUtils.producerSnapshotFile(logDir, 5);
        Files.createFile(file.toPath());
        ProducerStateManager manager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time);
        assertTrue(manager.latestSnapshotOffset().isPresent());
        Files.delete(file.toPath());
        assertFalse(manager.removeAndMarkSnapshotForDeletion(5).isPresent());
        assertFalse(manager.latestSnapshotOffset().isPresent());
    }

    @Test
    public void testEntryForVerification() {
        VerificationStateEntry originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);
        VerificationGuard originalEntryVerificationGuard = originalEntry.verificationGuard();

        // If we already have an entry, reuse it.
        VerificationStateEntry updateEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);
        VerificationStateEntry entry = stateManager.verificationStateEntry(producerId);
        assertEquals(originalEntryVerificationGuard, entry.verificationGuard());
        assertEquals(entry.verificationGuard(), updateEntry.verificationGuard());


        // Add the transactional data and clear the entry.
        appendClientEntry(stateManager, producerId, (short) 0, defaultSequence, 0, true);
        stateManager.clearVerificationStateEntry(producerId);
        assertNull(stateManager.verificationStateEntry(producerId));
    }

    @Test
    public void testSequenceAndEpochInVerificationEntry() {
        VerificationStateEntry originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 1, epoch);
        VerificationGuard originalEntryVerificationGuard = originalEntry.verificationGuard();

        verifyEntry(originalEntryVerificationGuard, originalEntry, 1, epoch);

        // If we see a lower sequence, update to the lower one.
        VerificationStateEntry updatedEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);
        verifyEntry(originalEntryVerificationGuard, updatedEntry, 0, epoch);

        // If we see a new epoch that is higher, update the sequence.
        VerificationStateEntry updatedEntryNewEpoch = stateManager.maybeCreateVerificationStateEntry(producerId, 2, (short) 1);
        verifyEntry(originalEntryVerificationGuard, updatedEntryNewEpoch, 2, (short) 1);

        // Ignore a lower epoch.
        VerificationStateEntry updatedEntryOldEpoch = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);
        verifyEntry(originalEntryVerificationGuard, updatedEntryOldEpoch, 2, (short) 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testThrowOutOfOrderSequenceWithVerificationSequenceCheck(boolean dynamicallyDisable) {
        VerificationStateEntry originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);

        // Even if we dynamically disable, we should still execute the sequence check if we have an entry
        if (dynamicallyDisable)
            producerStateManagerConfig.setTransactionVerificationEnabled(false);

        // Trying to append with a higher sequence should fail
        assertThrows(OutOfOrderSequenceException.class,
                () -> appendClientEntry(stateManager, producerId, epoch, 4, 0, true));

        assertEquals(originalEntry, stateManager.verificationStateEntry(producerId));
    }

    @Test
    public void testVerificationStateEntryExpiration() {
        VerificationStateEntry originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch);

        // Before timeout, we do not remove. Note: Accessing the verification entry does not update the time.
        time.sleep(producerStateManagerConfig.producerIdExpirationMs() / 2);
        stateManager.removeExpiredProducers(time.milliseconds());
        assertEquals(originalEntry, stateManager.verificationStateEntry(producerId));

        time.sleep((producerStateManagerConfig.producerIdExpirationMs() / 2) + 1);
        stateManager.removeExpiredProducers(time.milliseconds());
        assertNull(stateManager.verificationStateEntry(producerId));
    }

    @Test
    public void testLastStableOffsetCompletedTxn() {
        long segmentBaseOffset = 990000L;

        long producerId1 = producerId;
        long startOffset1 = 992342L;
        beginTransaction(producerId1, startOffset1, segmentBaseOffset);

        long producerId2 = producerId + 1;
        long startOffset2 = startOffset1 + 25;
        beginTransaction(producerId2, startOffset2, segmentBaseOffset);

        long producerId3 = producerId + 2;
        long startOffset3 = startOffset1 + 57;
        beginTransaction(producerId3, startOffset3, segmentBaseOffset);

        long lastOffset1 = startOffset3 + 15;
        CompletedTxn completedTxn1 = new CompletedTxn(producerId1, startOffset1, lastOffset1, false);
        assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn1));
        stateManager.completeTxn(completedTxn1);
        stateManager.onHighWatermarkUpdated(lastOffset1 + 1);

        assertEquals(Optional.of(startOffset2), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        long lastOffset3 = lastOffset1 + 20;
        CompletedTxn completedTxn3 = new CompletedTxn(producerId3, startOffset3, lastOffset3, false);
        assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn3));
        stateManager.completeTxn(completedTxn3);
        stateManager.onHighWatermarkUpdated(lastOffset3 + 1);
        assertEquals(Optional.of(startOffset2), stateManager.firstUnstableOffset().map(x -> x.messageOffset));

        long lastOffset2 = lastOffset3 + 78;
        CompletedTxn completedTxn2 = new CompletedTxn(producerId2, startOffset2, lastOffset2, false);
        assertEquals(lastOffset2 + 1, stateManager.lastStableOffset(completedTxn2));
        stateManager.completeTxn(completedTxn2);
        stateManager.onHighWatermarkUpdated(lastOffset2 + 1);
        assertEquals(Optional.empty(), stateManager.firstUnstableOffset());
    }

    @Test
    public void testSkipEmptyTransactions() {
        AtomicInteger seq = new AtomicInteger(defaultSequence);

        // Start one transaction in a separate append
        ProducerAppendInfo firstAppend = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        appendData(16L, 20L, firstAppend, seq);
        assertTxnMetadataEquals(new TxnMetadata(producerId, 16L), firstAppend.startedTransactions().get(0));
        stateManager.update(firstAppend);
        stateManager.onHighWatermarkUpdated(21L);
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), stateManager.firstUnstableOffset());

        // Now do a single append which completes the old transaction, mixes in
        // some empty transactions, one non-empty complete transaction, and one
        // incomplete transaction
        ProducerAppendInfo secondAppend = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        Optional<CompletedTxn> firstCompletedTxn = appendEndTransaction(ControlRecordType.COMMIT, 21, secondAppend);
        assertTrue(firstCompletedTxn.isPresent());
        assertEquals(Optional.of(new CompletedTxn(producerId, 16L, 21, false)), firstCompletedTxn);
        assertFalse(appendEndTransaction(ControlRecordType.COMMIT, 22, secondAppend).isPresent());
        assertFalse(appendEndTransaction(ControlRecordType.ABORT, 23, secondAppend).isPresent());
        appendData(24L, 27L, secondAppend, seq);
        Optional<CompletedTxn> secondCompletedTxn = appendEndTransaction(ControlRecordType.ABORT, 28L, secondAppend);
        assertTrue(secondCompletedTxn.isPresent());
        assertFalse(appendEndTransaction(ControlRecordType.ABORT, 29L, secondAppend).isPresent());
        appendData(30L, 31L, secondAppend, seq);

        int size = secondAppend.startedTransactions().size();
        assertEquals(2, size);
        assertTxnMetadataEquals(new TxnMetadata(producerId, new LogOffsetMetadata(24L)), secondAppend.startedTransactions().get(0));
        assertTxnMetadataEquals(new TxnMetadata(producerId, new LogOffsetMetadata(30L)), secondAppend.startedTransactions().get(size - 1));
        stateManager.update(secondAppend);
        stateManager.completeTxn(firstCompletedTxn.get());
        stateManager.completeTxn(secondCompletedTxn.get());
        stateManager.onHighWatermarkUpdated(32L);
        assertEquals(Optional.of(new LogOffsetMetadata(30L)), stateManager.firstUnstableOffset());
    }

    @Test
    public void testReadWriteSnapshot() throws IOException {
        Map<Long, ProducerStateEntry> expectedEntryMap = new HashMap<>();
        expectedEntryMap.put(1L, new ProducerStateEntry(1L, (short) 2, 3,
                RecordBatch.NO_TIMESTAMP,
                OptionalLong.of(100L),
                Optional.of(new BatchMetadata(1, 2L, 3, RecordBatch.NO_TIMESTAMP))));
        expectedEntryMap.put(11L, new ProducerStateEntry(11L, (short) 12, 13,
                123456L,
                OptionalLong.empty(),
                Optional.empty()));

        File file = new File(logDir, "testReadWriteSnapshot");
        ProducerStateManager.writeSnapshot(file, expectedEntryMap, true);
        assertEntries(expectedEntryMap, ProducerStateManager.readSnapshot(file));
    }

    private void appendEntry(ProducerStateManager stateManager,
                             long producerId,
                             short producerEpoch,
                             int seq,
                             long offset,
                             long milliseconds,
                             AppendOrigin appendOrigin,
                             boolean isTransactional) {
        ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(producerId, appendOrigin);
        producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, milliseconds,
                new LogOffsetMetadata(offset), offset, isTransactional);
        stateManager.update(producerAppendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private void appendClientEntry(ProducerStateManager stateManager,
                                   long producerId,
                                   short producerEpoch,
                                   int seq,
                                   long offset,
                                   long milliseconds,
                                   boolean isTransactional) {
        appendEntry(stateManager, producerId, producerEpoch, seq, offset, milliseconds, AppendOrigin.CLIENT, isTransactional);
    }

    private void appendClientEntry(ProducerStateManager stateManager,
                                   long producerId,
                                   short producerEpoch,
                                   int seq,
                                   long offset,
                                   boolean isTransactional) {
        appendClientEntry(stateManager, producerId, producerEpoch, seq, offset, time.milliseconds(), isTransactional);
    }

    private void appendReplicationEntry(ProducerStateManager stateManager,
                                        short producerEpoch,
                                        int seq,
                                        long offset,
                                        long milliseconds) {
        appendEntry(stateManager, 1L, producerEpoch, seq, offset, milliseconds, AppendOrigin.REPLICATION, false);
    }

    private void appendReplicationEntry(ProducerStateManager stateManager,
                                        short producerEpoch,
                                        int seq,
                                        long offset) {
        appendReplicationEntry(stateManager, producerEpoch, seq, offset, time.milliseconds());
    }

    private void appendEndTxnMarker(ProducerStateManager stateManager,
                                    long producerId,
                                    short producerEpoch,
                                    ControlRecordType controlType,
                                    long offset,
                                    int coordinatorEpoch,
                                    long timestamp) {
        ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.COORDINATOR);
        EndTransactionMarker endTxnMarker = new EndTransactionMarker(controlType, coordinatorEpoch);
        Optional<CompletedTxn> completedTxn = producerAppendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, offset, timestamp);
        stateManager.update(producerAppendInfo);
        completedTxn.ifPresent(stateManager::completeTxn);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private void appendEndTxnMarker(ProducerStateManager stateManager,
                                    long producerId,
                                    short producerEpoch,
                                    ControlRecordType controlType,
                                    long offset) {
        appendEndTxnMarker(stateManager, producerId, producerEpoch, controlType, offset, 0, time.milliseconds());
    }

    private ProducerStateEntry getLastEntryOrElseThrownByProducerId(ProducerStateManager stateManger, long producerId) {
        return stateManger.lastEntry(producerId)
                .orElseThrow(() -> new RuntimeException("This producerId:" + producerId + " should have last entry"));
    }

    private void verifyLastEntryWithTxnData(ProducerStateEntry lastEntry,
                                            int expectedFirstSeq,
                                            long expectedLastSeq,
                                            long expectedFirstDataOffset,
                                            long expectedLastDataOffset,
                                            OptionalLong expectedCurrentTxnFirstOffset,
                                            ProducerAppendInfo appendInfo) {
        assertEquals(epoch, lastEntry.producerEpoch());
        assertEquals(expectedFirstSeq, lastEntry.firstSeq());
        assertEquals(expectedLastSeq, lastEntry.lastSeq());
        assertEquals(expectedFirstDataOffset, lastEntry.firstDataOffset());
        assertEquals(expectedLastDataOffset, lastEntry.lastDataOffset());
        assertEquals(expectedCurrentTxnFirstOffset, lastEntry.currentTxnFirstOffset());
        assertTxnMetadataEquals(singletonList(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions());
    }

    private void assertTxnMetadataEquals(List<TxnMetadata> expected, List<TxnMetadata> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertTxnMetadataEquals(expected.get(i), actual.get(i));
        }
    }

    private void assertTxnMetadataEquals(TxnMetadata expected, TxnMetadata actual) {
        assertEquals(expected.producerId, actual.producerId);
        assertEquals(expected.firstOffset, actual.firstOffset);
        assertEquals(expected.lastOffset, actual.lastOffset);
    }

    private Set<Long> currentSnapshotOffsets() {
        return Arrays.stream(Objects.requireNonNull(logDir.listFiles()))
                .map(LogFileUtils::offsetFromFile)
                .collect(Collectors.toSet());
    }

    private void testLoadFromCorruptSnapshot(Consumer<FileChannel> consumer) throws IOException {
        appendClientEntry(stateManager, producerId, epoch, 0, 0L, false);
        stateManager.takeSnapshot();

        appendClientEntry(stateManager, producerId, epoch, 1, 1L, false);
        stateManager.takeSnapshot();

        // Truncate the last snapshot
        OptionalLong latestSnapshotOffsetOpt = stateManager.latestSnapshotOffset();
        assertTrue(latestSnapshotOffsetOpt.isPresent());
        long latestSnapshotOffset = latestSnapshotOffsetOpt.getAsLong();
        assertEquals(2L, latestSnapshotOffset);
        File snapshotToTruncate = LogFileUtils.producerSnapshotFile(logDir, latestSnapshotOffset);
        try (FileChannel channel = FileChannel.open(snapshotToTruncate.toPath(), StandardOpenOption.WRITE)) {
            consumer.accept(channel);
        }

        // Ensure that the truncated snapshot is deleted and producer state is loaded from the previous snapshot
        ProducerStateManager reloadedStateManager = new ProducerStateManager(partition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time);
        reloadedStateManager.truncateAndReload(0L, 20L, time.milliseconds());
        assertFalse(snapshotToTruncate.exists());

        ProducerStateEntry loadedProducerState = reloadedStateManager.activeProducers().get(producerId);
        assertNotNull(loadedProducerState);
        assertEquals(0L, loadedProducerState.lastDataOffset());
    }

    private void append(ProducerStateManager stateManager,
                        long producerId,
                        int offset,
                        RecordBatch batch) {
        ProducerAppendInfo appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        appendInfo.append(batch, Optional.empty());
        stateManager.update(appendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private void verifyEntry(VerificationGuard originalEntryVerificationGuard,
                             VerificationStateEntry newEntry,
                             int expectedSequence,
                             short expectedEpoch) {
        VerificationStateEntry entry = stateManager.verificationStateEntry(producerId);
        assertEquals(originalEntryVerificationGuard, entry.verificationGuard());
        assertEquals(entry.verificationGuard(), newEntry.verificationGuard());
        assertEquals(expectedSequence, entry.lowestSequence());
        assertEquals(expectedEpoch, entry.epoch());
    }

    private void beginTransaction(long producerId, long startOffset, long segmentBaseOffset) {
        int relativeOffset = (int) (startOffset - segmentBaseOffset);
        ProducerAppendInfo appendInfo = new ProducerAppendInfo(
                partition,
                producerId,
                ProducerStateEntry.empty(producerId),
                AppendOrigin.CLIENT,
                stateManager.maybeCreateVerificationStateEntry(producerId, 0, epoch)
        );
        LogOffsetMetadata firstOffsetMetadata = new LogOffsetMetadata(startOffset, segmentBaseOffset, 50 * relativeOffset);
        appendInfo.appendDataBatch(epoch, 0, 0, time.milliseconds(),
                firstOffsetMetadata, startOffset, true);
        stateManager.update(appendInfo);
    }

    private Optional<CompletedTxn> appendEndTransaction(ControlRecordType recordType, long offset, ProducerAppendInfo appendInfo) {
        return appendInfo.appendEndTxnMarker(new EndTransactionMarker(recordType, 27),
                epoch, offset, time.milliseconds());
    }

    private void appendData(long startOffset, long endOffset, ProducerAppendInfo appendInfo, AtomicInteger seq) {
        int count = (int) (endOffset - startOffset);
        appendInfo.appendDataBatch(epoch, seq.get(), seq.addAndGet(count),
                time.milliseconds(), new LogOffsetMetadata(startOffset), endOffset, true);
        seq.incrementAndGet();
    }

    private void assertEntries(Map<Long, ProducerStateEntry> expected, List<ProducerStateEntry> actual) {
        Map<Long, ProducerStateEntry> actualEntryMap =
                actual.stream().collect(Collectors.toMap(ProducerStateEntry::producerId, p -> p));
        assertEquals(expected.keySet(), actualEntryMap.keySet());
        expected.forEach((producerId, expectedEntry) -> {
            ProducerStateEntry actualEntry = actualEntryMap.get(producerId);
            assertProducerStateEntry(expectedEntry, actualEntry);
        });
    }

    private void assertProducerStateEntry(ProducerStateEntry expected, ProducerStateEntry actual) {
        assertEquals(expected.producerId(), actual.producerId());
        assertEquals(expected.producerEpoch(), actual.producerEpoch());
        assertEquals(expected.coordinatorEpoch(), actual.coordinatorEpoch());
        assertEquals(expected.lastTimestamp(), actual.lastTimestamp());
        assertEquals(expected.currentTxnFirstOffset(), actual.currentTxnFirstOffset());
        assertIterableEquals(expected.batchMetadata(), actual.batchMetadata());
    }
}
