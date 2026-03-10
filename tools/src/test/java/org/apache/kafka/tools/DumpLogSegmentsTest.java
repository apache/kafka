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
package org.apache.kafka.tools;

import kafka.utils.TestUtils;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.RecordVersion;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.internals.KafkaRaftLog;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.tools.ToolsTestUtils.captureStandardOut;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DumpLogSegmentsTest {
    private static final Pattern SIZE_PATTERN = Pattern.compile(".+?size:\\s(\\d+).+");

    private static final class BatchInfo {
        private final List<SimpleRecord> records;
        private final boolean hasKeys;
        private final boolean hasValues;

        private BatchInfo(List<SimpleRecord> records, boolean hasKeys, boolean hasValues) {
            this.records = records;
            this.hasKeys = hasKeys;
            this.hasValues = hasValues;
        }
    }

    private final File tmpDir = TestUtils.tempDir();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private final String segmentName = "00000000000000000000";
    private final String logFilePath = new File(logDir, segmentName + ".log").getAbsolutePath();
    private final String snapshotPath = new File(logDir, "00000000000000000000-0000000000.checkpoint").getAbsolutePath();
    private final String indexFilePath = new File(logDir, segmentName + ".index").getAbsolutePath();
    private final String timeIndexFilePath = new File(logDir, segmentName + ".timeindex").getAbsolutePath();
    private final MockTime time = new MockTime(0, 0);
    private UnifiedLog log;

    @AfterEach
    public void tearDown() {
        if (log != null) {
            Utils.closeQuietly(log, "UnifiedLog");
        }
    }

    private UnifiedLog createTestLog() throws Exception {
        Properties props = new Properties();
        props.setProperty(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "128");
        // This test uses future timestamps beyond the default of 1 hour.
        props.setProperty(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        return createLog(new LogConfig(props));
    }

    private UnifiedLog createLog(LogConfig logConfig) throws Exception {
        return createLog(logConfig, time);
    }

    private UnifiedLog createLog(LogConfig logConfig, MockTime logTime) throws Exception {
        log = UnifiedLog.create(
            logDir,
            logConfig,
            0L,
            0L,
            logTime.scheduler,
            new BrokerTopicStats(),
            logTime,
            5 * 60 * 1000,
            new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
            new LogDirFailureChannel(10),
            true,
            Optional.empty()
        );
        return log;
    }

    private LogConfig createLogConfig(int segmentBytes) {
        Properties props = new Properties();
        props.setProperty(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(segmentBytes));
        return new LogConfig(props);
    }

    private void addSimpleRecords(UnifiedLog log, List<BatchInfo> batches) throws Exception {
        long now = System.currentTimeMillis();
        List<SimpleRecord> firstBatchRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            firstBatchRecords.add(new SimpleRecord(
                now + i * 2L,
                ("message key " + i).getBytes(),
                ("message value " + i).getBytes()
            ));
        }
        batches.add(new BatchInfo(firstBatchRecords, true, true));

        List<SimpleRecord> secondBatchRecords = new ArrayList<>();
        for (int i = 10; i < 30; i++) {
            secondBatchRecords.add(new SimpleRecord(
                now + i * 3L,
                ("message key " + i).getBytes(),
                    null
            ));
        }
        batches.add(new BatchInfo(secondBatchRecords, true, false));

        List<SimpleRecord> thirdBatchRecords = new ArrayList<>();
        for (int i = 30; i < 50; i++) {
            thirdBatchRecords.add(new SimpleRecord(
                now + i * 5L,
                    null,
                ("message value " + i).getBytes()
            ));
        }
        batches.add(new BatchInfo(thirdBatchRecords, false, true));

        List<SimpleRecord> fourthBatchRecords = new ArrayList<>();
        for (int i = 50; i < 60; i++) {
            fourthBatchRecords.add(new SimpleRecord(now + i * 7L, null));
        }
        batches.add(new BatchInfo(fourthBatchRecords, false, false));

        for (BatchInfo batchInfo : batches) {
            log.appendAsLeader(
                MemoryRecords.withRecords(
                    Compression.NONE,
                    0,
                    batchInfo.records.toArray(new SimpleRecord[0])
                ),
                0
            );
        }
        // Flush, but don't close so that the indexes are not trimmed and contain some zero entries
        log.flush(false);
    }

    @Test
    public void testBatchAndRecordMetadataOutput() throws Exception {
        log = createTestLog();

        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, 0,
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        ), 0);

        log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), 0,
            new SimpleRecord(time.milliseconds(), "c".getBytes(), "1".getBytes()),
            new SimpleRecord("d".getBytes())
        ), 3);

        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, 0,
            new SimpleRecord("e".getBytes(), null),
            new SimpleRecord(null, "f".getBytes()),
            new SimpleRecord("g".getBytes())
        ), 3);

        log.appendAsLeader(MemoryRecords.withIdempotentRecords(Compression.NONE, 29342342L, (short) 15, 234123,
            new SimpleRecord("h".getBytes())
        ), 3);

        log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.gzip().build(), 98323L, (short) 99, 266,
            new SimpleRecord("i".getBytes()),
            new SimpleRecord("j".getBytes())
        ), 5);

        log.appendAsLeader(MemoryRecords.withEndTransactionMarker(98323L, (short) 99,
            new EndTransactionMarker(ControlRecordType.COMMIT, 100)
        ), 7, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        assertDumpLogRecordMetadata(log);
    }

    @Test
    public void testPrintDataLog() throws Exception {
        log = createTestLog();
        List<BatchInfo> batches = new ArrayList<>();
        addSimpleRecords(log, batches);

        // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
        verifyRecordsInOutput(batches, true, new String[] {"--print-data-log", "--files", logFilePath});
        // Verify that records are printed with --print-data-log if --deep-iteration is also specified
        verifyRecordsInOutput(batches, true, new String[] {"--print-data-log", "--deep-iteration", "--files", logFilePath});
        // Verify that records are printed with --value-decoder even if --print-data-log is not specified
        verifyRecordsInOutput(batches, true, new String[] {"--value-decoder-class", "org.apache.kafka.tools.api.StringDecoder", "--files", logFilePath});
        // Verify that records are printed with --key-decoder even if --print-data-log is not specified
        verifyRecordsInOutput(batches, true, new String[] {"--key-decoder-class", "org.apache.kafka.tools.api.StringDecoder", "--files", logFilePath});
        // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
        verifyRecordsInOutput(batches, false, new String[] {"--deep-iteration", "--files", logFilePath});

        // Verify that records are not printed by default
        verifyNoRecordsInOutput(new String[] {"--files", logFilePath});
    }

    @Test
    public void testDumpIndexMismatches() throws Exception {
        log = createTestLog();
        List<BatchInfo> batches = new ArrayList<>();
        addSimpleRecords(log, batches);

        Map<String, Map<Long, Long>> offsetMismatches = new HashMap<>();
        DumpLogSegments.dumpIndex(new File(indexFilePath), false, true, offsetMismatches, Integer.MAX_VALUE);
        assertEquals(Collections.emptyMap(), offsetMismatches);
    }

    @Test
    public void testDumpTimeIndexErrors() throws Exception {
        log = createTestLog();
        List<BatchInfo> batches = new ArrayList<>();
        addSimpleRecords(log, batches);

        DumpLogSegments.TimeIndexDumpErrors errors = new DumpLogSegments.TimeIndexDumpErrors();
        DumpLogSegments.dumpTimeIndex(new File(timeIndexFilePath), false, true, errors);
        assertEquals(Collections.emptyMap(), errors.misMatchesForTimeIndexFilesMap);
        assertEquals(Collections.emptyMap(), errors.outOfOrderTimestamp);
        assertEquals(Collections.emptyMap(), errors.shallowOffsetNotFound);
    }

    private int countSubstring(String str, String sub) {
        int count = 0;
        for (int i = 0; i <= str.length() - sub.length(); i++) {
            if (str.startsWith(sub, i)) {
                count++;
            }
        }
        return count;
    }

    // the number of batches in the log dump is equal to
    // the number of occurrences of the "baseOffset:" substring
    private int batchCount(String str) {
        return countSubstring(str, "baseOffset:");
    }

    // the number of records in the log dump is equal to
    // the number of occurrences of the "payload:" substring
    private int recordCount(String str) {
        return countSubstring(str, "payload:");
    }

    @Test
    public void testDumpRemoteLogMetadataEmpty() throws Exception {
        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath});
        assertEquals(0, batchCount(output));
        assertEquals(0, recordCount(output));
        assertTrue(output.contains("Log starting offset: 0"));
    }

    @Test
    public void testDumpRemoteLogMetadataOneRecordOneBatch() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";

        List<RemotePartitionDeleteMetadata> metadata = List.of(
            new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds(), 0)
        );

        SimpleRecord[] records = metadata.stream()
            .map(message -> new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message)))
            .toArray(SimpleRecord[]::new);

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records), 0);
        log.flush(false);

        String expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
            "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath});
        assertEquals(1, batchCount(output));
        assertEquals(1, recordCount(output));
        assertTrue(output.contains("Log starting offset: 0"));
        assertTrue(output.contains(expectedDeletePayload));
    }

    @Test
    public void testDumpRemoteLogMetadataMultipleRecordsOneBatch() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";
        Uuid remoteSegmentId = Uuid.randomUuid();

        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, remoteSegmentId);

        List<RemoteLogMetadata> metadata = List.of(
            new RemoteLogSegmentMetadataUpdate(
                remoteLogSegmentId,
                time.milliseconds(),
                Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {0, 1, 2, 3})),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0
            ),
            new RemotePartitionDeleteMetadata(
                topicIdPartition,
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED,
                time.milliseconds(),
                0
            )
        );

        SimpleRecord[] metadataRecords = metadata.stream()
            .map(message -> new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message)))
            .toArray(SimpleRecord[]::new);

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords), 0);
        log.flush(false);

        String expectedUpdatePayload = String.format("RemoteLogSegmentMetadataUpdate{remoteLogSegmentId=" +
            "RemoteLogSegmentId{topicIdPartition=%s:%s-0, id=%s}, customMetadata=Optional[" +
            "CustomMetadata{4 bytes}], state=COPY_SEGMENT_FINISHED, eventTimestampMs=0, brokerId=0}",
            topicId, topicName, remoteSegmentId);
        String expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
            "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath});
        assertEquals(1, batchCount(output));
        assertEquals(2, recordCount(output));
        assertTrue(output.contains("Log starting offset: 0"));
        assertTrue(output.contains(expectedUpdatePayload));
        assertTrue(output.contains(expectedDeletePayload));
    }

    @Test
    public void testDumpRemoteLogMetadataMultipleRecordsMultipleBatches() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";
        Uuid remoteSegmentId = Uuid.randomUuid();

        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, remoteSegmentId);

        List<RemoteLogMetadata> metadata = List.of(
            new RemoteLogSegmentMetadataUpdate(
                remoteLogSegmentId,
                time.milliseconds(),
                Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {0, 1, 2, 3})),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0
            ),
            new RemotePartitionDeleteMetadata(
                topicIdPartition,
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED,
                time.milliseconds(),
                0
            )
        );

        SimpleRecord[] records = metadata.stream()
            .map(message -> new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message)))
            .toArray(SimpleRecord[]::new);

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records), 0);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records), 0);
        log.flush(false);

        String expectedUpdatePayload = String.format("RemoteLogSegmentMetadataUpdate{remoteLogSegmentId=" +
            "RemoteLogSegmentId{topicIdPartition=%s:%s-0, id=%s}, customMetadata=Optional[" +
            "CustomMetadata{4 bytes}], state=COPY_SEGMENT_FINISHED, eventTimestampMs=0, brokerId=0}",
            topicId, topicName, remoteSegmentId);
        String expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
            "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath});
        assertEquals(2, batchCount(output));
        assertEquals(4, recordCount(output));
        assertTrue(output.contains("Log starting offset: 0"));
        assertEquals(2, countSubstring(output, expectedUpdatePayload));
        assertEquals(2, countSubstring(output, expectedDeletePayload));
    }

    @Test
    public void testDumpRemoteLogMetadataNonZeroStartingOffset() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";

        List<RemotePartitionDeleteMetadata> metadata = List.of(
            new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds(), 0)
        );

        SimpleRecord[] metadataRecords = metadata.stream()
            .map(message -> new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message)))
            .toArray(SimpleRecord[]::new);

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords), 0);
        LogSegment secondSegment = log.roll();
        secondSegment.append(1L, MemoryRecords.withRecords(Compression.NONE, metadataRecords));
        secondSegment.flush();
        log.flush(true);

        String expectedDeletePayload = String.format("RemotePartitionDeleteMetadata{topicPartition=%s:%s-0, " +
            "state=DELETE_PARTITION_MARKED, eventTimestampMs=0, brokerId=0}", topicId, topicName);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files",
            secondSegment.log().file().getAbsolutePath()});
        assertEquals(1, batchCount(output));
        assertEquals(1, recordCount(output));
        assertTrue(output.contains("Log starting offset: 1"));
        assertTrue(output.contains(expectedDeletePayload));
    }

    @Test
    public void testDumpRemoteLogMetadataWithCorruption() throws Exception {
        SimpleRecord[] metadataRecords = new SimpleRecord[] {new SimpleRecord(null, "corrupted".getBytes())};

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords), 0);
        log.flush(false);

        String output = runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath});
        assertEquals(1, batchCount(output));
        assertEquals(1, recordCount(output));
        assertTrue(output.contains("Log starting offset: 0"));
        assertTrue(output.contains("Could not deserialize metadata record"));
    }

    @Test
    public void testDumpRemoteLogMetadataIoException() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";

        List<RemotePartitionDeleteMetadata> metadata = List.of(
            new RemotePartitionDeleteMetadata(new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED, time.milliseconds(), 0)
        );

        SimpleRecord[] metadataRecords = metadata.stream()
            .map(message -> new SimpleRecord(null, new RemoteLogMetadataSerde().serialize(message)))
            .toArray(SimpleRecord[]::new);

        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, metadataRecords), 0);
        log.flush(false);

        Files.setPosixFilePermissions(Paths.get(logFilePath), PosixFilePermissions.fromString("-w-------"));

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", logFilePath}));
        assertInstanceOf(AccessDeniedException.class, thrown.getCause());
    }

    @Test
    public void testDumpRemoteLogMetadataNoFilesFlag() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
        try {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> runDumpLogSegments(new String[] {"--remote-log-metadata-decoder"}));
            assertEquals("Missing required argument \"[files]\"", thrown.getMessage());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testDumpRemoteLogMetadataNoSuchFileException() {
        String noSuchFileLogPath = "/tmp/nosuchfile/00000000000000000000.log";
        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> runDumpLogSegments(new String[] {"--remote-log-metadata-decoder", "--files", noSuchFileLogPath}));
        assertInstanceOf(NoSuchFileException.class, thrown.getCause());
    }

    @Test
    public void testDumpMetadataRecords() throws Exception {
        MockTime mockTime = new MockTime();
        LogConfig logConfig = createLogConfig(1024 * 1024);
        log = createLog(logConfig, mockTime);

        List<ApiMessageAndVersion> metadataRecords = List.of(
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), (short) 0),
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), (short) 0),
            new ApiMessageAndVersion(
                new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), (short) 0),
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1)
                    .setPartitionId(0).setIsr(List.of(0, 1, 2)), (short) 0)
        );

        List<SimpleRecord> records = new ArrayList<>();
        for (ApiMessageAndVersion message : metadataRecords) {
            MetadataRecordSerde serde = MetadataRecordSerde.INSTANCE;
            ObjectSerializationCache cache = new ObjectSerializationCache();
            int size = serde.recordSize(message, cache);
            ByteBuffer buf = ByteBuffer.allocate(size);
            ByteBufferAccessor writer = new ByteBufferAccessor(buf);
            serde.write(message, cache, writer);
            buf.flip();
            records.add(new SimpleRecord(null, buf.array()));
        }

        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records.toArray(new SimpleRecord[0])), 1);
        log.flush(false);

        String output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--files", logFilePath});
        assertTrue(output.contains("Log starting offset: 0"));
        assertTrue(output.contains("TOPIC_RECORD"));
        assertTrue(output.contains("BROKER_RECORD"));

        output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--skip-record-metadata", "--files", logFilePath});
        assertTrue(output.contains("TOPIC_RECORD"));
        assertTrue(output.contains("BROKER_RECORD"));

        // Bogus metadata record
        ByteBuffer buf = ByteBuffer.allocate(4);
        ByteBufferAccessor writer = new ByteBufferAccessor(buf);
        writer.writeUnsignedVarint(10000);
        writer.writeUnsignedVarint(10000);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(null, buf.array())), 2);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records.toArray(new SimpleRecord[0])), 2);

        output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--skip-record-metadata", "--files", logFilePath});
        assertTrue(output.contains("TOPIC_RECORD"));
        assertTrue(output.contains("BROKER_RECORD"));
        assertTrue(output.contains("skipping"));
    }

    @Test
    public void testDumpControlRecord() throws Exception {
        log = createTestLog();

        log.appendAsLeader(MemoryRecords.withEndTransactionMarker(0L, (short) 0,
            new EndTransactionMarker(ControlRecordType.COMMIT, 100)
        ), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        log.appendAsLeader(MemoryRecords.withLeaderChangeMessage(0L, 0L, 0, ByteBuffer.allocate(4),
            new LeaderChangeMessage()
        ), 0, AppendOrigin.COORDINATOR);

        log.appendAsLeader(MemoryRecords.withSnapshotHeaderRecord(0L, 0L, 0, ByteBuffer.allocate(4),
            new SnapshotHeaderRecord()
        ), 0, AppendOrigin.COORDINATOR);

        log.appendAsLeader(MemoryRecords.withSnapshotFooterRecord(0L, 0L, 0, ByteBuffer.allocate(4),
            new SnapshotFooterRecord()
                .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION)
        ), 0, AppendOrigin.COORDINATOR);

        log.appendAsLeader(MemoryRecords.withKRaftVersionRecord(0L, 0L, 0, ByteBuffer.allocate(4),
            new KRaftVersionRecord()
        ), 0, AppendOrigin.COORDINATOR);

        log.appendAsLeader(MemoryRecords.withVotersRecord(0L, 0L, 0, ByteBuffer.allocate(4),
            new VotersRecord()
        ), 0, AppendOrigin.COORDINATOR);
        log.flush(false);

        String output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--files", logFilePath});
        assertTrue(output.contains("endTxnMarker"), output);
        assertTrue(output.contains("LeaderChange"), output);
        assertTrue(output.contains("SnapshotHeader"), output);
        assertTrue(output.contains("SnapshotFooter"), output);
        assertTrue(output.contains("KRaftVersion"), output);
        assertTrue(output.contains("KRaftVoters"), output);
    }

    @Test
    public void testDumpMetadataSnapshot() throws Exception {
        List<ApiMessageAndVersion> metadataRecords = List.of(
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), (short) 0),
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), (short) 0),
            new ApiMessageAndVersion(
                new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), (short) 0),
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1)
                    .setPartitionId(0).setIsr(List.of(0, 1, 2)), (short) 0)
        );

        KafkaRaftLog metadataLog = KafkaRaftLog.createLog(
            Topic.CLUSTER_METADATA_TOPIC_PARTITION,
            Uuid.METADATA_TOPIC_ID,
            logDir,
            time,
            time.scheduler,
            createMetadataLogConfig(
                100 * 1024,
                10 * 1000,
                100 * 1024,
                60 * 1000
            ),
            1
        );

        long lastContainedLogTimestamp = 10000;

        try (RecordsSnapshotWriter<ApiMessageAndVersion> snapshotWriter = new RecordsSnapshotWriter.Builder()
            .setTime(new MockTime())
            .setLastContainedLogTimestamp(lastContainedLogTimestamp)
            .setRawSnapshotWriter(metadataLog.createNewSnapshot(new OffsetAndEpoch(0, 0)).get())
            .setKraftVersion(KRaftVersion.KRAFT_VERSION_1)
            .setVoterSet(Optional.of(createVoterSet()))
            .build(MetadataRecordSerde.INSTANCE)
        ) {
            snapshotWriter.append(metadataRecords);
            snapshotWriter.freeze();
        }

        String output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--files", snapshotPath});
        assertTrue(output.contains("Snapshot end offset: 0, epoch: 0"), output);
        assertTrue(output.contains("TOPIC_RECORD"), output);
        assertTrue(output.contains("BROKER_RECORD"), output);
        assertTrue(output.contains("SnapshotHeader"), output);
        assertTrue(output.contains("SnapshotFooter"), output);
        assertTrue(output.contains("KRaftVersion"), output);
        assertTrue(output.contains("KRaftVoters"), output);
        assertTrue(output.contains("\"lastContainedLogTimestamp\":" + lastContainedLogTimestamp), output);

        output = runDumpLogSegments(new String[] {"--cluster-metadata-decoder", "--skip-record-metadata", "--files", snapshotPath});
        assertTrue(output.contains("Snapshot end offset: 0, epoch: 0"), output);
        assertTrue(output.contains("TOPIC_RECORD"), output);
        assertTrue(output.contains("BROKER_RECORD"), output);
        assertFalse(output.contains("SnapshotHeader"), output);
        assertFalse(output.contains("SnapshotFooter"), output);
        assertFalse(output.contains("KRaftVersion"), output);
        assertFalse(output.contains("KRaftVoters"), output);
        assertFalse(output.contains("\"lastContainedLogTimestamp\": " + lastContainedLogTimestamp), output);
    }

    @Test
    public void testDumpEmptyIndex() throws Exception {
        log = createTestLog();

        File indexFile = new File(indexFilePath);
        new PrintWriter(indexFile).close();
        String expectOutput = indexFile + " is empty.";

        String output = captureStandardOut(() -> {
            try {
                DumpLogSegments.dumpIndex(
                    indexFile,
                    false,
                    true,
                    new HashMap<>(),
                    Integer.MAX_VALUE
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(expectOutput, output);
    }

    @Test
    public void testPrintDataLogPartialBatches() throws Exception {
        log = createTestLog();
        List<BatchInfo> batches = new ArrayList<>();
        addSimpleRecords(log, batches);

        int totalBatches = batches.size();
        int partialBatches = totalBatches / 2;

        // Get all the batches
        String output = runDumpLogSegments(new String[] {"--files", logFilePath});
        ListIterator<String> lines = Arrays.asList(output.split("\n")).listIterator();

        // Get total bytes of the partial batches
        int partialBatchesBytes = readPartialBatchesBytes(lines, partialBatches);

        // Request only the partial batches by bytes
        String partialOutput = runDumpLogSegments(new String[] {"--max-bytes", Integer.toString(partialBatchesBytes), "--files", logFilePath});
        ListIterator<String> partialLines = Arrays.asList(partialOutput.split("\n")).listIterator();

        // Count the total of partial batches limited by bytes
        int partialBatchesCount = countBatches(partialLines);

        assertEquals(partialBatches, partialBatchesCount);
    }

    private Record serializedRecord(ApiMessage key, ApiMessageAndVersion value) {
        byte[] valueBytes = value == null ? null : MessageUtil.toVersionPrefixedBytes(value.version(), value.message());
        return TestUtils.singletonRecords(
            valueBytes,
            MessageUtil.toCoordinatorTypePrefixedBytes(key),
            Compression.NONE,
            RecordBatch.NO_TIMESTAMP,
            RecordBatch.CURRENT_MAGIC_VALUE
        ).records().iterator().next();
    }

    @Test
    public void testOffsetsMessageParser() {
        DumpLogSegments.OffsetsMessageParser parser = new DumpLogSegments.OffsetsMessageParser();

        // The key is mandatory.
        assertEquals(
            "Failed to decode message at offset 0 using the specified decoder (message had a missing key)",
            assertThrows(RuntimeException.class, () ->
                parser.parse(TestUtils.singletonRecords(null, null, Compression.NONE, RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE).records().iterator().next())
            ).getMessage()
        );

        // A valid key and value should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                new ApiMessageAndVersion(
                    new ConsumerGroupMetadataValue().setEpoch(10),
                    (short) 0
                )
            )),
            Optional.of("{\"type\":\"3\",\"data\":{\"groupId\":\"group\"}}"),
            Optional.of("{\"version\":\"0\",\"data\":{\"epoch\":10}}")
        );

        // Consumer embedded protocol is parsed if possible.
        assertParseResult(
            parser.parse(serializedRecord(
                new GroupMetadataKey().setGroup("group"),
                new ApiMessageAndVersion(
                    new GroupMetadataValue()
                        .setProtocolType("consumer")
                        .setProtocol("range")
                        .setLeader("member")
                        .setGeneration(10)
                        .setMembers(Collections.singletonList(
                            new GroupMetadataValue.MemberMetadata()
                                .setMemberId("member")
                                .setClientId("client")
                                .setClientHost("host")
                                .setGroupInstanceId("instance")
                                .setSessionTimeout(100)
                                .setRebalanceTimeout(1000)
                                .setSubscription(Utils.toArray(ConsumerProtocol.serializeSubscription(
                                    new Subscription(
                                        Collections.singletonList("foo"),
                                        null,
                                        Collections.singletonList(new TopicPartition("foo", 0)),
                                        0,
                                        Optional.of("rack")))))
                                .setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                                    new Assignment(Collections.singletonList(new TopicPartition("foo", 0))))))
                        )),
                    GroupMetadataValue.HIGHEST_SUPPORTED_VERSION
                )
            )),
            Optional.of("{\"type\":\"2\",\"data\":{\"group\":\"group\"}}"),
            Optional.of("{\"version\":\"4\",\"data\":{\"protocolType\":\"consumer\",\"generation\":10,\"protocol\":\"range\"," +
                "\"leader\":\"member\",\"currentStateTimestamp\":-1,\"members\":[{\"memberId\":\"member\"," +
                "\"groupInstanceId\":\"instance\",\"clientId\":\"client\",\"clientHost\":\"host\"," +
                "\"rebalanceTimeout\":1000,\"sessionTimeout\":100,\"subscription\":{\"topics\":[\"foo\"]," +
                "\"userData\":null,\"ownedPartitions\":[{\"topic\":\"foo\",\"partitions\":[0]}]," +
                "\"generationId\":0,\"rackId\":\"rack\"},\"assignment\":{\"assignedPartitions\":" +
                "[{\"topic\":\"foo\",\"partitions\":[0]}],\"userData\":null}}]}}")
        );

        // Consumer embedded protocol is not parsed if malformed.
        assertParseResult(
            parser.parse(serializedRecord(
                new GroupMetadataKey().setGroup("group"),
                new ApiMessageAndVersion(
                    new GroupMetadataValue()
                        .setProtocolType("consumer")
                        .setProtocol("range")
                        .setLeader("member")
                        .setGeneration(10)
                        .setMembers(Collections.singletonList(
                            new GroupMetadataValue.MemberMetadata()
                                .setMemberId("member")
                                .setClientId("client")
                                .setClientHost("host")
                                .setGroupInstanceId("instance")
                                .setSessionTimeout(100)
                                .setRebalanceTimeout(1000)
                                .setSubscription("Subscription".getBytes())
                                .setAssignment("Assignment".getBytes())
                        )),
                    GroupMetadataValue.HIGHEST_SUPPORTED_VERSION
                )
            )),
            Optional.of("{\"type\":\"2\",\"data\":{\"group\":\"group\"}}"),
            Optional.of("{\"version\":\"4\",\"data\":{\"protocolType\":\"consumer\",\"generation\":10,\"protocol\":\"range\"," +
                "\"leader\":\"member\",\"currentStateTimestamp\":-1,\"members\":[{\"memberId\":\"member\"," +
                "\"groupInstanceId\":\"instance\",\"clientId\":\"client\",\"clientHost\":\"host\"," +
                "\"rebalanceTimeout\":1000,\"sessionTimeout\":100,\"subscription\":\"U3Vic2NyaXB0aW9u\"," +
                "\"assignment\":\"QXNzaWdubWVudA==\"}]}}")
        );

        // A valid key with a tombstone should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                null
            )),
            Optional.of("{\"type\":\"3\",\"data\":{\"groupId\":\"group\"}}"),
            Optional.of("<DELETE>")
        );

        // An unknown record type should be handled and reported as such.
        assertParseResult(
            parser.parse(
                TestUtils.singletonRecords(
                    new byte[0],
                    ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array(),
                    Compression.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE
                ).records().iterator().next()
            ),
            Optional.of("Unknown record type 32767 at offset 0, skipping."),
            Optional.empty()
        );

        // Any parsing error is swallowed and reported.
        assertParseResult(
            parser.parse(serializedRecord(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                new ApiMessageAndVersion(
                    new ConsumerGroupMemberMetadataValue(),
                    (short) 0
                )
            )),
            Optional.of("Error at offset 0, skipping. Could not read record with version 0 from value's buffer due to: " +
                "Error reading byte array of 536870911 byte(s): only 1 byte(s) available."),
            Optional.empty()
        );
    }

    @Test
    public void testTransactionLogMessageParser() {
        DumpLogSegments.TransactionLogMessageParser parser = new DumpLogSegments.TransactionLogMessageParser();

        // The key is mandatory.
        assertEquals(
            "Failed to decode message at offset 0 using the specified decoder (message had a missing key)",
            assertThrows(RuntimeException.class, () ->
                parser.parse(TestUtils.singletonRecords(null, null, Compression.NONE, RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE).records().iterator().next())
            ).getMessage()
        );

        // A valid key and value should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new TransactionLogKey().setTransactionalId("txnId"),
                new ApiMessageAndVersion(
                    new TransactionLogValue().setProducerId(123L),
                    (short) 0
                )
            )),
            Optional.of("{\"type\":\"0\",\"data\":{\"transactionalId\":\"txnId\"}}"),
            Optional.of("{\"version\":\"0\",\"data\":{\"producerId\":123,\"producerEpoch\":0,\"transactionTimeoutMs\":0," +
                "\"transactionStatus\":0,\"transactionPartitions\":[],\"transactionLastUpdateTimestampMs\":0," +
                "\"transactionStartTimestampMs\":0}}")
        );

        // A valid key with a tombstone should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new TransactionLogKey().setTransactionalId("txnId"),
                null
            )),
            Optional.of("{\"type\":\"0\",\"data\":{\"transactionalId\":\"txnId\"}}"),
            Optional.of("<DELETE>")
        );

        // An unknown record type should be handled and reported as such.
        assertParseResult(
            parser.parse(
                TestUtils.singletonRecords(
                    new byte[0],
                    ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array(),
                    Compression.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE
                ).records().iterator().next()
            ),
            Optional.of("Unknown record type 32767 at offset 0, skipping."),
            Optional.empty()
        );

        // A valid key and value with all fields set should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new TransactionLogKey().setTransactionalId("txnId"),
                new ApiMessageAndVersion(
                    new TransactionLogValue()
                        .setClientTransactionVersion((short) 0)
                        .setNextProducerId(10L)
                        .setPreviousProducerId(11L)
                        .setProducerEpoch((short) 2)
                        .setProducerId(12L)
                        .setTransactionLastUpdateTimestampMs(123L)
                        .setTransactionPartitions(List.of(
                            new TransactionLogValue.PartitionsSchema()
                                .setTopic("topic1")
                                .setPartitionIds(List.of(0, 1, 2)),
                            new TransactionLogValue.PartitionsSchema()
                                .setTopic("topic2")
                                .setPartitionIds(List.of(3, 4, 5))
                        ))
                        .setTransactionStartTimestampMs(13L)
                        .setTransactionStatus((byte) 0)
                        .setTransactionTimeoutMs(14),
                    (short) 1
                )
            )),
            Optional.of("{\"type\":\"0\",\"data\":{\"transactionalId\":\"txnId\"}}"),
            Optional.of("{\"version\":\"1\",\"data\":{\"producerId\":12,\"previousProducerId\":11,\"nextProducerId\":10," +
                "\"producerEpoch\":2,\"transactionTimeoutMs\":14,\"transactionStatus\":0," +
                "\"transactionPartitions\":[{\"topic\":\"topic1\",\"partitionIds\":[0,1,2]}," +
                "{\"topic\":\"topic2\",\"partitionIds\":[3,4,5]}],\"transactionLastUpdateTimestampMs\":123," +
                "\"transactionStartTimestampMs\":13}}")
        );
    }

    private Optional<String> readBatchMetadata(ListIterator<String> lines) {
        while (lines.hasNext()) {
            String line = lines.next();
            if (line.startsWith("|")) {
                throw new IllegalStateException("Read unexpected record entry");
            } else if (line.startsWith("baseOffset")) {
                return Optional.of(line);
            }
        }
        return Optional.empty();
    }

    // Returns the total bytes of the batches specified
    private int readPartialBatchesBytes(ListIterator<String> lines, int limit) {
        int batchesBytes = 0;
        int batchesCounter = 0;
        while (lines.hasNext()) {
            if (batchesCounter >= limit) {
                return batchesBytes;
            }
            String line = lines.next();
            if (line.startsWith("baseOffset")) {
                Matcher matcher = SIZE_PATTERN.matcher(line);
                if (matcher.matches()) {
                    batchesBytes += Integer.parseInt(matcher.group(1));
                } else {
                    throw new IllegalStateException("Failed to parse and find size value for batch line: " + line);
                }
                batchesCounter += 1;
            }
        }
        return batchesBytes;
    }

    private int countBatches(ListIterator<String> lines) {
        int countBatches = 0;
        while (lines.hasNext()) {
            String line = lines.next();
            if (line.startsWith("baseOffset")) {
                countBatches += 1;
            }
        }
        return countBatches;
    }

    private List<String> readBatchRecords(ListIterator<String> lines) {
        List<String> records = new ArrayList<>();
        while (lines.hasNext()) {
            String line = lines.next();
            if (line.startsWith("|")) {
                records.add(line.substring(1));
            } else {
                lines.previous();
                return records;
            }
        }
        return records;
    }

    private Map<String, String> parseMetadataFields(String line) {
        Map<String, String> fields = new HashMap<>();
        Iterator<String> tokens = Arrays.stream(line.split("\\s+"))
            .map(String::trim)
            .filter(token -> !token.isEmpty())
            .iterator();

        while (tokens.hasNext()) {
            String token = tokens.next();
            if (!token.endsWith(":")) {
                throw new IllegalStateException("Unexpected non-field token " + token);
            }

            String field = token.substring(0, token.length() - 1);
            if (!tokens.hasNext()) {
                throw new IllegalStateException("Failed to parse value for " + field);
            }

            String value = tokens.next();
            fields.put(field, value);
        }

        return fields;
    }

    private void assertDumpLogRecordMetadata(UnifiedLog log) throws Exception {
        FetchDataInfo logReadInfo = log.read(0, Integer.MAX_VALUE, FetchIsolation.LOG_END, true);

        String output = runDumpLogSegments(new String[] {"--deep-iteration", "--files", logFilePath});
        ListIterator<String> lines = Arrays.asList(output.split("\n")).listIterator();

        for (RecordBatch batch : logReadInfo.records.batches()) {
            Optional<String> parsedBatchOpt = readBatchMetadata(lines);
            assertTrue(parsedBatchOpt.isPresent());

            Map<String, String> parsedBatch = parseMetadataFields(parsedBatchOpt.get());
            assertEquals(Optional.of(batch.baseOffset()), optionalLong(parsedBatch.get("baseOffset")));
            assertEquals(Optional.of(batch.lastOffset()), optionalLong(parsedBatch.get("lastOffset")));
            assertEquals(Optional.ofNullable(batch.countOrNull()).map(Integer::longValue), optionalLong(parsedBatch.get("count")));
            assertEquals(Optional.of(batch.partitionLeaderEpoch()), optionalInt(parsedBatch.get("partitionLeaderEpoch")));
            assertEquals(Optional.of(batch.isTransactional()), optionalBoolean(parsedBatch.get("isTransactional")));
            assertEquals(Optional.of(batch.isControlBatch()), optionalBoolean(parsedBatch.get("isControl")));
            assertEquals(Optional.of(batch.producerId()), optionalLong(parsedBatch.get("producerId")));
            assertEquals(Optional.of(batch.producerEpoch()), optionalShort(parsedBatch.get("producerEpoch")));
            assertEquals(Optional.of(batch.baseSequence()), optionalInt(parsedBatch.get("baseSequence")));
            assertEquals(Optional.of(batch.compressionType().name), Optional.ofNullable(parsedBatch.get("compresscodec")));

            Iterator<String> parsedRecordIter = readBatchRecords(lines).iterator();
            for (Record record : batch) {
                assertTrue(parsedRecordIter.hasNext());
                Map<String, String> parsedRecord = parseMetadataFields(parsedRecordIter.next());
                assertEquals(Optional.of(record.offset()), optionalLong(parsedRecord.get("offset")));
                assertEquals(Optional.of(record.keySize()), optionalInt(parsedRecord.get("keySize")));
                assertEquals(Optional.of(record.valueSize()), optionalInt(parsedRecord.get("valueSize")));
                assertEquals(Optional.of(record.timestamp()), optionalLong(parsedRecord.get(batch.timestampType().name)));

                if (batch.magic() >= RecordVersion.V2.value) {
                    assertEquals(Optional.of(record.sequence()), optionalInt(parsedRecord.get("sequence")));
                }

                // Batch fields should not be present in the record output
                assertFalse(parsedRecord.containsKey("baseOffset"));
                assertFalse(parsedRecord.containsKey("lastOffset"));
                assertFalse(parsedRecord.containsKey("partitionLeaderEpoch"));
                assertFalse(parsedRecord.containsKey("producerId"));
                assertFalse(parsedRecord.containsKey("producerEpoch"));
                assertFalse(parsedRecord.containsKey("baseSequence"));
                assertFalse(parsedRecord.containsKey("isTransactional"));
                assertFalse(parsedRecord.containsKey("isControl"));
                assertFalse(parsedRecord.containsKey("compresscodec"));
            }
        }
    }

    @Test
    public void testShareGroupStateMessageParser() {
        DumpLogSegments.ShareGroupStateMessageParser parser = new DumpLogSegments.ShareGroupStateMessageParser();
        long timestamp = System.currentTimeMillis();

        // The key is mandatory.
        assertEquals(
            "Failed to decode message at offset 0 using the specified decoder (message had a missing key)",
            assertThrows(RuntimeException.class, () ->
                parser.parse(TestUtils.singletonRecords(null, null, Compression.NONE, RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE).records().iterator().next())
            ).getMessage()
        );

        // A valid key and value should work (ShareSnapshot).
        assertParseResult(
            parser.parse(serializedRecord(
                new ShareSnapshotKey()
                    .setGroupId("gs1")
                    .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
                    .setPartition(0),
                new ApiMessageAndVersion(new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStartOffset(0)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(4)
                            .setDeliveryState((byte) 2)
                            .setDeliveryCount((short) 1)
                    )),
                    (short) 0)
            )),
            Optional.of("{\"type\":\"0\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
            Optional.of(String.format("{\"version\":\"0\",\"data\":{\"snapshotEpoch\":0,\"stateEpoch\":0,\"leaderEpoch\":0," +
                "\"startOffset\":0,\"createTimestamp\":%d,\"writeTimestamp\":%d,\"stateBatches\":[{" +
                "\"firstOffset\":0,\"lastOffset\":4,\"deliveryState\":2,\"deliveryCount\":1}]}}",
                timestamp, timestamp))
        );

        // A valid key and value should work (ShareUpdate).
        assertParseResult(
            parser.parse(serializedRecord(
                new ShareUpdateKey()
                    .setGroupId("gs1")
                    .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
                    .setPartition(0),
                new ApiMessageAndVersion(new ShareUpdateValue()
                    .setSnapshotEpoch(0)
                    .setLeaderEpoch(0)
                    .setStartOffset(0)
                    .setStateBatches(List.of(
                        new ShareUpdateValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(4)
                            .setDeliveryState((byte) 2)
                            .setDeliveryCount((short) 1)
                    )),
                    (short) 0)
            )),
            Optional.of("{\"type\":\"1\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
            Optional.of("{\"version\":\"0\",\"data\":{\"snapshotEpoch\":0,\"leaderEpoch\":0,\"startOffset\":0," +
                "\"stateBatches\":[{\"firstOffset\":0,\"lastOffset\":4,\"deliveryState\":2,\"deliveryCount\":1}]}}")
        );

        // A valid key with a tombstone should work.
        assertParseResult(
            parser.parse(serializedRecord(
                new ShareSnapshotKey()
                    .setGroupId("gs1")
                    .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
                    .setPartition(0),
                null
            )),
            Optional.of("{\"type\":\"0\",\"data\":{\"groupId\":\"gs1\",\"topicId\":\"Uj5wn_FqTXirEASvVZRY1w\",\"partition\":0}}"),
            Optional.of("<DELETE>")
        );

        // An unknown record type should be handled and reported as such.
        assertParseResult(
            parser.parse(
                TestUtils.singletonRecords(
                    new byte[0],
                    ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array(),
                    Compression.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE
                ).records().iterator().next()
            ),
            Optional.of("Unknown record type 32767 at offset 0, skipping."),
            Optional.empty()
        );

        // Any parsing error is swallowed and reported.
        assertParseResult(
            parser.parse(serializedRecord(
                new ShareUpdateKey()
                    .setGroupId("group")
                    .setTopicId(Uuid.fromString("Uj5wn_FqTXirEASvVZRY1w"))
                    .setPartition(0),
                new ApiMessageAndVersion(
                    new ShareSnapshotValue(),
                    (short) 0
                )
            )),
            Optional.of("Error at offset 0, skipping. Could not read record with version 0 from value's buffer due to: " +
                "non-nullable field stateBatches was serialized as null."),
            Optional.empty()
        );
    }

    private MetadataLogConfig createMetadataLogConfig(
        int internalLogSegmentBytes,
        long logSegmentMillis,
        long retentionMaxBytes,
        long retentionMillis
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(MetadataLogConfig.INTERNAL_METADATA_LOG_SEGMENT_BYTES_CONFIG, internalLogSegmentBytes);
        config.put(MetadataLogConfig.METADATA_LOG_SEGMENT_MILLIS_CONFIG, logSegmentMillis);
        config.put(MetadataLogConfig.METADATA_MAX_RETENTION_BYTES_CONFIG, retentionMaxBytes);
        config.put(MetadataLogConfig.METADATA_MAX_RETENTION_MILLIS_CONFIG, retentionMillis);
        return new MetadataLogConfig(new AbstractConfig(MetadataLogConfig.CONFIG_DEF, config, false));
    }

    private void verifyRecordsInOutput(List<BatchInfo> batches, boolean checkKeysAndValues, String[] args) {
        String output = runDumpLogSegments(args);
        String[] lines = output.split("\n");
        assertTrue(lines.length > 2, "Data not printed: " + output);

        int totalRecords = 0;
        for (BatchInfo batchInfo : batches) {
            totalRecords += batchInfo.records.size();
        }

        int offset = 0;
        Iterator<BatchInfo> batchIterator = batches.iterator();
        BatchInfo batch = null;

        for (int index = 0; index < totalRecords + batches.size(); index++) {
            String line = lines[lines.length - totalRecords - batches.size() + index];
            // The base offset of the batch is the offset of the first record in the batch, so we
            // only increment the offset if it's not a batch
            if (isBatch(batches, index)) {
                assertTrue(line.startsWith("baseOffset: " + offset + " lastOffset: "),
                    "Not a valid batch-level message record: " + line);
                batch = batchIterator.next();
            } else {
                assertTrue(line.startsWith(DumpLogSegments.RECORD_INDENT + " offset: " + offset),
                    "Not a valid message record: " + line);
                if (checkKeysAndValues) {
                    StringBuilder suffix = new StringBuilder("headerKeys: []");
                    if (batch.hasKeys) {
                        suffix.append(" key: message key ").append(offset);
                    }
                    if (batch.hasValues) {
                        suffix.append(" payload: message value ").append(offset);
                    }
                    assertTrue(line.endsWith(suffix.toString()), "Message record missing key or value: " + line);
                }
                offset += 1;
            }
        }
    }

    private void verifyNoRecordsInOutput(String[] args) {
        String output = runDumpLogSegments(args);
        assertFalse(output.matches("(?s).*offset: [0-9]* isvalid.*"), "Data should not have been printed: " + output);
    }

    private boolean isBatch(List<BatchInfo> batches, int index) {
        int i = 0;
        for (BatchInfo batch : batches) {
            if (i == index) {
                return true;
            }
            i += 1;
            for (int recordIndex = 0; recordIndex < batch.records.size(); recordIndex++) {
                if (i == index) {
                    return false;
                }
                i += 1;
            }
        }
        throw new AssertionError("No match for index " + index);
    }

    private String runDumpLogSegments(String[] args) {
        return captureStandardOut(() -> {
            try {
                DumpLogSegments.main(args);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Optional<Long> optionalLong(String value) {
        if (value == null || "null".equals(value)) {
            return Optional.empty();
        }
        return Optional.of(Long.parseLong(value));
    }

    private Optional<Integer> optionalInt(String value) {
        if (value == null || "null".equals(value)) {
            return Optional.empty();
        }
        return Optional.of(Integer.parseInt(value));
    }

    private Optional<Short> optionalShort(String value) {
        if (value == null || "null".equals(value)) {
            return Optional.empty();
        }
        return Optional.of(Short.parseShort(value));
    }

    private Optional<Boolean> optionalBoolean(String value) {
        if (value == null || "null".equals(value)) {
            return Optional.empty();
        }
        return Optional.of(Boolean.parseBoolean(value));
    }

    private void assertParseResult(
        DumpLogSegments.ParseResult<String, String> result,
        Optional<String> expectedKey,
        Optional<String> expectedValue
    ) {
        assertEquals(expectedKey, result.key());
        assertEquals(expectedValue, result.value());
    }

    private VoterSet createVoterSet() {
        Map<Integer, InetSocketAddress> voters = Map.of(
            1, InetSocketAddress.createUnresolved("localhost", 9991),
            2, InetSocketAddress.createUnresolved("localhost", 9992),
            3, InetSocketAddress.createUnresolved("localhost", 9993)
        );
        return VoterSet.fromInetSocketAddresses(ListenerName.normalised("LISTENER"), voters);
    }

    @Test
    public void testLegacyRecordBatchOutputFormat() throws Exception {
        // Create a legacy format log file directly (bypassing UnifiedLog which auto-upgrades to V2)
        // Must use numeric naming format expected by DumpLogSegments
        File legacyLogFile = new File(logDir, "00000000000000001000.log");

        MemoryRecords legacyRecords = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
            Compression.NONE, TimestampType.CREATE_TIME,
            new SimpleRecord(time.milliseconds(), "key1".getBytes(), "value1".getBytes()),
            new SimpleRecord(time.milliseconds(), "key2".getBytes(), "value2".getBytes())
        );

        // Write the legacy records directly to a file (create it first)
        try (FileRecords fileRecords = FileRecords.open(legacyLogFile, true)) {
            fileRecords.append(legacyRecords);
            fileRecords.flush();
        }

        // Dump the legacy log file
        String output = runDumpLogSegments(new String[] {"--deep-iteration", "--files", legacyLogFile.getAbsolutePath()});

        // Verify the output contains legacy batch fields
        assertTrue(output.contains("Log starting offset:"), "Output should contain log starting offset");

        // For legacy batches, the wrapper record is an AbstractLegacyRecordBatch
        // and should output "isValid:" and "crc:" fields
        assertTrue(output.contains("isValid:"),
            "Output should contain 'isValid:' field for legacy batches. Output:\n" + output);
        assertTrue(output.contains("crc:"),
            "Output should contain 'crc:' field for legacy batches. Output:\n" + output);

        // Critical: Verify no unmatched closing braces in the output
        // Count all braces in the entire output
        long openBraces = output.chars().filter(ch -> ch == '{').count();
        long closeBraces = output.chars().filter(ch -> ch == '}').count();

        assertEquals(openBraces, closeBraces,
            "Output should have balanced braces (no unmatched closing brace). " +
            "Found " + openBraces + " '{' and " + closeBraces + " '}'");
    }
}
