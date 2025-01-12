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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class LocalLogTest {

    private static final MockTime MOCK_TIME = new MockTime();

    private final File tmpDir = TestUtils.tempDirectory();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private final TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    private final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);

    private LocalLog log;

    @BeforeEach
    public void setUp() throws IOException {
        log = createLocalLogWithActiveSegment(logDir, new LogConfig(new Properties()));
    }

    @AfterEach
    public void tearDown() throws IOException {
        try {
            log.close();
        } catch (KafkaStorageException kse) {
            // ignore
        }
    }

    record KeyValue(String key, String value) {

        SimpleRecord toRecord(long timestamp) {
            return new SimpleRecord(timestamp, key.getBytes(), value.getBytes());
        }

        SimpleRecord toRecord() {
            return new SimpleRecord(MOCK_TIME.milliseconds(), key.getBytes(), value.getBytes());
        }

        static KeyValue fromRecord(Record record) {
            String key = record.hasKey()
                ? StandardCharsets.UTF_8.decode(record.key()).toString()
                : "";
            String value = record.hasValue()
                ? StandardCharsets.UTF_8.decode(record.value()).toString()
                : "";
            return new KeyValue(key, value);
        }
    }

    private List<SimpleRecord> kvsToRecords(List<KeyValue> keyValues) {
        return keyValues.stream().map(KeyValue::toRecord).collect(Collectors.toList());
    }

    private List<KeyValue> recordsToKvs(Iterable<Record> records) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (Record record : records) {
            keyValues.add(KeyValue.fromRecord(record));
        }
        return keyValues;
    }

    private void appendRecords(List<SimpleRecord> records, long initialOffset) throws IOException {
        log.append(initialOffset + records.size() - 1,
                records.get(0).timestamp(),
                initialOffset,
                MemoryRecords.withRecords(initialOffset, Compression.NONE, 0, records.toArray(new SimpleRecord[0])));
    }

    private FetchDataInfo readRecords(long startOffset) throws IOException {
        return readRecords(
                startOffset,
                log.segments().activeSegment().size(),
                log.logEndOffsetMetadata()
        );
    }

    private FetchDataInfo readRecords(int maxLength) throws IOException {
        return readRecords(
                0L,
                maxLength,
                log.logEndOffsetMetadata()
        );
    }

    private FetchDataInfo readRecords(long startOffset, LogOffsetMetadata maxOffsetMetadata) throws IOException {
        return readRecords(
                startOffset,
                log.segments().activeSegment().size(),
                maxOffsetMetadata
        );
    }

    private FetchDataInfo readRecords(
                            long startOffset,
                            int maxLength,
                            LogOffsetMetadata maxOffsetMetadata) throws IOException {
        return log.read(startOffset,
                maxLength,
                false,
                maxOffsetMetadata,
                false);
    }

    @Test
    public void testLogDeleteSegmentsSuccess() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        log.roll(0L);
        assertEquals(2, log.segments().numberOfSegments());
        assertNotEquals(0, logDir.listFiles().length);
        List<LogSegment> segmentsBeforeDelete = new ArrayList<>(log.segments().values());
        List<LogSegment> deletedSegments = log.deleteAllSegments();
        assertTrue(log.segments().isEmpty());
        assertEquals(segmentsBeforeDelete, deletedSegments);
        assertThrows(KafkaStorageException.class, () -> log.checkIfMemoryMappedBufferClosed());
        assertTrue(logDir.exists());
    }

    @Test
    public void testRollEmptyActiveSegment() {
        LogSegment oldActiveSegment = log.segments().activeSegment();
        log.roll(0L);
        assertEquals(1, log.segments().numberOfSegments());
        assertNotEquals(oldActiveSegment, log.segments().activeSegment());
        assertNotEquals(0, logDir.listFiles().length);
        assertTrue(oldActiveSegment.hasSuffix(LogFileUtils.DELETED_FILE_SUFFIX));
    }

    @Test
    public void testLogDeleteDirSuccessWhenEmptyAndFailureWhenNonEmpty() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        log.roll(0L);
        assertEquals(2, log.segments().numberOfSegments());
        assertNotEquals(0, logDir.listFiles().length);

        assertThrows(IllegalStateException.class, () -> log.deleteEmptyDir());
        assertTrue(logDir.exists());

        log.deleteAllSegments();
        log.deleteEmptyDir();
        assertFalse(logDir.exists());
    }

    @Test
    public void testUpdateConfig() {
        LogConfig oldConfig = log.config();
        assertEquals(oldConfig, log.config());

        Properties props = new Properties();
        props.put(TopicConfig.SEGMENT_BYTES_CONFIG, oldConfig.segmentSize + 1);
        LogConfig newConfig = new LogConfig(props);
        log.updateConfig(newConfig);
        assertEquals(newConfig, log.config());
    }

    @Test
    public void testLogDirRenameToNewDir() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        log.roll(0L);
        assertEquals(2, log.segments().numberOfSegments());
        File newLogDir = TestUtils.randomPartitionLogDir(tmpDir);
        assertTrue(log.renameDir(newLogDir.getName()));
        assertFalse(logDir.exists());
        assertTrue(newLogDir.exists());
        assertEquals(newLogDir, log.dir());
        assertEquals(newLogDir.getParent(), log.parentDir());
        assertEquals(newLogDir.getParent(), log.dir().getParent());
        log.segments().values().forEach(segment -> assertEquals(newLogDir.getPath(), segment.log().file().getParentFile().getPath()));
        assertEquals(2, log.segments().numberOfSegments());
    }

    @Test
    public void testLogDirRenameToExistingDir() {
        assertFalse(log.renameDir(log.dir().getName()));
    }

    @Test
    public void testLogFlush() throws IOException {
        assertEquals(0L, log.recoveryPoint());
        assertEquals(MOCK_TIME.milliseconds(), log.lastFlushTime());

        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        MOCK_TIME.sleep(1);
        LogSegment newSegment = log.roll(0L);
        log.flush(newSegment.baseOffset());
        log.markFlushed(newSegment.baseOffset());
        assertEquals(1L, log.recoveryPoint());
        assertEquals(MOCK_TIME.milliseconds(), log.lastFlushTime());
    }

    @Test
    public void testLogAppend() throws IOException {
        FetchDataInfo fetchDataInfoBeforeAppend = readRecords(1);
        assertFalse(fetchDataInfoBeforeAppend.records.records().iterator().hasNext());

        MOCK_TIME.sleep(1);
        List<KeyValue> keyValues = List.of(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), 0L);
        assertEquals(2L, log.logEndOffset());
        assertEquals(0L, log.recoveryPoint());
        FetchDataInfo fetchDataInfo = readRecords(0L);
        assertEquals(2L, Utils.toList(fetchDataInfo.records.records()).size());
        assertEquals(keyValues, recordsToKvs(fetchDataInfo.records.records()));
    }

    @Test
    public void testLogCloseSuccess() throws IOException {
        List<KeyValue> keyValues = List.of(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), 0L);
        log.close();
        assertThrows(ClosedChannelException.class, () -> appendRecords(kvsToRecords(keyValues), 2L));
    }

    @Test
    public void testLogCloseIdempotent() {
        log.close();
        // Check that LocalLog.close() is idempotent
        log.close();
    }

    @Test
    public void testLogCloseFailureWhenInMemoryBufferClosed() throws IOException {
        List<KeyValue> keyValues = List.of(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), 0L);
        log.closeHandlers();
        assertThrows(KafkaStorageException.class, () -> log.close());
    }

    @Test
    public void testLogCloseHandlers() throws IOException {
        List<KeyValue> keyValues = List.of(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), 0L);
        log.closeHandlers();
        assertThrows(ClosedChannelException.class, () -> appendRecords(kvsToRecords(keyValues), 2L));
    }

    @Test
    public void testLogCloseHandlersIdempotent() {
        log.closeHandlers();
        // Check that LocalLog.closeHandlers() is idempotent
        log.closeHandlers();
    }

    static class TestDeletionReason implements SegmentDeletionReason {
        private Collection<LogSegment> deletedSegments = new ArrayList<>();

        @Override
        public void logReason(List<LogSegment> toDelete) {
            deletedSegments = new ArrayList<>(toDelete);
        }

        Collection<LogSegment> deletedSegments() {
            return deletedSegments;
        }
    }

    private void testRemoveAndDeleteSegments(boolean asyncDelete) throws IOException {
        for (int offset = 0; offset < 9; offset++) {
            SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
            appendRecords(List.of(record), offset);
            log.roll(0L);
        }

        assertEquals(10L, log.segments().numberOfSegments());


        TestDeletionReason reason = new TestDeletionReason();
        List<LogSegment> toDelete = new ArrayList<>(log.segments().values());
        log.removeAndDeleteSegments(toDelete, asyncDelete, reason);
        if (asyncDelete) {
            MOCK_TIME.sleep(log.config().fileDeleteDelayMs + 1);
        }
        assertTrue(log.segments().isEmpty());
        assertEquals(toDelete, reason.deletedSegments());
        toDelete.forEach(segment -> assertTrue(segment.deleted()));
    }

    @Test
    public void testRemoveAndDeleteSegmentsSync() throws IOException {
        testRemoveAndDeleteSegments(false);
    }

    @Test
    public void testRemoveAndDeleteSegmentsAsync() throws IOException {
        testRemoveAndDeleteSegments(true);
    }

    private void testDeleteSegmentFiles(boolean asyncDelete) throws IOException {
        for (int offset = 0; offset < 9; offset++) {
            SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
            appendRecords(List.of(record), offset);
            log.roll(0L);
        }

        assertEquals(10L, log.segments().numberOfSegments());

        Collection<LogSegment> toDelete = log.segments().values();
        LocalLog.deleteSegmentFiles(toDelete, asyncDelete, log.dir(), log.topicPartition(), log.config(), log.scheduler(), log.logDirFailureChannel(), "");
        if (asyncDelete) {
            toDelete.forEach(segment -> {
                assertFalse(segment.deleted());
                assertTrue(segment.hasSuffix(LogFileUtils.DELETED_FILE_SUFFIX));
            });
            MOCK_TIME.sleep(log.config().fileDeleteDelayMs + 1);
        }
        toDelete.forEach(segment -> assertTrue(segment.deleted()));
    }

    @Test
    public void testDeleteSegmentFilesSync() throws IOException {
        testDeleteSegmentFiles(false);
    }

    @Test
    public void testDeleteSegmentFilesAsync() throws IOException {
        testDeleteSegmentFiles(true);
    }

    @Test
    public void testCreateAndDeleteSegment() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        long newOffset = log.segments().activeSegment().baseOffset() + 1;
        LogSegment oldActiveSegment = log.segments().activeSegment();
        LogSegment newActiveSegment = log.createAndDeleteSegment(newOffset, log.segments().activeSegment(), true, new LogTruncation(log.logger()));
        assertEquals(1, log.segments().numberOfSegments());
        assertEquals(newActiveSegment, log.segments().activeSegment());
        assertNotEquals(oldActiveSegment, log.segments().activeSegment());
        assertTrue(oldActiveSegment.hasSuffix(LogFileUtils.DELETED_FILE_SUFFIX));
        assertEquals(newOffset, log.segments().activeSegment().baseOffset());
        assertEquals(0L, log.recoveryPoint());
        assertEquals(newOffset, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(newOffset);
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
    }

    @Test
    public void testTruncateFullyAndStartAt() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        for (int offset = 0; offset < 8; offset++) {
            appendRecords(List.of(record), offset);
            if (offset % 2 != 0)
                log.roll(0L);
        }
        for (int offset = 8; offset < 13; offset++) {
            SimpleRecord r = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
            appendRecords(List.of(r), offset);
        }
        assertEquals(5, log.segments().numberOfSegments());
        assertNotEquals(10L, log.segments().activeSegment().baseOffset());
        List<LogSegment> expected = new ArrayList<>(log.segments().values());
        List<LogSegment> deleted = log.truncateFullyAndStartAt(10L);
        assertEquals(expected, deleted);
        assertEquals(1, log.segments().numberOfSegments());
        assertEquals(10L, log.segments().activeSegment().baseOffset());
        assertEquals(0L, log.recoveryPoint());
        assertEquals(10L, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(10L);
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
    }

    @Test
    public void testWhenFetchOffsetHigherThanMaxOffset() throws IOException {
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        for (int offset = 0; offset < 5; offset++) {
            appendRecords(List.of(record), offset);
            if (offset % 2 != 0)
                log.roll(0L);
        }
        assertEquals(3, log.segments().numberOfSegments());

        // case-0: valid case, `startOffset` < `maxOffsetMetadata.offset`
        var fetchDataInfo = readRecords(3L, new LogOffsetMetadata(4L, 4L, 0));
        assertEquals(1, Utils.toList(fetchDataInfo.records.records()).size());
        assertEquals(new LogOffsetMetadata(3, 2L, 69), fetchDataInfo.fetchOffsetMetadata);

        // case-1: `startOffset` == `maxOffsetMetadata.offset`
        fetchDataInfo = readRecords(4L, new LogOffsetMetadata(4L, 4L, 0));
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
        assertEquals(new LogOffsetMetadata(4L, 4L, 0), fetchDataInfo.fetchOffsetMetadata);

        // case-2: `startOffset` > `maxOffsetMetadata.offset`
        fetchDataInfo = readRecords(5L, new LogOffsetMetadata(4L, 4L, 0));
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
        assertEquals(new LogOffsetMetadata(5L, 4L, 69), fetchDataInfo.fetchOffsetMetadata);

        // case-3: `startOffset` < `maxMessageOffset.offset` but `maxMessageOffset.messageOnlyOffset` is true
        fetchDataInfo = readRecords(3L, new LogOffsetMetadata(4L, -1L, -1));
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
        assertEquals(new LogOffsetMetadata(3L, 2L, 69), fetchDataInfo.fetchOffsetMetadata);

        // case-4: `startOffset` < `maxMessageOffset.offset`, `maxMessageOffset.messageOnlyOffset` is false, but
        // `maxOffsetMetadata.segmentBaseOffset` < `startOffset.segmentBaseOffset`
        fetchDataInfo = readRecords(3L, new LogOffsetMetadata(4L, 0L, 40));
        assertFalse(fetchDataInfo.records.records().iterator().hasNext());
        assertEquals(new LogOffsetMetadata(3L, 2L, 69), fetchDataInfo.fetchOffsetMetadata);
    }

    @Test
    public void testTruncateTo() throws IOException {
        for (int offset = 0; offset < 12; offset++) {
            SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
            appendRecords(List.of(record), offset);
            if (offset % 3 == 2)
                log.roll(0L);
        }
        assertEquals(5, log.segments().numberOfSegments());
        assertEquals(12L, log.logEndOffset());

        List<LogSegment> expected = new ArrayList<>(log.segments().values(9L, log.logEndOffset() + 1));
        // Truncate to an offset before the base offset of the active segment
        Collection<LogSegment> deleted = log.truncateTo(7L);
        assertEquals(expected, deleted);
        assertEquals(3, log.segments().numberOfSegments());
        assertEquals(6L, log.segments().activeSegment().baseOffset());
        assertEquals(0L, log.recoveryPoint());
        assertEquals(7L, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(6L);
        assertEquals(1, Utils.toList(fetchDataInfo.records.records()).size());
        assertEquals(List.of(new KeyValue("", "a")), recordsToKvs(fetchDataInfo.records.records()));

        // Verify that we can still append to the active segment
        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 7L);
        assertEquals(8L, log.logEndOffset());
    }

    @Test
    public void testNonActiveSegmentsFrom() throws IOException {
        for (int i = 0; i < 5; i++) {
            List<KeyValue> keyValues = List.of(new KeyValue(String.valueOf(i), String.valueOf(i)));
            appendRecords(kvsToRecords(keyValues), i);
            log.roll(0L);
        }

        assertEquals(5L, log.segments().activeSegment().baseOffset());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), nonActiveBaseOffsetsFrom(0L));
        assertEquals(List.of(), nonActiveBaseOffsetsFrom(5L));
        assertEquals(List.of(2L, 3L, 4L), nonActiveBaseOffsetsFrom(2L));
        assertEquals(List.of(), nonActiveBaseOffsetsFrom(6L));
    }

    private List<Long> nonActiveBaseOffsetsFrom(long startOffset) {
        return log.segments().nonActiveLogSegmentsFrom(startOffset).stream()
                .map(LogSegment::baseOffset)
                .collect(Collectors.toList());
    }

    private String topicPartitionName(String topic, String partition) {
        return topic + "-" + partition;
    }

    @Test
    public void testParseTopicPartitionName() throws IOException {
        String topic = "test_topic";
        String partition = "143";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        TopicPartition topicPartition = LocalLog.parseTopicPartitionName(dir);
        assertEquals(topic, topicPartition.topic());
        assertEquals(Integer.parseInt(partition), topicPartition.partition());
    }

    /**
     * Tests that log directories with a period in their name that have been marked for deletion
     * are parsed correctly by `Log.parseTopicPartitionName` (see KAFKA-5232 for details).
     */
    @Test
    public void testParseTopicPartitionNameWithPeriodForDeletedTopic() throws IOException {
        String topic = "foo.bar-testtopic";
        String partition = "42";
        File dir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, Integer.parseInt(partition))));
        TopicPartition topicPartition = LocalLog.parseTopicPartitionName(dir);
        assertEquals(topic, topicPartition.topic(), "Unexpected topic name parsed");
        assertEquals(Integer.parseInt(partition), topicPartition.partition(), "Unexpected partition number parsed");
    }

    @Test
    public void testParseTopicPartitionNameForEmptyName() throws IOException {
        File dir = new File("");
        String msg = "KafkaException should have been thrown for dir: " + dir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir), msg);
    }

    @Test
    public void testParseTopicPartitionNameForNull() {
        File dir = null;
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + dir);
    }

    @Test
    public void testParseTopicPartitionNameForMissingSeparator() throws IOException {
        String topic = "test_topic";
        String partition = "1999";
        File dir = new File(logDir, topic + partition);
        String msg = "KafkaException should have been thrown for dir: " + dir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir), msg);
        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topic + partition + "." + LogFileUtils.DELETE_DIR_SUFFIX);
        msg = "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir), msg);
    }

    @Test
    public void testParseTopicPartitionNameForMissingTopic() throws IOException {
        String topic = "";
        String partition = "1999";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        String msg = "KafkaException should have been thrown for dir: " + dir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir), msg);

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, Integer.parseInt(partition))));

        msg = "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir), msg);
    }

    @Test
    public void testParseTopicPartitionNameForMissingPartition() throws IOException {
        String topic = "test_topic";
        String partition = "";
        File dir = new File(logDir.getPath() + topicPartitionName(topic, partition));
        String msg = "KafkaException should have been thrown for dir: " + dir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir), msg);

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topicPartitionName(topic, partition) + "." + LogFileUtils.DELETE_DIR_SUFFIX);
        msg = "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir), msg);
    }

    @Test
    public void testParseTopicPartitionNameForInvalidPartition() throws IOException {
        String topic = "test_topic";
        String partition = "1999a";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        String msg = "KafkaException should have been thrown for dir: " + dir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir), msg);

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topic + partition + "." + LogFileUtils.DELETE_DIR_SUFFIX);
        msg = "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir), msg);
    }

    @Test
    public void testParseTopicPartitionNameForExistingInvalidDir() throws IOException {
        File dir1 = new File(logDir.getPath() + "/non_kafka_dir");
        String msg = "KafkaException should have been thrown for dir: " + dir1.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir1), msg);
        File dir2 = new File(logDir.getPath() + "/non_kafka_dir-delete");
        msg = "KafkaException should have been thrown for dir: " + dir2.getCanonicalPath();
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir2), msg);
    }

    @Test
    public void testLogDeleteDirName() {
        String name1 = LocalLog.logDeleteDirName(new TopicPartition("foo", 3));
        assertTrue(name1.length() <= 255);
        assertTrue(Pattern.compile("foo-3\\.[0-9a-z]{32}-delete").matcher(name1).matches());
        assertTrue(LocalLog.DELETE_DIR_PATTERN.matcher(name1).matches());
        assertFalse(LocalLog.FUTURE_DIR_PATTERN.matcher(name1).matches());
        String name2 = LocalLog.logDeleteDirName(
                new TopicPartition("n" + String.join("", Collections.nCopies(248, "o")), 5));
        assertEquals(255, name2.length());
        assertTrue(Pattern.compile("n[o]{212}-5\\.[0-9a-z]{32}-delete").matcher(name2).matches());
        assertTrue(LocalLog.DELETE_DIR_PATTERN.matcher(name2).matches());
        assertFalse(LocalLog.FUTURE_DIR_PATTERN.matcher(name2).matches());
    }

    @Test
    public void testOffsetFromFile() {
        long offset = 23423423L;

        File logFile = LogFileUtils.logFile(tmpDir, offset);
        assertEquals(offset, LogFileUtils.offsetFromFile(logFile));

        File offsetIndexFile = LogFileUtils.offsetIndexFile(tmpDir, offset);
        assertEquals(offset, LogFileUtils.offsetFromFile(offsetIndexFile));

        File timeIndexFile = LogFileUtils.timeIndexFile(tmpDir, offset);
        assertEquals(offset, LogFileUtils.offsetFromFile(timeIndexFile));
    }

    @Test
    public void testRollSegmentThatAlreadyExists() throws IOException {
        assertEquals(1, log.segments().numberOfSegments(), "Log begins with a single empty segment.");

        // roll active segment with the same base offset of size zero should recreate the segment
        log.roll(0L);
        assertEquals(1, log.segments().numberOfSegments(), "Expect 1 segment after roll() empty segment with base offset.");

        // should be able to append records to active segment
        List<KeyValue> keyValues1 = List.of(new KeyValue("k1", "v1"));
        appendRecords(kvsToRecords(keyValues1), 0);
        assertEquals(0L, log.segments().activeSegment().baseOffset());
        // make sure we can append more records
        List<KeyValue> keyValues2 = List.of(new KeyValue("k2", "v2"));
        appendRecords(keyValues2.stream()
                .map(kv -> kv.toRecord(MOCK_TIME.milliseconds() + 10))
                .collect(Collectors.toList()),
                1L);
        assertEquals(2, log.logEndOffset(), "Expect two records in the log");
        FetchDataInfo readResult = readRecords(0L);
        assertEquals(2L, Utils.toList(readResult.records.records()).size());
        assertEquals(Stream.concat(keyValues1.stream(), keyValues2.stream()).collect(Collectors.toList()), recordsToKvs(readResult.records.records()));

        // roll so that active segment is empty
        log.roll(0L);
        assertEquals(2L, log.segments().activeSegment().baseOffset(), "Expect base offset of active segment to be LEO");
        assertEquals(2, log.segments().numberOfSegments(), "Expect two segments.");
        assertEquals(2L, log.logEndOffset());
    }

    @Test
    public void testNewSegmentsAfterRoll() throws IOException {
        assertEquals(1, log.segments().numberOfSegments(), "Log begins with a single empty segment.");

        // roll active segment with the same base offset of size zero should recreate the segment
        {
            LogSegment newSegment = log.roll(0L);
            assertEquals(0L, newSegment.baseOffset());
            assertEquals(1, log.segments().numberOfSegments());
            assertEquals(0L, log.logEndOffset());
        }

        appendRecords(List.of(new KeyValue("k1", "v1").toRecord()), 0L);

        {
            LogSegment newSegment = log.roll(0L);
            assertEquals(1L, newSegment.baseOffset());
            assertEquals(2, log.segments().numberOfSegments());
            assertEquals(1L, log.logEndOffset());
        }

        appendRecords(List.of(new KeyValue("k2", "v2").toRecord()), 1L);

        {
            LogSegment newSegment = log.roll(1L);
            assertEquals(2L, newSegment.baseOffset());
            assertEquals(3, log.segments().numberOfSegments());
            assertEquals(2L, log.logEndOffset());
        }
    }

    @Test
    public void testRollSegmentErrorWhenNextOffsetIsIllegal() throws IOException {
        assertEquals(1, log.segments().numberOfSegments(), "Log begins with a single empty segment.");

        List<KeyValue> keyValues = List.of(new KeyValue("k1", "v1"), new KeyValue("k2", "v2"), new KeyValue("k3", "v3"));
        appendRecords(kvsToRecords(keyValues), 0L);
        assertEquals(0L, log.segments().activeSegment().baseOffset());
        assertEquals(3, log.logEndOffset(), "Expect two records in the log");

        // roll to create an empty active segment
        log.roll(0L);
        assertEquals(3L, log.segments().activeSegment().baseOffset());

        // intentionally setup the logEndOffset to introduce an error later
        log.updateLogEndOffset(1L);

        // expect an error because of attempt to roll to a new offset (1L) that's lower than the
        // base offset (3L) of the active segment
        assertThrows(KafkaException.class, () -> log.roll(0L));
    }

    @Test
    public void testFlushingNonExistentDir() throws IOException {
        LocalLog spyLog = spy(log);

        SimpleRecord record = new SimpleRecord(MOCK_TIME.milliseconds(), "a".getBytes());
        appendRecords(List.of(record), 0L);
        MOCK_TIME.sleep(1);
        LogSegment newSegment = log.roll(0L);

        // simulate the directory is renamed concurrently
        doReturn(new File("__NON_EXISTENT__")).when(spyLog).dir();
        assertDoesNotThrow(() -> spyLog.flush(newSegment.baseOffset()));
    }

    private LocalLog createLocalLogWithActiveSegment(File dir, LogConfig config) throws IOException {
        LogSegments segments = new LogSegments(topicPartition);
        segments.add(LogSegment.open(dir,
                0L,
                config,
                MOCK_TIME,
                config.initFileSize(),
                config.preallocate));
        return new LocalLog(dir,
                config,
                segments,
                0L,
                new LogOffsetMetadata(0L, 0L, 0),
                MOCK_TIME.scheduler,
                MOCK_TIME,
                topicPartition,
                logDirFailureChannel);
    }
}
