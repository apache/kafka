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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolAssignmentJsonConverter;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.ConsumerProtocolSubscriptionJsonConverter;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.KRaftVersionRecordJsonConverter;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessageJsonConverter;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotFooterRecordJsonConverter;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecordJsonConverter;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.message.VotersRecordJsonConverter;
import org.apache.kafka.common.metadata.MetadataJsonConverters;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.internal.AbstractLegacyRecordBatch;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.FileLogInputStream;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde;
import org.apache.kafka.coordinator.share.ShareCoordinatorRecordSerde;
import org.apache.kafka.coordinator.transaction.TransactionCoordinatorRecordSerde;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.snapshot.SnapshotPath;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.CorruptSnapshotException;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.tools.api.Decoder;
import org.apache.kafka.tools.api.StringDecoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

public class DumpLogSegments {
    // Visible for testing
    static final String RECORD_INDENT = "|";

    public static void main(String[] args) throws IOException {
        DumpLogSegmentsOptions opts = new DumpLogSegmentsOptions(args);
        CommandLineUtils.maybePrintHelpOrVersion(
            opts,
            "This tool helps to parse a log file and dump its contents to the console, useful for debugging a seemingly corrupt log segment."
        );
        opts.checkArgs();

        Map<String, Map<Long, Long>> misMatchesForIndexFilesMap = new HashMap<>();
        TimeIndexDumpErrors timeIndexDumpErrors = new TimeIndexDumpErrors();
        Map<String, Map<Long, Long>> nonConsecutivePairsForLogFilesMap = new HashMap<>();

        for (String arg : opts.files()) {
            File file = new File(arg);
            System.out.println("Dumping " + file);

            String filename = file.getName();
            String suffix = filename.substring(filename.lastIndexOf("."));

            switch (suffix) {
                case UnifiedLog.LOG_FILE_SUFFIX, Snapshots.SUFFIX ->
                    dumpLog(file, opts.shouldPrintDataLog(), nonConsecutivePairsForLogFilesMap,
                        opts.isDeepIteration(), opts.messageParser(), opts.skipRecordMetadata(), opts.maxBytes());
                case UnifiedLog.INDEX_FILE_SUFFIX -> dumpIndex(file, opts.indexSanityOnly(), opts.verifyOnly(),
                    misMatchesForIndexFilesMap, opts.maxMessageSize());
                case UnifiedLog.TIME_INDEX_FILE_SUFFIX ->
                    dumpTimeIndex(file, opts.indexSanityOnly(), opts.verifyOnly(), timeIndexDumpErrors);
                case LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX -> dumpProducerIdSnapshot(file);
                case UnifiedLog.TXN_INDEX_FILE_SUFFIX -> dumpTxnIndex(file);
                default -> System.err.println("Ignoring unknown file " + file);
            }
        }

        misMatchesForIndexFilesMap.forEach((fileName, mismatchesByOffset) -> {
            System.err.println("Mismatches in :" + fileName);
            mismatchesByOffset.forEach((indexOffset, logOffset) ->
                System.err.println("  Index offset: " + indexOffset + ", log offset: " + logOffset));
        });

        timeIndexDumpErrors.printErrors();

        nonConsecutivePairsForLogFilesMap.forEach((fileName, listOfNonConsecutivePairs) -> {
            System.err.println("Non-consecutive offsets in " + fileName);
            listOfNonConsecutivePairs.forEach((key, value) -> System.err.println("  " + key + " is followed by " + value));
        });
    }

    private static void dumpTxnIndex(File file) throws IOException {
        try (TransactionIndex index = new TransactionIndex(UnifiedLog.offsetFromFile(file), file)) {
            for (AbortedTxn abortedTxn : index.allAbortedTxns()) {
                System.out.println("version: " + abortedTxn.version() +
                    " producerId: " + abortedTxn.producerId() +
                    " firstOffset: " + abortedTxn.firstOffset() +
                    " lastOffset: " + abortedTxn.lastOffset() +
                    " lastStableOffset: " + abortedTxn.lastStableOffset());
            }
        }
    }

    private static void dumpProducerIdSnapshot(File file) throws IOException {
        try {
            List<ProducerStateEntry> entries = ProducerStateManager.readSnapshot(file);
            for (ProducerStateEntry entry : entries) {
                System.out.print("producerId: " + entry.producerId() +
                    " producerEpoch: " + entry.producerEpoch() +
                    " coordinatorEpoch: " + entry.coordinatorEpoch() +
                    " currentTxnFirstOffset: " + entry.currentTxnFirstOffset() +
                    " lastTimestamp: " + entry.lastTimestamp());

                if (!entry.batchMetadata().isEmpty()) {
                    BatchMetadata metadata = entry.batchMetadata().iterator().next();
                    System.out.print(" firstSequence: " + metadata.firstSeq() +
                        " lastSequence: " + metadata.lastSeq() +
                        " lastOffset: " + metadata.lastOffset() +
                        " offsetDelta: " + metadata.offsetDelta() +
                        " timestamp: " + metadata.timestamp());
                }
                System.out.println();
            }
        } catch (CorruptSnapshotException e) {
            System.err.println(e.getMessage());
        }
    }

    // Visible for testing
    static void dumpIndex(File file,
                          boolean indexSanityOnly,
                          boolean verifyOnly,
                          Map<String, Map<Long, Long>> misMatchesForIndexFilesMap,
                          int maxMessageSize) throws IOException {
        long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
        File logFile = new File(file.getAbsoluteFile().getParent(),
            file.getName().split("\\.")[0] + UnifiedLog.LOG_FILE_SUFFIX);

        try (FileRecords fileRecords = FileRecords.open(logFile, false);
             OffsetIndex index = new OffsetIndex(file, startOffset, -1, false)) {

            if (index.entries() == 0) {
                System.out.println(file + " is empty.");
                return;
            }

            // Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
            if (indexSanityOnly) {
                index.sanityCheck();
                System.out.println(file + " passed sanity check.");
                return;
            }

            for (int i = 0; i < index.entries(); i++) {
                OffsetPosition entry = index.entry(i);

                // since it is a sparse file, in the event of a crash there may be many zero entries,  stop if we see one
                if (entry.offset() == index.baseOffset() && i > 0) {
                    return;
                }

                FileRecords slice = fileRecords.slice(entry.position(), maxMessageSize);
                long firstBatchLastOffset = slice.batches().iterator().next().lastOffset();
                if (firstBatchLastOffset != entry.offset()) {
                    Map<Long, Long> mismatchesByOffset = misMatchesForIndexFilesMap
                        .computeIfAbsent(file.getAbsolutePath(), k -> new TreeMap<>(java.util.Collections.reverseOrder()));
                    mismatchesByOffset.put(entry.offset(), firstBatchLastOffset);
                }
                if (!verifyOnly) {
                    System.out.println("offset: " + entry.offset() + " position: " + entry.position());
                }
            }
        }
    }

    // Visible for testing
    static void dumpTimeIndex(File file,
                              boolean indexSanityOnly,
                              boolean verifyOnly,
                              TimeIndexDumpErrors timeIndexDumpErrors) throws IOException {
        long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
        File logFile = new File(file.getAbsoluteFile().getParent(),
            file.getName().split("\\.")[0] + UnifiedLog.LOG_FILE_SUFFIX);
        File indexFile = new File(file.getAbsoluteFile().getParent(),
            file.getName().split("\\.")[0] + UnifiedLog.INDEX_FILE_SUFFIX);

        FileRecords fileRecords = null;
        OffsetIndex index = null;
        TimeIndex timeIndex = null;
        try {
            fileRecords = FileRecords.open(logFile, false);
            index = new OffsetIndex(indexFile, startOffset, -1, false);
            timeIndex = new TimeIndex(file, startOffset, -1, false);

            // Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
            if (indexSanityOnly) {
                timeIndex.sanityCheck();
                System.out.println(file + " passed sanity check.");
                return;
            }

            long prevTimestamp = RecordBatch.NO_TIMESTAMP;
            for (int i = 0; i < timeIndex.entries(); i++) {
                TimestampOffset entry = timeIndex.entry(i);

                // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
                if (entry.offset() == timeIndex.baseOffset() && i > 0) {
                    return;
                }

                OffsetPosition offsetPosition = index.lookup(entry.offset());
                FileRecords partialFileRecords = fileRecords.slice(offsetPosition.position(), Integer.MAX_VALUE);
                List<FileLogInputStream.FileChannelRecordBatch> batches = new ArrayList<>();
                partialFileRecords.batches().forEach(batches::add);

                long maxTimestamp = RecordBatch.NO_TIMESTAMP;
                // We first find the message by offset then check if the timestamp is correct.
                Optional<FileLogInputStream.FileChannelRecordBatch> matchingBatch = batches.stream()
                    .filter(batch -> batch.lastOffset() >= entry.offset())
                    .findFirst();

                if (matchingBatch.isEmpty()) {
                    timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset(), -1L);
                } else if (matchingBatch.get().lastOffset() != entry.offset()) {
                    timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset(),
                        matchingBatch.get().lastOffset());
                } else {
                    RecordBatch batch = matchingBatch.get();
                    for (Record record : batch) {
                        maxTimestamp = Math.max(maxTimestamp, record.timestamp());
                    }

                    if (maxTimestamp != entry.timestamp()) {
                        timeIndexDumpErrors.recordMismatchTimeIndex(file, entry.timestamp(), maxTimestamp);
                    }

                    if (prevTimestamp >= entry.timestamp()) {
                        timeIndexDumpErrors.recordOutOfOrderIndexTimestamp(file, entry.timestamp(), prevTimestamp);
                    }
                }

                if (!verifyOnly) {
                    System.out.println("timestamp: " + entry.timestamp() + " offset: " + entry.offset());
                }
                prevTimestamp = entry.timestamp();
            }
        } finally {
            if (fileRecords != null) {
                fileRecords.closeHandlers();
            }
            if (index != null) {
                index.closeHandler();
            }
            if (timeIndex != null) {
                timeIndex.closeHandler();
            }
        }
    }

    interface MessageParser<K, V> {
        ParseResult<K, V> parse(Record record);
    }

    record ParseResult<K, V>(Optional<K> key, Optional<V> value) { }

    static class DecoderMessageParser<K, V> implements MessageParser<K, V> {
        private final Decoder<K> keyDecoder;
        private final Decoder<V> valueDecoder;

        public DecoderMessageParser(Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
        }

        @Override
        public ParseResult<K, V> parse(Record record) {
            Optional<K> key = record.hasKey()
                ? Optional.of(keyDecoder.fromBytes(Utils.readBytes(record.key())))
                : Optional.empty();

            Optional<V> value;
            if (!record.hasValue()) {
                value = Optional.empty();
            } else {
                value = Optional.of(valueDecoder.fromBytes(Utils.readBytes(record.value())));
            }

            return new ParseResult<>(key, value);
        }
    }

    /* print out the contents of the log */
    private static void dumpLog(File file,
                                boolean printContents,
                                Map<String, Map<Long, Long>> nonConsecutivePairsForLogFilesMap,
                                boolean isDeepIteration,
                                MessageParser<?, ?> parser,
                                boolean skipRecordMetadata,
                                int maxBytes) throws IOException {
        if (file.getName().endsWith(UnifiedLog.LOG_FILE_SUFFIX)) {
            long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
            System.out.println("Log starting offset: " + startOffset);
        } else if (file.getName().endsWith(Snapshots.SUFFIX)) {
            if (file.getName().equals(BootstrapDirectory.BINARY_BOOTSTRAP_FILENAME)) {
                System.out.println("KRaft bootstrap snapshot");
            } else {
                Optional<SnapshotPath> pathOpt = Snapshots.parse(file.toPath());
                System.out.println("Snapshot end offset: " + pathOpt.get().snapshotId().offset() +
                        ", epoch: " + pathOpt.get().snapshotId().epoch());
            }
        }

        FileRecords fileRecords = null;
        try {
            fileRecords = FileRecords.open(file, false).slice(0, maxBytes);
            long validBytes = 0L;
            AtomicLong lastOffset = new AtomicLong(-1L);

            for (FileLogInputStream.FileChannelRecordBatch batch : fileRecords.batches()) {
                printBatchLevel(batch, validBytes);
                if (isDeepIteration) {
                    dumpBatchRecords(batch, lastOffset, file, nonConsecutivePairsForLogFilesMap,
                        skipRecordMetadata, printContents, parser);
                }
                validBytes += batch.sizeInBytes();
            }

            printTrailingBytes(fileRecords, validBytes, maxBytes, file);
        } finally {
            if (fileRecords != null) {
                fileRecords.closeHandlers();
            }
        }
    }

    private static void dumpBatchRecords(FileLogInputStream.FileChannelRecordBatch batch,
                                         AtomicLong lastOffset,
                                         File file,
                                         Map<String, Map<Long, Long>> nonConsecutivePairsForLogFilesMap,
                                         boolean skipRecordMetadata,
                                         boolean printContents,
                                         MessageParser<?, ?> parser) {
        for (Record record : batch) {
            long previousOffset = lastOffset.get();
            if (record.offset() != previousOffset + 1) {
                Map<Long, Long> nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap
                    .computeIfAbsent(file.getAbsolutePath(), k -> new TreeMap<>());
                nonConsecutivePairsSeq.put(previousOffset, record.offset());
            }
            lastOffset.set(record.offset());

            if (!skipRecordMetadata) {
                printRecordMetadata(batch, record);
            }

            if (printContents && !batch.isControlBatch()) {
                String prefix = skipRecordMetadata ? RECORD_INDENT + " " : " ";
                printRecordContents(parser, record, prefix);
            }
            System.out.println();
        }
    }

    private static void printRecordMetadata(FileLogInputStream.FileChannelRecordBatch batch,
                                            Record record) {
        System.out.print(RECORD_INDENT + " " + "offset: " + record.offset() +
            " " + batch.timestampType() + ": " + record.timestamp() +
            " keySize: " + record.keySize() + " valueSize: " + record.valueSize());

        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            System.out.print(" sequence: " + record.sequence() +
                " headerKeys: " + Arrays.stream(record.headers())
                    .map(Header::key)
                    .collect(Collectors.joining(",", "[", "]")));
        }

        if (record instanceof AbstractLegacyRecordBatch r) {
            System.out.print(" isValid: " + r.isValid() + " crc: " + r.checksum());
        }

        if (batch.isControlBatch()) {
            printControlRecord(record);
        }
    }

    private static void printRecordContents(MessageParser<?, ?> parser, Record record, String prefix) {
        ParseResult<?, ?> result = parser.parse(record);
        if (result.key().isPresent()) {
            System.out.print(prefix + "key: " + result.key().get());
            prefix = " ";
        }
        if (result.value().isPresent()) {
            System.out.print(prefix + "payload: " + result.value().get());
        }
    }

    private static void printTrailingBytes(FileRecords fileRecords, long validBytes, int maxBytes, File file) {
        long trailingBytes = fileRecords.sizeInBytes() - validBytes;
        if (trailingBytes > 0 && maxBytes == Integer.MAX_VALUE) {
            System.out.println("Found " + trailingBytes + " invalid bytes at the end of " + file.getName());
        }
    }

    private static void printControlRecord(Record record) {
        short controlTypeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType controlType = ControlRecordType.fromTypeId(controlTypeId);

        switch (controlType) {
            case ABORT:
            case COMMIT:
                EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                System.out.print(" endTxnMarker: " + endTxnMarker.controlType() +
                    " coordinatorEpoch: " + endTxnMarker.coordinatorEpoch());
                break;
            case LEADER_CHANGE:
                LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(record);
                System.out.print(" LeaderChange: " +
                    LeaderChangeMessageJsonConverter.write(leaderChangeMessage, leaderChangeMessage.version()));
                break;
            case SNAPSHOT_HEADER:
                SnapshotHeaderRecord header = ControlRecordUtils.deserializeSnapshotHeaderRecord(record);
                System.out.print(" SnapshotHeader " +
                    SnapshotHeaderRecordJsonConverter.write(header, header.version()));
                break;
            case SNAPSHOT_FOOTER:
                SnapshotFooterRecord footer = ControlRecordUtils.deserializeSnapshotFooterRecord(record);
                System.out.print(" SnapshotFooter " +
                    SnapshotFooterRecordJsonConverter.write(footer, footer.version()));
                break;
            case KRAFT_VERSION:
                KRaftVersionRecord kraftVersion = ControlRecordUtils.deserializeKRaftVersionRecord(record);
                System.out.print(" KRaftVersion " +
                    KRaftVersionRecordJsonConverter.write(kraftVersion, kraftVersion.version()));
                break;
            case KRAFT_VOTERS:
                VotersRecord voters = ControlRecordUtils.deserializeVotersRecord(record);
                System.out.print(" KRaftVoters " +
                    VotersRecordJsonConverter.write(voters, voters.version()));
                break;
            default:
                System.out.print(" controlType: " + controlType + "(" + controlTypeId + ")");
                break;
        }
    }

    private static void printBatchLevel(FileLogInputStream.FileChannelRecordBatch batch, long accumulativeBytes) {
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            System.out.print("baseOffset: " + batch.baseOffset() +
                " lastOffset: " + batch.lastOffset() +
                " count: " + batch.countOrNull() +
                " baseSequence: " + batch.baseSequence() +
                " lastSequence: " + batch.lastSequence() +
                " producerId: " + batch.producerId() +
                " producerEpoch: " + batch.producerEpoch() +
                " partitionLeaderEpoch: " + batch.partitionLeaderEpoch() +
                " isTransactional: " + batch.isTransactional() +
                " isControl: " + batch.isControlBatch() +
                " deleteHorizonMs: " + batch.deleteHorizonMs());
        } else {
            System.out.print("offset: " + batch.lastOffset());
        }

        System.out.println(" position: " + accumulativeBytes +
            " " + batch.timestampType() + ": " + batch.maxTimestamp() +
            " size: " + batch.sizeInBytes() +
            " magic: " + batch.magic() +
            " compresscodec: " + batch.compressionType().name +
            " crc: " + batch.checksum() +
            " isvalid: " + batch.isValid());
    }

    static class TimeIndexDumpErrors {
        final Map<String, List<Pair<Long, Long>>> misMatchesForTimeIndexFilesMap = new HashMap<>();
        final Map<String, List<Pair<Long, Long>>> outOfOrderTimestamp = new HashMap<>();
        final Map<String, List<Pair<Long, Long>>> shallowOffsetNotFound = new HashMap<>();

        void recordMismatchTimeIndex(File file, long indexTimestamp, long logTimestamp) {
            List<Pair<Long, Long>> misMatchesSeq = misMatchesForTimeIndexFilesMap
                .computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            misMatchesSeq.add(new Pair<>(indexTimestamp, logTimestamp));
        }

        void recordOutOfOrderIndexTimestamp(File file, long indexTimestamp, long prevIndexTimestamp) {
            List<Pair<Long, Long>> outOfOrderSeq = outOfOrderTimestamp
                .computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            outOfOrderSeq.add(new Pair<>(indexTimestamp, prevIndexTimestamp));
        }

        void recordShallowOffsetNotFound(File file, long indexOffset, long logOffset) {
            List<Pair<Long, Long>> shallowOffsetNotFoundSeq = shallowOffsetNotFound
                .computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            shallowOffsetNotFoundSeq.add(new Pair<>(indexOffset, logOffset));
        }

        void printErrors() {
            misMatchesForTimeIndexFilesMap.forEach((fileName, listOfMismatches) -> {
                System.err.println("Found timestamp mismatch in :" + fileName);
                listOfMismatches.forEach(m ->
                    System.err.println("  Index timestamp: " + m.first + ", log timestamp: " + m.second)
                );
            });

            outOfOrderTimestamp.forEach((fileName, outOfOrderTimestamps) -> {
                System.err.println("Found out of order timestamp in :" + fileName);
                outOfOrderTimestamps.forEach(m ->
                    System.err.println("  Index timestamp: " + m.first + ", Previously indexed timestamp: " + m.second)
                );
            });

            shallowOffsetNotFound.values().forEach(listOfShallowOffsetNotFound -> {
                System.err.println("The following indexed offsets are not found in the log.");
                listOfShallowOffsetNotFound.forEach(pair ->
                    System.err.println("Indexed offset: " + pair.first + ", found log offset: " + pair.second)
                );
            });
        }
    }

    abstract static class CoordinatorRecordMessageParser implements MessageParser<String, String> {
        private final CoordinatorRecordSerde serde;

        protected CoordinatorRecordMessageParser(CoordinatorRecordSerde serde) {
            this.serde = serde;
        }

        @Override
        public ParseResult<String, String> parse(Record record) {
            if (!record.hasKey()) {
                throw new RuntimeException("Failed to decode message at offset " + record.offset() +
                    " using the specified decoder (message had a missing key)");
            }

            try {
                CoordinatorRecord r = serde.deserialize(record.key(), record.value());
                return new ParseResult<>(
                    Optional.of(prepareKey(r.key())),
                    Optional.ofNullable(r.value())
                        .map(v -> prepareValue(v.message(), v.version()))
                        .or(() -> Optional.of("<DELETE>"))
                );
            } catch (Deserializer.UnknownRecordTypeException e) {
                return new ParseResult<>(
                    Optional.of("Unknown record type " + e.unknownType() + " at offset " +
                        record.offset() + ", skipping."),
                    Optional.empty()
                );
            } catch (Throwable e) {
                return new ParseResult<>(
                    Optional.of("Error at offset " + record.offset() + ", skipping. " + e.getMessage()),
                    Optional.empty()
                );
            }
        }

        private String prepareKey(ApiMessage message) {
            ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
            json.set("type", new TextNode(String.valueOf(message.apiKey())));
            json.set("data", keyAsJson(message));
            return json.toString();
        }

        private String prepareValue(ApiMessage message, short version) {
            ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
            json.set("version", new TextNode(String.valueOf(version)));
            json.set("data", valueAsJson(message, version));
            return json.toString();
        }

        protected abstract JsonNode keyAsJson(ApiMessage message);
        protected abstract JsonNode valueAsJson(ApiMessage message, short version);
    }

    // Package private for testing
    static class OffsetsMessageParser extends CoordinatorRecordMessageParser {
        public OffsetsMessageParser() {
            super(new GroupCoordinatorRecordSerde());
        }

        @Override
        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.group.generated.CoordinatorRecordJsonConverters
                .writeRecordKeyAsJson(message);
        }

        @Override
        protected JsonNode valueAsJson(ApiMessage message, short version) {
            if (message.apiKey() == org.apache.kafka.coordinator.group.generated.CoordinatorRecordType.GROUP_METADATA.id()) {
                return prepareGroupMetadataValue(
                    (org.apache.kafka.coordinator.group.generated.GroupMetadataValue) message, version);
            } else {
                return org.apache.kafka.coordinator.group.generated.CoordinatorRecordJsonConverters
                    .writeRecordValueAsJson(message, version);
            }
        }

        private JsonNode prepareGroupMetadataValue(
            org.apache.kafka.coordinator.group.generated.GroupMetadataValue message, short version) {
            JsonNode json = org.apache.kafka.coordinator.group.generated.GroupMetadataValueJsonConverter
                .write(message, version);

            JsonNode protocolTypeNode = json.get("protocolType");
            if (protocolTypeNode != null && protocolTypeNode.asText().equals(ConsumerProtocol.PROTOCOL_TYPE)) {
                JsonNode membersNode = json.get("members");
                if (membersNode != null && membersNode.isArray()) {
                    membersNode.forEach(memberNode -> {
                        // Replace the subscription field
                        replaceField(
                            memberNode,
                            "subscription",
                            ConsumerProtocolSubscription::new,
                            ConsumerProtocolSubscriptionJsonConverter::write
                        );

                        // Replace the assignment field
                        replaceField(
                            memberNode,
                            "assignment",
                            ConsumerProtocolAssignment::new,
                            ConsumerProtocolAssignmentJsonConverter::write
                        );
                    });
                }
            }

            return json;
        }

        private <T> void replaceField(
            JsonNode node,
            String field,
            BiFunction<Readable, Short, T> reader,
            BiFunction<T, Short, JsonNode> writer
        ) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null) {
                try {
                    byte[] bytes = fieldNode.binaryValue();
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
                    short version = accessor.readShort();
                    T data = reader.apply(accessor, version);
                    ((ObjectNode) node).replace(field, writer.apply(data, version));
                } catch (RuntimeException | IOException e) {
                    // Swallow and keep the original bytes
                }
            }
        }
    }

    // Package private for testing
    static class TransactionLogMessageParser extends CoordinatorRecordMessageParser {
        public TransactionLogMessageParser() {
            super(new TransactionCoordinatorRecordSerde());
        }

        @Override
        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordJsonConverters
                .writeRecordKeyAsJson(message);
        }

        @Override
        protected JsonNode valueAsJson(ApiMessage message, short version) {
            return org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordJsonConverters
                .writeRecordValueAsJson(message, version);
        }
    }

    private static class ClusterMetadataLogMessageParser implements MessageParser<String, String> {
        private final MetadataRecordSerde metadataRecordSerde = new MetadataRecordSerde();

        @Override
        public ParseResult<String, String> parse(Record record) {
            String output;
            try {
                ApiMessageAndVersion messageAndVersion = metadataRecordSerde.read(
                    new ByteBufferAccessor(record.value()), record.valueSize());
                ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
                json.set("type", new TextNode(
                    MetadataRecordType.fromId(messageAndVersion.message().apiKey()).toString()));
                json.set("version", new IntNode(messageAndVersion.version()));
                json.set("data", MetadataJsonConverters.writeJson(
                    messageAndVersion.message(), messageAndVersion.version()));
                output = json.toString();
            } catch (Throwable e) {
                output = "Error at " + record.offset() + ", skipping. " + e.getMessage();
            }
            // No keys for metadata records
            return new ParseResult<>(Optional.empty(), Optional.of(output));
        }
    }

    private static class RemoteMetadataLogMessageParser implements MessageParser<String, String> {
        private final RemoteLogMetadataSerde metadataRecordSerde = new RemoteLogMetadataSerde();

        @Override
        public ParseResult<String, String> parse(Record record) {
            String output;
            try {
                byte[] data = new byte[record.value().remaining()];
                record.value().get(data);
                output = metadataRecordSerde.deserialize(data).toString();
            } catch (Throwable e) {
                output = "Error at offset " + record.offset() + ", skipping. " + e.getMessage();
            }
            // No keys for metadata records
            return new ParseResult<>(Optional.empty(), Optional.of(output));
        }
    }

    // Package private for testing
    static class ShareGroupStateMessageParser extends CoordinatorRecordMessageParser {
        public ShareGroupStateMessageParser() {
            super(new ShareCoordinatorRecordSerde());
        }

        @Override
        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.share.generated.CoordinatorRecordJsonConverters
                .writeRecordKeyAsJson(message);
        }

        @Override
        protected JsonNode valueAsJson(ApiMessage message, short version) {
            return org.apache.kafka.coordinator.share.generated.CoordinatorRecordJsonConverters
                .writeRecordValueAsJson(message, version);
        }
    }

    record Pair<F, S>(F first, S second) { }

    private static class DumpLogSegmentsOptions extends CommandDefaultOptions {
        private final OptionSpec<Void> printOpt;
        private final OptionSpec<Void> verifyOpt;
        private final OptionSpec<Void> indexSanityOpt;
        private final OptionSpec<String> filesOpt;
        private final OptionSpec<Integer> maxMessageSizeOpt;
        private final OptionSpec<Integer> maxBytesOpt;
        private final OptionSpec<Void> deepIterationOpt;
        private final OptionSpec<String> valueDecoderOpt;
        private final OptionSpec<String> keyDecoderOpt;
        private final OptionSpec<Void> offsetsOpt;
        private final OptionSpec<Void> transactionLogOpt;
        private final OptionSpec<Void> clusterMetadataOpt;
        private final OptionSpec<Void> remoteMetadataOpt;
        private final OptionSpec<Void> shareStateOpt;
        private final OptionSpec<Void> skipRecordMetadataOpt;

        DumpLogSegmentsOptions(String[] args) {
            super(args);

            printOpt = parser.accepts("print-data-log",
                "If set, printing the messages content when dumping data logs. Automatically set if any decoder option is specified.");
            verifyOpt = parser.accepts("verify-index-only",
                "If set, just verify the index log without printing its content.");
            indexSanityOpt = parser.accepts("index-sanity-check",
                "If set, just checks the index sanity without printing its content. " +
                "This is the same check that is executed on broker startup to determine if an index needs rebuilding or not.");
            filesOpt = parser.accepts("files",
                    "REQUIRED: The comma separated list of data and index log files to be dumped.")
                .withRequiredArg()
                .describedAs("file1, file2, ...")
                .ofType(String.class);
            maxMessageSizeOpt = parser.accepts("max-message-size", "Size of largest message.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(5 * 1024 * 1024);
            maxBytesOpt = parser.accepts("max-bytes",
                    "Limit the amount of total batches read in bytes avoiding reading the whole .log file(s).")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(Integer.MAX_VALUE);
            deepIterationOpt = parser.accepts("deep-iteration",
                "If set, uses deep instead of shallow iteration. Automatically set if print-data-log is enabled.");
            valueDecoderOpt = parser.accepts("value-decoder-class",
                    "If set, used to deserialize the messages. This class should implement org.apache.kafka.tools.api.Decoder trait. Custom jar should be available in kafka/libs directory.")
                .withOptionalArg()
                .ofType(String.class)
                .defaultsTo(StringDecoder.class.getName());
            keyDecoderOpt = parser.accepts("key-decoder-class",
                    "If set, used to deserialize the keys. This class should implement org.apache.kafka.tools.api.Decoder trait. Custom jar should be available in kafka/libs directory.")
                .withOptionalArg()
                .ofType(String.class)
                .defaultsTo(StringDecoder.class.getName());
            offsetsOpt = parser.accepts("offsets-decoder",
                "If set, log data will be parsed as offset data from the __consumer_offsets topic.");
            transactionLogOpt = parser.accepts("transaction-log-decoder",
                "If set, log data will be parsed as transaction metadata from the __transaction_state topic.");
            clusterMetadataOpt = parser.accepts("cluster-metadata-decoder",
                "If set, log data will be parsed as cluster metadata records.");
            remoteMetadataOpt = parser.accepts("remote-log-metadata-decoder",
                "If set, log data will be parsed as TopicBasedRemoteLogMetadataManager (RLMM) metadata records. " +
                "Instead, the value-decoder-class option can be used if a custom RLMM implementation is configured.");
            shareStateOpt = parser.accepts("share-group-state-decoder",
                "If set, log data will be parsed as share group state data from the __share_group_state topic.");
            skipRecordMetadataOpt = parser.accepts("skip-record-metadata",
                "Skip metadata when printing records. This flag also skips control records.");

            this.options = parser.parse(args);
        }

        MessageParser<?, ?> messageParser() {
            if (options.has(offsetsOpt)) {
                return new OffsetsMessageParser();
            } else if (options.has(transactionLogOpt)) {
                return new TransactionLogMessageParser();
            } else if (options.has(clusterMetadataOpt)) {
                return new ClusterMetadataLogMessageParser();
            } else if (options.has(remoteMetadataOpt)) {
                return new RemoteMetadataLogMessageParser();
            } else if (options.has(shareStateOpt)) {
                return new ShareGroupStateMessageParser();
            } else {
                try {
                    Decoder<?> valueDecoder = Utils.newInstance(options.valueOf(valueDecoderOpt), Decoder.class);
                    Decoder<?> keyDecoder = Utils.newInstance(options.valueOf(keyDecoderOpt), Decoder.class);
                    return new DecoderMessageParser<>(keyDecoder, valueDecoder);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Failed to load decoder class", e);
                }
            }
        }

        boolean shouldPrintDataLog() {
            return hasAnyOption(
                printOpt,
                offsetsOpt,
                transactionLogOpt,
                clusterMetadataOpt,
                remoteMetadataOpt,
                valueDecoderOpt,
                keyDecoderOpt,
                shareStateOpt
            );
        }

        private boolean hasAnyOption(OptionSpec<?>... specs) {
            for (OptionSpec<?> spec : specs) {
                if (options.has(spec)) {
                    return true;
                }
            }
            return false;
        }

        boolean skipRecordMetadata() {
            return options.has(skipRecordMetadataOpt);
        }

        boolean isDeepIteration() {
            return options.has(deepIterationOpt) || shouldPrintDataLog();
        }

        boolean verifyOnly() {
            return options.has(verifyOpt);
        }

        boolean indexSanityOnly() {
            return options.has(indexSanityOpt);
        }

        String[] files() {
            return options.valueOf(filesOpt).split(",");
        }

        int maxMessageSize() {
            return options.valueOf(maxMessageSizeOpt);
        }

        int maxBytes() {
            return options.valueOf(maxBytesOpt);
        }

        void checkArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, filesOpt);
        }
    }
}
