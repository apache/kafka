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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.LogReader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Shared helper for DLQ record building and source-record fetching.
 * Used by both the K1 ({@link ShareGroupDLQStateManager}) and K2 DLQ manager implementations.
 */
public class ShareGroupDLQRecordHelper {

    /**
     * In most cases we expect the records getting DLQ'ed will be single offsets and
     * not complete batches. Hence, using a large upper limit while reading from the log
     * would be fruitless in most cases. Therefore, the value of 1 MB has been chosen
     * for the DLQ-related log reads.
     */
    public static final int DLQ_MAX_FETCH_BYTES = 1024 * 1024;

    public static final String HEADER_DLQ_ERRORS_TOPIC = "__dlq.errors.topic";
    public static final String HEADER_DLQ_ERRORS_PARTITION = "__dlq.errors.partition";
    public static final String HEADER_DLQ_ERRORS_OFFSET = "__dlq.errors.offset";
    public static final String HEADER_DLQ_ERRORS_GROUP = "__dlq.errors.group";
    public static final String HEADER_DLQ_ERRORS_DELIVERY_COUNT = "__dlq.errors.delivery.count";
    public static final String HEADER_DLQ_ERRORS_MESSAGE = "__dlq.errors.message";

    /**
     * Result of building DLQ records for a range of offsets, respecting maxMessageBytes.
     *
     * @param records         The built MemoryRecords containing DLQ records with headers
     * @param lastOffsetIncluded The last source offset included in this batch
     * @param recordCount     The number of individual records in the batch
     */
    public record BuildResult(MemoryRecords records, long lastOffsetIncluded, int recordCount) {
    }

    /**
     * Builds DLQ headers for a single offset.
     *
     * @param sourceTopic   The resolved source topic name
     * @param partition     The source partition number
     * @param offset        The source offset
     * @param groupId       The share group ID
     * @param deliveryCount Optional delivery count
     * @param cause         Optional cause/reason for DLQ
     * @return Array of DLQ headers
     */
    private static Header[] headers(
            String sourceTopic,
            int partition,
            long offset,
            String groupId,
            Optional<Short> deliveryCount,
            Optional<Throwable> cause
    ) {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(HEADER_DLQ_ERRORS_TOPIC, sourceTopic.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_DLQ_ERRORS_PARTITION, Integer.toString(partition).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_DLQ_ERRORS_OFFSET, Long.toString(offset).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(HEADER_DLQ_ERRORS_GROUP, groupId.getBytes(StandardCharsets.UTF_8)));
        deliveryCount.ifPresent(dc -> headers.add(
                new RecordHeader(HEADER_DLQ_ERRORS_DELIVERY_COUNT, Short.toString(dc).getBytes(StandardCharsets.UTF_8))));
        cause.ifPresent(c -> {
            if (c.getMessage() != null) {
                headers.add(new RecordHeader(HEADER_DLQ_ERRORS_MESSAGE, c.getMessage().getBytes(StandardCharsets.UTF_8)));
            }
        });
        return headers.toArray(new Header[0]);
    }

    /**
     * Resolves the source topic name from a TopicIdPartition, falling back to the topic ID string.
     *
     * @param topicIdPartition The source topic-partition
     * @param topicNameResolver Resolver that maps topic ID to name
     * @return The resolved topic name
     */
    public static String resolveSourceTopicName(TopicIdPartition topicIdPartition, java.util.function.Function<Uuid, Optional<String>> topicNameResolver) {
        String recordTopicName = topicIdPartition.topic();
        if (recordTopicName == null || recordTopicName.isEmpty()) {
            // If topic name lookup fails, use topic id as a String in the header.
            recordTopicName = topicNameResolver.apply(topicIdPartition.topicId()).orElse(topicIdPartition.topicId().toString());
        }
        return recordTopicName;
    }

    /**
     * Computes the destination DLQ partition from the source partition.
     *
     * @param sourcePartition   The source partition number
     * @param numDlqPartitions  The number of partitions in the DLQ topic
     * @return The destination DLQ partition number
     */
    public static int dlqDestinationPartition(int sourcePartition, int numDlqPartitions) {
        return sourcePartition % numDlqPartitions;
    }

    /**
     * Builds DLQ MemoryRecords for a range of offsets, respecting maxMessageBytes for batch splitting.
     *
     * @param param              The DLQ record parameter with offset range and metadata
     * @param resolvedRecordData Map of source offset to source Record (can be incomplete)
     * @param nextOffsetToSend   The first offset to include in this batch
     * @param lastResolvedOffset The last offset that was resolved by the fetcher
     * @param maxMessageBytes    Maximum batch size in bytes
     * @param time               Time instance for wall-clock timestamps
     * @param sourceTopic        The resolved source topic name
     * @return BuildResult containing the MemoryRecords and the last offset included
     */
    public static BuildResult buildDLQRecords(
            ShareGroupDLQRecordParameter param,
            Map<Long, Record> resolvedRecordData,
            long nextOffsetToSend,
            long lastResolvedOffset,
            int maxMessageBytes,
            Time time,
            String sourceTopic
    ) {
        // In most cases the offset range is a single offset (see DLQ_MAX_FETCH_BYTES). Track the
        // running batch size incrementally via DefaultRecord.sizeInBytes() - the same formula
        // MemoryRecords itself uses - instead of re-serializing the whole batch-so-far on every
        // offset, which would make this loop quadratic in the number of offsets.
        List<SimpleRecord> simpleRecords = new ArrayList<>();
        int batchSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        Long baseTimestamp = null;
        // Capped at lastResolvedOffsetThisRound, not just param.lastOffset(): offsets beyond it were
        // never attempted by this round's fetch and must stay untouched for a fresh round to retry,
        // rather than being packed in here as headers-only just because they have no map entry yet.
        // Floored at nextOffsetToSend itself (mirroring the single-record floor below for the
        // size-exceeds-limit case): a fetch that resolved nothing at all for this round - e.g. a read
        // that failed outright - would otherwise leave this loop with zero iterations, producing an
        // empty record batch, which the broker rejects outright. Sending nextOffsetToSend alone,
        // headers-only, guarantees forward progress even when nothing could be resolved.
        long roundEnd = Math.max(nextOffsetToSend, Math.min(param.lastOffset(), lastResolvedOffset));

        for (long offset = nextOffsetToSend; offset <= roundEnd; offset++) {
            // Must be wall-clock (epoch) time: log retention decides whether to delete this
            // record's segment by comparing its timestamp against the current wall-clock time.
            long timestamp = time.milliseconds();
            ByteBuffer key = null;
            ByteBuffer value = null;
            Record record = resolvedRecordData.get(offset);
            if (record != null) {
                key = record.hasKey() ? record.key() : null;
                value = record.hasValue() ? record.value() : null;
            }
            Header[] recordHeaders = headers(sourceTopic, param.topicIdPartition().partition(),
                    offset, param.groupId(), param.deliveryCount(), param.cause());
            if (baseTimestamp == null) {
                baseTimestamp = timestamp;
            }
            int recordSize = DefaultRecord.sizeInBytes(simpleRecords.size(), timestamp - baseTimestamp, key, value, recordHeaders);

            if (batchSize + recordSize > maxMessageBytes && !simpleRecords.isEmpty()) {
                // Adding this record would exceed the limit and the batch already has at least one
                // record - stop here and send the rest in a follow-up request.
                break;
            }
            simpleRecords.add(new SimpleRecord(timestamp, key, value, recordHeaders));
            batchSize += recordSize;
            if (batchSize > maxMessageBytes) {
                // A single record (with its DLQ headers) already exceeds the limit on its own;
                // nothing to be gained by holding it back, so send it and let the broker
                // enforce/report the ultimate limit for this one, rather than stalling forever.
                break;
            }
        }

        long lastOffsetIncluded = nextOffsetToSend + simpleRecords.size() - 1;
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                simpleRecords.toArray(new SimpleRecord[]{})
        );
        return new BuildResult(records, lastOffsetIncluded, simpleRecords.size());
    }

    /**
     * Optionally fetches source records for DLQ copy-record mode. If copy-record is disabled
     * for the group, returns an empty result immediately.
     *
     * @param param              The DLQ record parameter
     * @param fromOffset         The first offset to fetch from
     * @param cacheHelper        Metadata cache helper for config lookups
     * @param logReader          Log reader for fetching source records
     * @param time               Time instance
     * @param topicNameResolver  Resolves the raw DLQ topic name to the name used in the metadata
     *                           cache. K2 prepends the tenant prefix; K1 passes {@link Function#identity()}.
     * @return A future with the fetch result (empty map if copy-record is disabled)
     */
    public static CompletableFuture<ShareGroupDLQRecordFetcher.FetchResult> maybeFetchSourceRecords(
            ShareGroupDLQRecordParameter param,
            long fromOffset,
            ShareGroupDLQMetadataCacheHelper cacheHelper,
            LogReader logReader,
            Time time,
            Function<String, String> topicNameResolver
    ) {
        if (!cacheHelper.isShareGroupDlqCopyRecordEnabled(param.groupId())) {
            return CompletableFuture.completedFuture(
                    new ShareGroupDLQRecordFetcher.FetchResult(Map.of(), param.lastOffset()));
        }

        // Bounds decompression memory against a pathologically compressible source record: there's
        // no point retaining more decompressed data than the DLQ topic could ever accept anyway, and
        // (unlike the user's own topic config) this value isn't controlled by whoever produced the
        // record being copied. Falls back to DLQ_MAX_FETCH_BYTES in the (defensive-only) case the DLQ
        // topic isn't resolvable here - copy-record being enabled implies one is configured in practice.
        int maxDecompressedBytes = cacheHelper.shareGroupDlqTopic(param.groupId())
                .map(topicNameResolver)
                .map(cacheHelper::dlqTopicMaxMessageBytes)
                .orElse(DLQ_MAX_FETCH_BYTES);

        // param itself is never mutated - headers()/topicProduceData() rely on its original,
        // unwindowed firstOffset/lastOffset for the handler's whole lifetime. Build a throwaway
        // windowed copy only to scope this round's fetch (and its decompression budget) to what's
        // left to send.
        ShareGroupDLQRecordParameter window;
        if (fromOffset == param.firstOffset()) {
            window = param;
        } else {
            window = new ShareGroupDLQRecordParameter(param.groupId(), param.topicIdPartition(), fromOffset,
                    param.lastOffset(), param.deliveryCount(), param.cause());
        }
        return new ShareGroupDLQRecordFetcher(logReader, time, window, DLQ_MAX_FETCH_BYTES, maxDecompressedBytes).fetch();
    }
}
