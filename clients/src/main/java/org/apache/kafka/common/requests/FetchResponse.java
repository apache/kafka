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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.MultiSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports all versions of the Fetch API
 */
public class FetchResponse extends AbstractResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String HIGH_WATERMARK_KEY_NAME = "high_watermark";
    private static final String LAST_STABLE_OFFSET_KEY_NAME = "last_stable_offset";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";
    private static final String ABORTED_TRANSACTIONS_KEY_NAME = "aborted_transactions";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    // aborted transaction field names
    private static final String PID_KEY_NAME = "producer_id";
    private static final String FIRST_OFFSET_KEY_NAME = "first_offset";

    private static final int DEFAULT_THROTTLE_TIME = 0;
    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    /**
     * Possible error codes:
     *
     *  OFFSET_OUT_OF_RANGE (1)
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  REPLICA_NOT_AVAILABLE (9)
     *  UNKNOWN (-1)
     */

    private final LinkedHashMap<TopicPartition, PartitionData> responseData;
    private final int throttleTimeMs;

    public static final class AbortedTransaction {
        public final long producerId;
        public final long firstOffset;

        public AbortedTransaction(long producerId, long firstOffset) {
            this.producerId = producerId;
            this.firstOffset = firstOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            AbortedTransaction that = (AbortedTransaction) o;

            return producerId == that.producerId && firstOffset == that.firstOffset;
        }

        @Override
        public int hashCode() {
            int result = (int) (producerId ^ (producerId >>> 32));
            result = 31 * result + (int) (firstOffset ^ (firstOffset >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "(producerId=" + producerId + ", firstOffset=" + firstOffset + ")";
        }
    }

    public static final class PartitionData {
        public final Errors error;
        public final long highWatermark;
        public final long lastStableOffset;
        public final long logStartOffset;
        public final List<AbortedTransaction> abortedTransactions;
        public final Records records;

        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             List<AbortedTransaction> abortedTransactions,
                             Records records) {
            this.error = error;
            this.highWatermark = highWatermark;
            this.lastStableOffset = lastStableOffset;
            this.logStartOffset = logStartOffset;
            this.abortedTransactions = abortedTransactions;
            this.records = records;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionData that = (PartitionData) o;

            return error == that.error &&
                    highWatermark == that.highWatermark &&
                    lastStableOffset == that.lastStableOffset &&
                    logStartOffset == that.logStartOffset &&
                    (abortedTransactions == null ? that.abortedTransactions == null : abortedTransactions.equals(that.abortedTransactions)) &&
                    (records == null ? that.records == null : records.equals(that.records));
        }

        @Override
        public int hashCode() {
            int result = error != null ? error.hashCode() : 0;
            result = 31 * result + (int) (highWatermark ^ (highWatermark >>> 32));
            result = 31 * result + (int) (lastStableOffset ^ (lastStableOffset >>> 32));
            result = 31 * result + (int) (logStartOffset ^ (logStartOffset >>> 32));
            result = 31 * result + (abortedTransactions != null ? abortedTransactions.hashCode() : 0);
            result = 31 * result + (records != null ? records.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "(error=" + error + ", highWaterMark=" + highWatermark +
                    ", lastStableOffset = " + lastStableOffset +
                    ", logStartOffset = " + logStartOffset +
                    ", abortedTransactions = " + abortedTransactions + ", records=" + records + ")";
        }
    }

    /**
     * Constructor for all versions.
     *
     * From version 3 or later, the entries in `responseData` should be in the same order as the entries in
     * `FetchRequest.fetchData`.
     *
     * @param responseData fetched data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    public FetchResponse(LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTimeMs) {
        this.responseData = responseData;
        this.throttleTimeMs = throttleTimeMs;
    }

    public FetchResponse(Struct struct) {
        LinkedHashMap<TopicPartition, PartitionData> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
                int partition = partitionResponseHeader.getInt(PARTITION_KEY_NAME);
                Errors error = Errors.forCode(partitionResponseHeader.getShort(ERROR_CODE_KEY_NAME));
                long highWatermark = partitionResponseHeader.getLong(HIGH_WATERMARK_KEY_NAME);
                long lastStableOffset = INVALID_LAST_STABLE_OFFSET;
                if (partitionResponseHeader.hasField(LAST_STABLE_OFFSET_KEY_NAME))
                    lastStableOffset = partitionResponseHeader.getLong(LAST_STABLE_OFFSET_KEY_NAME);
                long logStartOffset = INVALID_LOG_START_OFFSET;
                if (partitionResponseHeader.hasField(LOG_START_OFFSET_KEY_NAME))
                    logStartOffset = partitionResponseHeader.getLong(LOG_START_OFFSET_KEY_NAME);
                
                Records records = partitionResponse.getRecords(RECORD_SET_KEY_NAME);

                List<AbortedTransaction> abortedTransactions = null;
                if (partitionResponseHeader.hasField(ABORTED_TRANSACTIONS_KEY_NAME)) {
                    Object[] abortedTransactionsArray = partitionResponseHeader.getArray(ABORTED_TRANSACTIONS_KEY_NAME);
                    if (abortedTransactionsArray != null) {
                        abortedTransactions = new ArrayList<>(abortedTransactionsArray.length);
                        for (Object abortedTransactionObj : abortedTransactionsArray) {
                            Struct abortedTransactionStruct = (Struct) abortedTransactionObj;
                            long producerId = abortedTransactionStruct.getLong(PID_KEY_NAME);
                            long firstOffset = abortedTransactionStruct.getLong(FIRST_OFFSET_KEY_NAME);
                            abortedTransactions.add(new AbortedTransaction(producerId, firstOffset));
                        }
                    }
                }

                PartitionData partitionData = new PartitionData(error, highWatermark, lastStableOffset, logStartOffset,
                        abortedTransactions, records);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        this.responseData = responseData;
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
    }

    @Override
    public Struct toStruct(short version) {
        return toStruct(version, responseData, throttleTimeMs);
    }

    @Override
    public Send toSend(String dest, RequestHeader requestHeader) {
        return toSend(toStruct(requestHeader.apiVersion()), throttleTimeMs, dest, requestHeader);
    }

    public Send toSend(Struct responseStruct, int throttleTimeMs, String dest, RequestHeader requestHeader) {
        Struct responseHeader = new ResponseHeader(requestHeader.correlationId()).toStruct();

        // write the total size and the response header
        ByteBuffer buffer = ByteBuffer.allocate(responseHeader.sizeOf() + 4);
        buffer.putInt(responseHeader.sizeOf() + responseStruct.sizeOf());
        responseHeader.writeTo(buffer);
        buffer.rewind();

        List<Send> sends = new ArrayList<>();
        sends.add(new ByteBufferSend(dest, buffer));
        addResponseData(responseStruct, throttleTimeMs, dest, sends);
        return new MultiSend(dest, sends);
    }

    public LinkedHashMap<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }

    public static FetchResponse parse(ByteBuffer buffer, short version) {
        return new FetchResponse(ApiKeys.FETCH.responseSchema(version).read(buffer));
    }

    private static void addResponseData(Struct struct, int throttleTimeMs, String dest, List<Send> sends) {
        Object[] allTopicData = struct.getArray(RESPONSES_KEY_NAME);

        if (struct.hasField(THROTTLE_TIME_KEY_NAME)) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(throttleTimeMs);
            buffer.putInt(allTopicData.length);
            buffer.rewind();
            sends.add(new ByteBufferSend(dest, buffer));
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(allTopicData.length);
            buffer.rewind();
            sends.add(new ByteBufferSend(dest, buffer));
        }

        for (Object topicData : allTopicData)
            addTopicData(dest, sends, (Struct) topicData);
    }

    private static void addTopicData(String dest, List<Send> sends, Struct topicData) {
        String topic = topicData.getString(TOPIC_KEY_NAME);
        Object[] allPartitionData = topicData.getArray(PARTITIONS_KEY_NAME);

        // include the topic header and the count for the number of partitions
        ByteBuffer buffer = ByteBuffer.allocate(Type.STRING.sizeOf(topic) + 4);
        Type.STRING.write(buffer, topic);
        buffer.putInt(allPartitionData.length);
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        for (Object partitionData : allPartitionData)
            addPartitionData(dest, sends, (Struct) partitionData);
    }

    private static void addPartitionData(String dest, List<Send> sends, Struct partitionData) {
        Struct header = partitionData.getStruct(PARTITION_HEADER_KEY_NAME);
        Records records = partitionData.getRecords(RECORD_SET_KEY_NAME);

        // include the partition header and the size of the record set
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + 4);
        header.writeTo(buffer);
        buffer.putInt(records.sizeInBytes());
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        // finally the send for the record set itself
        sends.add(new RecordsSend(dest, records));
    }

    private static Struct toStruct(short version, LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTime) {
        Struct struct = new Struct(ApiKeys.FETCH.responseSchema(version));
        List<FetchRequest.TopicAndPartitionData<PartitionData>> topicsData = FetchRequest.TopicAndPartitionData.batchByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<PartitionData> topicEntry: topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                Struct partitionDataHeader = partitionData.instance(PARTITION_HEADER_KEY_NAME);
                partitionDataHeader.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionDataHeader.set(ERROR_CODE_KEY_NAME, fetchPartitionData.error.code());
                partitionDataHeader.set(HIGH_WATERMARK_KEY_NAME, fetchPartitionData.highWatermark);

                if (partitionDataHeader.hasField(LAST_STABLE_OFFSET_KEY_NAME)) {
                    partitionDataHeader.set(LAST_STABLE_OFFSET_KEY_NAME, fetchPartitionData.lastStableOffset);

                    if (fetchPartitionData.abortedTransactions == null) {
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, null);
                    } else {
                        List<Struct> abortedTransactionStructs = new ArrayList<>(fetchPartitionData.abortedTransactions.size());
                        for (AbortedTransaction abortedTransaction : fetchPartitionData.abortedTransactions) {
                            Struct abortedTransactionStruct = partitionDataHeader.instance(ABORTED_TRANSACTIONS_KEY_NAME);
                            abortedTransactionStruct.set(PID_KEY_NAME, abortedTransaction.producerId);
                            abortedTransactionStruct.set(FIRST_OFFSET_KEY_NAME, abortedTransaction.firstOffset);
                            abortedTransactionStructs.add(abortedTransactionStruct);
                        }
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, abortedTransactionStructs.toArray());
                    }
                }
                if (partitionDataHeader.hasField(LOG_START_OFFSET_KEY_NAME))
                    partitionDataHeader.set(LOG_START_OFFSET_KEY_NAME, fetchPartitionData.logStartOffset);

                partitionData.set(PARTITION_HEADER_KEY_NAME, partitionDataHeader);
                partitionData.set(RECORD_SET_KEY_NAME, fetchPartitionData.records);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());

        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTime);

        return struct;
    }

    public static int sizeOf(short version, LinkedHashMap<TopicPartition, PartitionData> responseData) {
        return 4 + toStruct(version, responseData, 0).sizeOf();
    }

}
