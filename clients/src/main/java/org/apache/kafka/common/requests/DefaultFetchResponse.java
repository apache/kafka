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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * This wrapper supports all versions of the Fetch API
 */
public class DefaultFetchResponse extends WriteableFetchResponse<DefaultFetchResponse.PartitionData> {
    public DefaultFetchResponse(Errors error,
                                LinkedHashMap<TopicPartition, PartitionData> responseData,
                                int throttleTimeMs,
                                int sessionId) {
        super(error, responseData, throttleTimeMs, sessionId);
    }

    public static DefaultFetchResponse constructFetchResponse(Struct struct) {
        LinkedHashMap<TopicPartition, PartitionData> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
                int partition = partitionResponseHeader.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponseHeader.get(ERROR_CODE));
                long highWatermark = partitionResponseHeader.getLong(HIGH_WATERMARK_KEY_NAME);
                long lastStableOffset = INVALID_LAST_STABLE_OFFSET;
                if (partitionResponseHeader.hasField(LAST_STABLE_OFFSET_KEY_NAME))
                    lastStableOffset = partitionResponseHeader.getLong(LAST_STABLE_OFFSET_KEY_NAME);
                long logStartOffset = INVALID_LOG_START_OFFSET;
                if (partitionResponseHeader.hasField(LOG_START_OFFSET_KEY_NAME))
                    logStartOffset = partitionResponseHeader.getLong(LOG_START_OFFSET_KEY_NAME);

                Records records = (Records) partitionResponse.getRecords(RECORD_SET_KEY_NAME);

                List<AbortedTransaction> abortedTransactions = null;
                if (partitionResponseHeader.hasField(ABORTED_TRANSACTIONS_KEY_NAME)) {
                    Object[] abortedTransactionsArray = partitionResponseHeader.getArray(ABORTED_TRANSACTIONS_KEY_NAME);
                    if (abortedTransactionsArray != null) {
                        abortedTransactions = new ArrayList<>(abortedTransactionsArray.length);
                        for (Object abortedTransactionObj : abortedTransactionsArray) {
                            Struct abortedTransactionStruct = (Struct) abortedTransactionObj;
                            long producerId = abortedTransactionStruct.getLong(PRODUCER_ID_KEY_NAME);
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
        return new DefaultFetchResponse(Errors.forCode(struct.getOrElse(ERROR_CODE, (short) 0)),
                responseData,
                struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME),
                struct.getOrElse(SESSION_ID, INVALID_SESSION_ID));
    }

    public static DefaultFetchResponse parse(ByteBuffer buffer, short version) {
        return constructFetchResponse(ApiKeys.FETCH.responseSchema(version).read(buffer));
    }

    public static class PartitionData extends WriteableFetchResponse.PartitionData {
        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             List<AbortedTransaction> abortedTransactions,
                             Records records) {
            super(error, highWatermark, lastStableOffset, logStartOffset, abortedTransactions, records);
        }

        @Override
        public Records records() {
            return (Records) super.records();
        }
    }
}
