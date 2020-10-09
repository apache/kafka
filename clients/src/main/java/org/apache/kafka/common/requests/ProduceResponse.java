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
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This wrapper supports both v0 and v8 of ProduceResponse.
 */
public class ProduceResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    private final ProduceResponseData data;
    private final Map<TopicPartition, PartitionResponse> responses;

    public ProduceResponse(ProduceResponseData produceResponseData) {
        this.data = produceResponseData;
        this.responses = data.responses()
            .stream()
            .flatMap(t -> t.partitions()
                .stream()
                .map(p -> new AbstractMap.SimpleEntry<>(new TopicPartition(t.name(), p.partitionIndex()),
                    new PartitionResponse(
                        Errors.forCode(p.errorCode()),
                        p.baseOffset(),
                        p.logAppendTimeMs(),
                        p.logStartOffset(),
                        p.recordErrors()
                            .stream()
                            .map(e -> new RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                            .collect(Collectors.toList()),
                        p.errorMessage()))))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    /**
     * Constructor for Version 0
     * @param responses Produced data grouped by topic-partition
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
        this(responses, DEFAULT_THROTTLE_TIME);
    }

    /**
     * Constructor for the latest version
     * @param responses Produced data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTimeMs) {
        this.responses = responses;
        this.data = new ProduceResponseData()
            .setResponses(responses.entrySet()
                .stream()
                .collect(Collectors.groupingBy(e -> e.getKey().topic()))
                .entrySet()
                .stream()
                .map(topicData -> new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.getKey())
                    .setPartitions(topicData.getValue()
                        .stream()
                        .map(p -> new ProduceResponseData.PartitionProduceResponse()
                            .setPartitionIndex(p.getKey().partition())
                            .setBaseOffset(p.getValue().baseOffset)
                            .setLogStartOffset(p.getValue().logStartOffset)
                            .setLogAppendTimeMs(p.getValue().logAppendTime)
                            .setErrorMessage(p.getValue().errorMessage)
                            .setErrorCode(p.getValue().error.code())
                            .setRecordErrors(p.getValue().recordErrors
                                .stream()
                                .map(e -> new ProduceResponseData.BatchIndexAndErrorMessage()
                                    .setBatchIndex(e.batchIndex)
                                    .setBatchIndexErrorMessage(e.message))
                                .collect(Collectors.toList())))
                        .collect(Collectors.toList())))
                .collect(Collectors.toList()))
            .setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        responses.values().forEach(response ->
            updateErrorCounts(errorCounts, response.error)
        );
        return errorCounts;
    }

    public static final class PartitionResponse {
        public Errors error;
        public long baseOffset;
        public long logAppendTime;
        public long logStartOffset;
        public List<RecordError> recordErrors;
        public String errorMessage;

        public PartitionResponse(Errors error) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset) {
            this(error, baseOffset, logAppendTime, logStartOffset, Collections.emptyList(), null);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<RecordError> recordErrors) {
            this(error, baseOffset, logAppendTime, logStartOffset, recordErrors, null);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<RecordError> recordErrors, String errorMessage) {
            this.error = error;
            this.baseOffset = baseOffset;
            this.logAppendTime = logAppendTime;
            this.logStartOffset = logStartOffset;
            this.recordErrors = recordErrors;
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(error);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append(",logAppendTime: ");
            b.append(logAppendTime);
            b.append(", logStartOffset: ");
            b.append(logStartOffset);
            b.append(", recordErrors: ");
            b.append(recordErrors);
            b.append(", errorMessage: ");
            if (errorMessage != null) {
                b.append(errorMessage);
            } else {
                b.append("null");
            }
            b.append('}');
            return b.toString();
        }
    }

    public static final class RecordError {
        public final int batchIndex;
        public final String message;

        public RecordError(int batchIndex, String message) {
            this.batchIndex = batchIndex;
            this.message = message;
        }

        public RecordError(int batchIndex) {
            this.batchIndex = batchIndex;
            this.message = null;
        }

    }

    public static ProduceResponse parse(ByteBuffer buffer, short version) {
        return new ProduceResponse(new ProduceResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
