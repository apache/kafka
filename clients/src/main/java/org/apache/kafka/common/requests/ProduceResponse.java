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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This wrapper supports both v0 and v8 of ProduceResponse.
 *
 * Possible error code:
 *
 * {@link Errors#CORRUPT_MESSAGE}
 * {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 * {@link Errors#NOT_LEADER_OR_FOLLOWER}
 * {@link Errors#MESSAGE_TOO_LARGE}
 * {@link Errors#INVALID_TOPIC_EXCEPTION}
 * {@link Errors#RECORD_LIST_TOO_LARGE}
 * {@link Errors#NOT_ENOUGH_REPLICAS}
 * {@link Errors#NOT_ENOUGH_REPLICAS_AFTER_APPEND}
 * {@link Errors#INVALID_REQUIRED_ACKS}
 * {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT}
 * {@link Errors#INVALID_PRODUCER_EPOCH}
 * {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 * {@link Errors#INVALID_RECORD}
 */
public class ProduceResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    private final ProduceResponseData data;

    public ProduceResponse(ProduceResponseData produceResponseData) {
        super(ApiKeys.PRODUCE);
        this.data = produceResponseData;
    }

    /**
     * Constructor for Version 0
     * @param responses Produced data grouped by topic-partition
     */
    @Deprecated
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
        this(responses, DEFAULT_THROTTLE_TIME);
    }

    /**
     * Constructor for the latest version
     * @param responses Produced data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    @Deprecated
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTimeMs) {
        this(toData(responses, throttleTimeMs));
    }

    private static ProduceResponseData toData(Map<TopicPartition, PartitionResponse> responses, int throttleTimeMs) {
        ProduceResponseData data = new ProduceResponseData().setThrottleTimeMs(throttleTimeMs);
        responses.forEach((tp, response) -> {
            ProduceResponseData.TopicProduceResponse tpr = data.responses().find(tp.topic());
            if (tpr == null) {
                tpr = new ProduceResponseData.TopicProduceResponse().setName(tp.topic());
                data.responses().add(tpr);
            }
            tpr.partitionResponses()
                .add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(tp.partition())
                    .setBaseOffset(response.baseOffset)
                    .setLogStartOffset(response.logStartOffset)
                    .setLogAppendTimeMs(response.logAppendTime)
                    .setErrorMessage(response.errorMessage)
                    .setErrorCode(response.error.code())
                    .setRecordErrors(response.recordErrors
                        .stream()
                        .map(e -> new ProduceResponseData.BatchIndexAndErrorMessage()
                            .setBatchIndex(e.batchIndex)
                            .setBatchIndexErrorMessage(e.message))
                        .collect(Collectors.toList())));
        });
        return data;
    }

    @Override
    public ProduceResponseData data() {
        return this.data;
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.responses().forEach(t -> t.partitionResponses().forEach(p -> updateErrorCounts(errorCounts, Errors.forCode(p.errorCode()))));
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

        public PartitionResponse(Errors error, String errorMessage) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET, Collections.emptyList(), errorMessage);
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionResponse that = (PartitionResponse) o;
            return baseOffset == that.baseOffset &&
                    logAppendTime == that.logAppendTime &&
                    logStartOffset == that.logStartOffset &&
                    error == that.error &&
                    Objects.equals(recordErrors, that.recordErrors) &&
                    Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(error, baseOffset, logAppendTime, logStartOffset, recordErrors, errorMessage);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RecordError that = (RecordError) o;
            return batchIndex == that.batchIndex &&
                    Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(batchIndex, message);
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
