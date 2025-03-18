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
package org.apache.kafka.server.common;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.purgatory.DelayedOperation;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DeleteRecordsPartitionStatus {
    private final long requiredOffset;
    private final DeleteRecordsResponseData.DeleteRecordsPartitionResult responseStatus;
    private volatile boolean acksPending;

    public DeleteRecordsPartitionStatus(long requiredOffset,
                                        DeleteRecordsResponseData.DeleteRecordsPartitionResult responseStatus) {
        this.requiredOffset = requiredOffset;
        this.responseStatus = responseStatus;
        this.acksPending = false;
    }

    public boolean isAcksPending() {
        return acksPending;
    }

    public void setAcksPending(boolean acksPending) {
        this.acksPending = acksPending;
    }


    public DeleteRecordsResponseData.DeleteRecordsPartitionResult getResponseStatus() {
        return responseStatus;
    }

    public long getRequiredOffset() {
        return requiredOffset;
    }

    @Override
    public String toString() {
        return String.format("[acksPending: %b, error: %s, lowWatermark: %d, requiredOffset: %d]",
                acksPending, Errors.forCode(responseStatus.errorCode()).toString(), responseStatus.lowWatermark(),
                requiredOffset);
    }

    /**
     * A delayed delete records operation that can be created by the replica manager and watched
     * in the delete records operation purgatory
     */
    public static class DelayedDeleteRecords extends DelayedOperation {
        private final Map<TopicPartition, DeleteRecordsPartitionStatus> deleteRecordsStatus;
        private final BiConsumer<TopicPartition, DeleteRecordsPartitionStatus> onAcksPending;
        private final Consumer<Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback;
        private static final Logger log = LoggerFactory.getLogger(DelayedDeleteRecords.class);

        public DelayedDeleteRecords(long delayMs,
                                    Map<TopicPartition, DeleteRecordsPartitionStatus> deleteRecordsStatus,
                                    //  To maintain compatibility with dependency packages, the logic has been moved to the caller.
                                    BiConsumer<TopicPartition, DeleteRecordsPartitionStatus> onAcksPending,
                                    Consumer<Map<TopicPartition,
                                            DeleteRecordsResponseData.DeleteRecordsPartitionResult>> responseCallback) {
            super(delayMs);
            this.onAcksPending = onAcksPending;
            this.deleteRecordsStatus = new ConcurrentHashMap<>(deleteRecordsStatus);
            this.responseCallback = responseCallback;
            // first update the acks pending variable according to the error code
            deleteRecordsStatus.forEach((topicPartition, status) -> {
                if (status.getResponseStatus().errorCode() == Errors.NONE.code()) {
                    // Timeout error state will be cleared when required acks are received
                    status.setAcksPending(true);
                    status.getResponseStatus().setErrorCode(Errors.REQUEST_TIMED_OUT.code());
                } else {
                    status.setAcksPending(false);
                }
            });
        }

        /**
         * The delayed delete records operation can be completed if every partition specified in the request satisfied one of the following:
         *
         * 1) There was an error while checking if all replicas have caught up to the deleteRecordsOffset: set an error in response
         * 2) The low watermark of the partition has caught up to the deleteRecordsOffset. set the low watermark in response
         *
         */
        @Override
        public boolean tryComplete() {
            deleteRecordsStatus.forEach((topicPartition, status) -> {
                log.trace("Checking delete records satisfaction for {}, current status {}", topicPartition, status);
                if (status.isAcksPending()) {
                    onAcksPending.accept(topicPartition, status);
                }
            });
            return deleteRecordsStatus.values().stream().noneMatch(DeleteRecordsPartitionStatus::isAcksPending) && forceComplete();
        }

        @Override
        public void onExpiration() {
            deleteRecordsStatus.forEach((topicPartition, status) -> {
                if (status.isAcksPending()) {
                    DelayedDeleteRecordsMetrics.recordExpiration(topicPartition);
                }
            });
        }

        /**
         * Upon completion, return the current response status along with the error code per partition
         */
        @Override
        public void onComplete() {
            Map<TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult> responseStatus =
                    new HashMap<>();
            deleteRecordsStatus.forEach((k, status) -> {
                responseStatus.put(k, status.getResponseStatus());
            });
            responseCallback.accept(responseStatus);
        }
    }

    private static class DelayedDeleteRecordsMetrics {
        private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup(DelayedDeleteRecordsMetrics.class);
        private static final Meter AGGREGATE_EXPIRATION_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests",
                TimeUnit.SECONDS);

        public static void recordExpiration(TopicPartition partition) {
            AGGREGATE_EXPIRATION_METER.mark();
        }
    }
}
