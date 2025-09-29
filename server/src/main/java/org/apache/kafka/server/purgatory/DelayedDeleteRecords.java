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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A delayed delete records operation that can be created by the replica manager and watched
 * in the delete records operation purgatory
 */
public class DelayedDeleteRecords extends DelayedOperation {
    
    private static final Logger LOG = LoggerFactory.getLogger(DelayedDeleteRecords.class);
    
    //  migration from kafka.server.DelayedDeleteRecordsMetrics
    private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup("kafka.server", "DelayedDeleteRecordsMetrics");
    private static final Meter AGGREGATE_EXPIRATION_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests",
            TimeUnit.SECONDS);
    
    private final Map<TopicPartition, DeleteRecordsPartitionStatus> deleteRecordsStatus;
    private final BiConsumer<TopicPartition, DeleteRecordsPartitionStatus> onAcksPending;
    private final Consumer<Map<TopicPartition, DeleteRecordsPartitionResult>> responseCallback;

    public DelayedDeleteRecords(long delayMs,
                                Map<TopicPartition, DeleteRecordsPartitionStatus> deleteRecordsStatus,
                                //  To maintain compatibility with dependency packages, the logic has been moved to the caller.
                                BiConsumer<TopicPartition, DeleteRecordsPartitionStatus> onAcksPending,
                                Consumer<Map<TopicPartition, DeleteRecordsPartitionResult>> responseCallback) {
        super(delayMs);
        this.onAcksPending = onAcksPending;
        this.deleteRecordsStatus = Map.copyOf(deleteRecordsStatus);
        this.responseCallback = responseCallback;
        // first update the acks pending variable according to the error code
        deleteRecordsStatus.forEach((topicPartition, status) -> {
            if (status.responseStatus().errorCode() == Errors.NONE.code()) {
                // Timeout error state will be cleared when required acks are received
                status.setAcksPending(true);
                status.responseStatus().setErrorCode(Errors.REQUEST_TIMED_OUT.code());
            } else {
                status.setAcksPending(false);
            }
            
            LOG.trace("Initial partition status for {} is {}", topicPartition, status);
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
        //  check for each partition if it still has pending acks
        deleteRecordsStatus.forEach((topicPartition, status) -> {
            LOG.trace("Checking delete records satisfaction for {}, current status {}", topicPartition, status);
            //  skip those partitions that have already been satisfied
            if (status.acksPending()) {
                onAcksPending.accept(topicPartition, status);
            }
        });
        //  check if every partition has satisfied at least one of case A or B
        return deleteRecordsStatus.values().stream().noneMatch(DeleteRecordsPartitionStatus::acksPending) && forceComplete();
    }

    @Override
    public void onExpiration() {
        AGGREGATE_EXPIRATION_METER.mark(deleteRecordsStatus.values().stream().filter(DeleteRecordsPartitionStatus::acksPending).count());
    }

    /**
     * Upon completion, return the current response status along with the error code per partition
     */
    @Override
    public void onComplete() {
        responseCallback.accept(deleteRecordsStatus.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().responseStatus())));
    }
}
