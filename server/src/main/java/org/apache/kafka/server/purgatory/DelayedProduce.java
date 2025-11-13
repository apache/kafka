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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
public class DelayedProduce extends DelayedOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedProduce.class);

    public static final class ProducePartitionStatus {
        private final long requiredOffset;
        private final PartitionResponse responseStatus;

        private volatile boolean acksPending;

        public ProducePartitionStatus(long requiredOffset, PartitionResponse responseStatus) {
            this.requiredOffset = requiredOffset;
            this.responseStatus = responseStatus;
        }

        public long requiredOffset() {
            return requiredOffset;
        }

        public PartitionResponse responseStatus() {
            return responseStatus;
        }

        public boolean acksPending() {
            return acksPending;
        }

        public void setAcksPending(boolean acksPending) {
            this.acksPending = acksPending;
        }

        @Override
        public String toString() {
            return String.format(
                    "[acksPending: %s, error: %s, startOffset: %s, requiredOffset: %d]",
                    acksPending,
                    responseStatus.error.code(),
                    responseStatus.baseOffset,
                    requiredOffset
            );
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ProducePartitionStatus that = (ProducePartitionStatus) o;
            return requiredOffset == that.requiredOffset && acksPending == that.acksPending && Objects.equals(responseStatus, that.responseStatus);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredOffset, responseStatus, acksPending);
        }
    }

    /**
     * The produce metadata maintained by the delayed produce operation
     */
    public static final class ProduceMetadata {
        private final short produceRequiredAcks;
        private final Map<TopicIdPartition, ProducePartitionStatus> produceStatus;

        public ProduceMetadata(short produceRequiredAcks,
                               Map<TopicIdPartition, ProducePartitionStatus> produceStatus) {
            this.produceRequiredAcks = produceRequiredAcks;
            this.produceStatus = produceStatus;
        }

        @Override
        public String toString() {
            return String.format(
                    "[requiredAcks: %d, partitionStatus: %s]",
                    produceRequiredAcks,
                    produceStatus
            );
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ProduceMetadata that = (ProduceMetadata) o;
            return produceRequiredAcks == that.produceRequiredAcks && Objects.equals(produceStatus, that.produceStatus);
        }

        @Override
        public int hashCode() {
            return Objects.hash(produceRequiredAcks, produceStatus);
        }
    }

    private static final class DelayedProduceMetrics {
        // Changing the package or class name may cause incompatibility with existing code and metrics configuration
        private static final String METRICS_PACKAGE = "kafka.server";
        private static final String METRICS_CLASS_NAME = "DelayedProduceMetrics";
        private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup(METRICS_PACKAGE, METRICS_CLASS_NAME);
        private static final Meter AGGREGATE_EXPIRATION_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);
        private static final ConcurrentHashMap<TopicPartition, Meter> PARTITION_EXPIRATION_METERS = new ConcurrentHashMap<>();

        private static void recordExpiration(TopicPartition partition) {
            AGGREGATE_EXPIRATION_METER.mark();
            PARTITION_EXPIRATION_METERS.computeIfAbsent(partition,
                            key -> METRICS_GROUP.newMeter("ExpiresPerSec",
                                        "requests",
                                        TimeUnit.SECONDS,
                                        Map.of("topic", key.topic(), "partition", String.valueOf(key.partition())))
                                    ).mark();
        }
    }

    private final ProduceMetadata produceMetadata;
    private final BiFunction<TopicPartition, Long, Map<Boolean, Errors>> validatePartitionAndReplicaStatus;
    private final Consumer<Map<TopicIdPartition, PartitionResponse>> responseCallback;

    public DelayedProduce(long delayMs,
                          ProduceMetadata produceMetadata,
                          BiFunction<TopicPartition, Long, Map<Boolean, Errors>> validatePartitionAndReplicaStatus,
                          Consumer<Map<TopicIdPartition, PartitionResponse>> responseCallback) {
        super(delayMs);

        this.produceMetadata = produceMetadata;
        this.validatePartitionAndReplicaStatus = validatePartitionAndReplicaStatus;
        this.responseCallback = responseCallback;

        // first update the acks pending variable according to the error code
        produceMetadata.produceStatus.forEach((topicPartition, status) -> {
            if (status.responseStatus.error == Errors.NONE) {
                // Timeout error state will be cleared when required acks are received
                status.acksPending = true;
                status.responseStatus.error = Errors.REQUEST_TIMED_OUT;
            } else {
                status.acksPending = false;
            }

            LOGGER.trace("Initial partition status for {} is {}", topicPartition, status);
        });
    }

    /**
     * The delayed produce operation can be completed if every partition
     * it produces to is satisfied by one of the following:
     *
     * Case A: Replica not assigned to partition
     * Case B: Replica is no longer the leader of this partition
     * Case C: This broker is the leader:
     *   C.1 - If there was a local error thrown while checking if at least requiredAcks
     *         replicas have caught up to this operation: set an error in response
     *   C.2 - Otherwise, set the response with no error.
     *
     * These cases were originally validated by some methods in the ReplicaManager.
     * However, since DelayedProduce has been moved to the server module, it cannot directly access the ReplicaManager.
     * Therefore, these validations have been delegated to the method within `ReplicaManager#maybeAddDelayedProduce()`.
     */
    @Override
    public boolean tryComplete() {
        // check for each partition if it still has pending acks
        produceMetadata.produceStatus.forEach((topicIdPartition, status) -> {
            LOGGER.trace("Checking produce satisfaction for {}, current status {}", topicIdPartition, status);
            // skip those partitions that have already been satisfied
            if (status.acksPending) {
                // Delegate to `ReplicaManager#maybeAddDelayedProduce`
                // Validate Cases A, B, or C
                Map.Entry<Boolean, Errors> result = validatePartitionAndReplicaStatus
                        .apply(topicIdPartition.topicPartition(), status.requiredOffset)
                        .entrySet().stream()
                        .findFirst()
                        .get(); // Safe to call get() with isPresent() as the result must exist.


                // Update the partition status to reflect Case A, B, or C:
                boolean hasEnough = result.getKey();
                Errors errors = result.getValue();
                if (errors != Errors.NONE || hasEnough) {
                    status.setAcksPending(false);
                    status.responseStatus.error = errors;
                }
            }
        });

        // check if every partition has satisfied at least one of case A, B or C
        boolean anyPending = produceMetadata.produceStatus
                                            .values()
                                            .stream()
                                            .anyMatch(ProducePartitionStatus::acksPending);
        if (!anyPending) {
            return forceComplete();
        }

        return false;
    }

    @Override
    public void onExpiration() {
        produceMetadata.produceStatus.forEach((topicIdPartition, status) -> {
            if (status.acksPending) {
                LOGGER.debug("Expiring produce request for partition {} with status {}", topicIdPartition, status);
                DelayedProduceMetrics.recordExpiration(topicIdPartition.topicPartition());
            }
        });
    }

    /**
     * Upon completion, return the current response status along with the error code per partition
     */
    @Override
    public void onComplete() {
        Map<TopicIdPartition, PartitionResponse> responseStatus =
                produceMetadata.produceStatus.entrySet().stream().
                        collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().responseStatus()));

        responseCallback.accept(responseStatus);
    }
}
