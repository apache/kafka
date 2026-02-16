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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * Specification of a transaction to abort on a specific topic partition.
 * <p>
 * This is used to forcefully abort a hanging transaction that was not completed by its producer
 * (e.g., because the producer crashed). The required fields ({@code producerId}, {@code producerEpoch},
 * {@code coordinatorEpoch}) can be obtained by inspecting the partition's transaction state via
 * {@link Admin#describeProducers(java.util.Collection)}.
 *
 * @see Admin#abortTransaction(AbortTransactionSpec)
 */
public class AbortTransactionSpec {
    private final TopicPartition topicPartition;
    private final long producerId;
    private final short producerEpoch;
    private final int coordinatorEpoch;

    /**
     * @param topicPartition   The topic partition where the transaction is open
     * @param producerId       The ID of the producer that initiated the transaction
     * @param producerEpoch    The epoch of the producer
     * @param coordinatorEpoch The epoch of the transaction coordinator
     */
    public AbortTransactionSpec(
        TopicPartition topicPartition,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch
    ) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    /**
     * The topic partition on which the transaction is open and should be aborted.
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * The producer ID of the open transaction to abort. This can be obtained from
     * {@link org.apache.kafka.clients.admin.DescribeProducersResult}.
     */
    public long producerId() {
        return producerId;
    }

    /**
     * The epoch of the producer. Each time a producer is initialized (or recovers from a failure),
     * its epoch is incremented. This ensures that stale producers from previous sessions cannot
     * interfere with the current transaction.
     */
    public short producerEpoch() {
        return producerEpoch;
    }

    /**
     * The epoch of the transaction coordinator at the time the transaction was started. This is
     * used for fencing: the broker rejects abort requests with a stale coordinator epoch to prevent
     * aborting transactions that may have already been committed by a new coordinator.
     */
    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbortTransactionSpec that = (AbortTransactionSpec) o;
        return producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            coordinatorEpoch == that.coordinatorEpoch &&
            Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, producerId, producerEpoch, coordinatorEpoch);
    }

    @Override
    public String toString() {
        return "AbortTransactionSpec(" +
            "topicPartition=" + topicPartition +
            ", producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", coordinatorEpoch=" + coordinatorEpoch +
            ')';
    }

}
