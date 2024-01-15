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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

@InterfaceStability.Evolving
public class TransactionDescription {
    private final int coordinatorId;
    private final TransactionState state;
    private final long producerId;
    private final int producerEpoch;
    private final long transactionTimeoutMs;
    private final OptionalLong transactionStartTimeMs;
    private final Set<TopicPartition> topicPartitions;

    public TransactionDescription(
        int coordinatorId,
        TransactionState state,
        long producerId,
        int producerEpoch,
        long transactionTimeoutMs,
        OptionalLong transactionStartTimeMs,
        Set<TopicPartition> topicPartitions
    ) {
        this.coordinatorId = coordinatorId;
        this.state = state;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionStartTimeMs = transactionStartTimeMs;
        this.topicPartitions = topicPartitions;
    }

    public int coordinatorId() {
        return coordinatorId;
    }

    public TransactionState state() {
        return state;
    }

    public long producerId() {
        return producerId;
    }

    public int producerEpoch() {
        return producerEpoch;
    }

    public long transactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    public OptionalLong transactionStartTimeMs() {
        return transactionStartTimeMs;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionDescription that = (TransactionDescription) o;
        return coordinatorId == that.coordinatorId &&
            producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            transactionTimeoutMs == that.transactionTimeoutMs &&
            state == that.state &&
            Objects.equals(transactionStartTimeMs, that.transactionStartTimeMs) &&
            Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinatorId, state, producerId, producerEpoch, transactionTimeoutMs, transactionStartTimeMs, topicPartitions);
    }

    @Override
    public String toString() {
        return "TransactionDescription(" +
            "coordinatorId=" + coordinatorId +
            ", state=" + state +
            ", producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", transactionTimeoutMs=" + transactionTimeoutMs +
            ", transactionStartTimeMs=" + transactionStartTimeMs +
            ", topicPartitions=" + topicPartitions +
            ')';
    }
}
