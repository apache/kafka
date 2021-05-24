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

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class ProducerState {
    private final long producerId;
    private final int producerEpoch;
    private final int lastSequence;
    private final long lastTimestamp;
    private final OptionalInt coordinatorEpoch;
    private final OptionalLong currentTransactionStartOffset;

    public ProducerState(
        long producerId,
        int producerEpoch,
        int lastSequence,
        long lastTimestamp,
        OptionalInt coordinatorEpoch,
        OptionalLong currentTransactionStartOffset
    ) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.lastSequence = lastSequence;
        this.lastTimestamp = lastTimestamp;
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTransactionStartOffset = currentTransactionStartOffset;
    }

    public long producerId() {
        return producerId;
    }

    public int producerEpoch() {
        return producerEpoch;
    }

    public int lastSequence() {
        return lastSequence;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public OptionalLong currentTransactionStartOffset() {
        return currentTransactionStartOffset;
    }

    public OptionalInt coordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerState that = (ProducerState) o;
        return producerId == that.producerId &&
            producerEpoch == that.producerEpoch &&
            lastSequence == that.lastSequence &&
            lastTimestamp == that.lastTimestamp &&
            Objects.equals(coordinatorEpoch, that.coordinatorEpoch) &&
            Objects.equals(currentTransactionStartOffset, that.currentTransactionStartOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, producerEpoch, lastSequence, lastTimestamp,
            coordinatorEpoch, currentTransactionStartOffset);
    }

    @Override
    public String toString() {
        return "ProducerState(" +
            "producerId=" + producerId +
            ", producerEpoch=" + producerEpoch +
            ", lastSequence=" + lastSequence +
            ", lastTimestamp=" + lastTimestamp +
            ", coordinatorEpoch=" + coordinatorEpoch +
            ", currentTransactionStartOffset=" + currentTransactionStartOffset +
            ')';
    }
}
