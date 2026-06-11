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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;

import java.util.Objects;

/**
 * This class contains the information for a single batch of state information for use by the {@link Persister}.
 */
public class PersisterStateBatch implements Comparable<PersisterStateBatch> {
    public static final long NO_STAGED_PRODUCER_ID = -1L;
    public static final short NO_STAGED_PRODUCER_EPOCH = -1;
    public static final byte NO_STAGED_ACK_TYPE = -1;

    private final long firstOffset;
    private final long lastOffset;
    private final short deliveryCount;
    private final byte deliveryState;
    private final long stagedProducerId;
    private final short stagedProducerEpoch;
    private final byte stagedAckType;

    public PersisterStateBatch(long firstOffset, long lastOffset, byte deliveryState, short deliveryCount) {
        this(
            firstOffset,
            lastOffset,
            deliveryState,
            deliveryCount,
            NO_STAGED_PRODUCER_ID,
            NO_STAGED_PRODUCER_EPOCH,
            NO_STAGED_ACK_TYPE
        );
    }

    public PersisterStateBatch(
        long firstOffset,
        long lastOffset,
        byte deliveryState,
        short deliveryCount,
        long stagedProducerId,
        short stagedProducerEpoch,
        byte stagedAckType
    ) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.deliveryState = deliveryState;
        this.deliveryCount = deliveryCount;
        this.stagedProducerId = stagedProducerId;
        this.stagedProducerEpoch = stagedProducerEpoch;
        this.stagedAckType = stagedAckType;
    }

    public long firstOffset() {
        return firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public byte deliveryState() {
        return deliveryState;
    }

    public short deliveryCount() {
        return deliveryCount;
    }

    public long stagedProducerId() {
        return stagedProducerId;
    }

    public short stagedProducerEpoch() {
        return stagedProducerEpoch;
    }

    public byte stagedAckType() {
        return stagedAckType;
    }

    public static PersisterStateBatch from(ReadShareGroupStateResponseData.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount(),
            batch.stagedProducerId(),
            batch.stagedProducerEpoch(),
            batch.stagedAckType()
        );
    }

    public static PersisterStateBatch from(WriteShareGroupStateRequestData.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount(),
            batch.stagedProducerId(),
            batch.stagedProducerEpoch(),
            batch.stagedAckType()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersisterStateBatch that = (PersisterStateBatch) o;
        return firstOffset == that.firstOffset &&
            lastOffset == that.lastOffset &&
            deliveryCount == that.deliveryCount &&
            deliveryState == that.deliveryState &&
            stagedProducerId == that.stagedProducerId &&
            stagedProducerEpoch == that.stagedProducerEpoch &&
            stagedAckType == that.stagedAckType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstOffset, lastOffset, deliveryCount, deliveryState, stagedProducerId, stagedProducerEpoch, stagedAckType);
    }

    @Override
    public String toString() {
        return "PersisterStateBatch(" +
            "firstOffset=" + firstOffset + "," +
            "lastOffset=" + lastOffset + "," +
            "deliveryCount=" + deliveryCount + "," +
            "deliveryState=" + deliveryState + "," +
            "stagedProducerId=" + stagedProducerId + "," +
            "stagedProducerEpoch=" + stagedProducerEpoch + "," +
            "stagedAckType=" + stagedAckType +
            ")";
    }

    /**
     * Compares 2 PersisterStateBatches in various dimensions.
     * The priority of the dimensions are:
     * - firstOffset
     * - lastOffset
     * - deliveryCount
     * - deliveryState
     * - stagedProducerId
     * - stagedProducerEpoch
     * - stagedAckType
     * <p>
     * Does not check all dimensions in every case. The first dimension
     * check resulting in non-zero comparison result is returned.
     * <p>
     * In case the 2 objects are equal, all dimension comparisons must
     * be 0.
     * <p>
     * This method could be used for storing PersisterStateBatch objects
     * in containers which allow a Comparator argument or various sort algorithms
     * in the java library.
     *
     * @param other - object representing another PersisterStateBatch
     * @return -INT, 0, +INT based on "this" being smaller, equal or larger than the argument.
     */
    @Override
    public int compareTo(PersisterStateBatch other) {
        int deltaFirst = Long.compare(this.firstOffset(), other.firstOffset());
        if (deltaFirst == 0) {
            int deltaLast = Long.compare(this.lastOffset(), other.lastOffset());
            if (deltaLast == 0) {
                int deltaCount = this.deliveryCount() - other.deliveryCount();
                if (deltaCount == 0) {
                    int deltaState = Byte.compare(this.deliveryState(), other.deliveryState());
                    if (deltaState == 0) {
                        int deltaProducerId = Long.compare(this.stagedProducerId(), other.stagedProducerId());
                        if (deltaProducerId == 0) {
                            int deltaProducerEpoch = Short.compare(this.stagedProducerEpoch(), other.stagedProducerEpoch());
                            if (deltaProducerEpoch == 0) {
                                return Byte.compare(this.stagedAckType(), other.stagedAckType());
                            }
                            return deltaProducerEpoch;
                        }
                        return deltaProducerId;
                    }
                    return deltaState;
                }
                return deltaCount;
            }
            return deltaLast;
        }
        return deltaFirst;
    }
}
