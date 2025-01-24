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
public class PersisterStateBatch implements Comparable {
    private final long firstOffset;
    private final long lastOffset;
    private final short deliveryCount;
    private final byte deliveryState;

    public PersisterStateBatch(long firstOffset, long lastOffset, byte deliveryState, short deliveryCount) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.deliveryState = deliveryState;
        this.deliveryCount = deliveryCount;
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

    public static PersisterStateBatch from(ReadShareGroupStateResponseData.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount());
    }

    public static PersisterStateBatch from(WriteShareGroupStateRequestData.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersisterStateBatch that = (PersisterStateBatch) o;
        return firstOffset == that.firstOffset &&
            lastOffset == that.lastOffset &&
            deliveryCount == that.deliveryCount &&
            deliveryState == that.deliveryState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstOffset, lastOffset, deliveryCount, deliveryState);
    }

    @Override
    public String toString() {
        return "PersisterStateBatch(" +
            "firstOffset=" + firstOffset + "," +
            "lastOffset=" + lastOffset + "," +
            "deliveryCount=" + deliveryCount + "," +
            "deliveryState=" + deliveryState +
            ")";
    }

    /**
     * Compares 2 PersisterStateBatches in various dimensions.
     * The priority of the dimensions are:
     * - firstOffset
     * - lastOffset
     * - deliveryCount
     * - deliveryState
     * <p>
     * Does not check all dimensions in every case. The first dimension
     * check resulting in non-zero comparison result is returned.
     * <p>
     * In case the 2 objects are equal, all 4 dimension comparisons must
     * be 0.
     * <p>
     * This method could be used for storing PersisterStateBatch objects
     * in containers which allow a Comparator argument or various sort algorithms
     * in the java library.
     *
     * @param o - object representing another PersisterStateBatch
     * @return -INT, 0, +INT based on "this" being smaller, equal or larger than the argument.
     */
    @Override
    public int compareTo(Object o) {
        PersisterStateBatch that = (PersisterStateBatch) o;
        int deltaFirst = Long.compare(this.firstOffset(), that.firstOffset());
        if (deltaFirst == 0) {
            int deltaLast = Long.compare(this.lastOffset(), that.lastOffset());
            if (deltaLast == 0) {
                int deltaCount = this.deliveryCount() - that.deliveryCount();
                if (deltaCount == 0) {
                    return Byte.compare(this.deliveryState(), that.deliveryState());
                }
                return deltaCount;
            }
            return deltaLast;
        }
        return deltaFirst;
    }
}
