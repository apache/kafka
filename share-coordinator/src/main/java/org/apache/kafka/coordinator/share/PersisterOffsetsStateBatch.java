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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.PersisterStateBatch;

import java.util.Objects;

/**
 * This is a helper class which overrides the equals and hashcode
 * methods to only focus on the first and last offset fields of the
 * state batch. This is useful when combining batches.
 */
public class PersisterOffsetsStateBatch {
    private final PersisterStateBatch delegate;

    public PersisterOffsetsStateBatch(
        long firstOffset,
        long lastOffset,
        byte deliveryState,
        short deliveryCount
    ) {
        delegate = new PersisterStateBatch(firstOffset, lastOffset, deliveryState, deliveryCount);
    }

    public long firstOffset() {
        return delegate.firstOffset();
    }

    public long lastOffset() {
        return delegate.lastOffset();
    }

    public byte deliveryState() {
        return delegate.deliveryState();
    }

    public short deliveryCount() {
        return delegate.deliveryCount();
    }

    public static PersisterOffsetsStateBatch from(WriteShareGroupStateRequestData.StateBatch batch) {
        return new PersisterOffsetsStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }

    public static PersisterOffsetsStateBatch from(ShareUpdateValue.StateBatch batch) {
        return new PersisterOffsetsStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PersisterOffsetsStateBatch)) {
            return false;
        }
        PersisterOffsetsStateBatch that = (PersisterOffsetsStateBatch) o;
        return this.firstOffset() == that.firstOffset() && this.lastOffset() == that.lastOffset();
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstOffset(), lastOffset());
    }

    @Override
    public String toString() {
        return "PersisterOffsetsStateBatch(" +
            "firstOffset=" + firstOffset() + "," +
            "lastOffset=" + lastOffset() + "," +
            "deliveryState=" + deliveryState() + "," +
            "deliveryCount=" + deliveryCount() +
            ")";
    }
}
