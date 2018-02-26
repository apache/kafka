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

import org.apache.kafka.common.protocol.Errors;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * The offset, fetched from a leader, for a particular partition.
 */

public class EpochEndOffset {
    public static final long UNDEFINED_EPOCH_OFFSET = NO_PARTITION_LEADER_EPOCH;
    public static final int UNDEFINED_EPOCH = -1;

    private Errors error;
    private long endOffset;

    public EpochEndOffset(Errors error, long endOffset) {
        this.error = error;
        this.endOffset = endOffset;
    }

    public EpochEndOffset(long endOffset) {
        this.error = Errors.NONE;
        this.endOffset = endOffset;
    }

    public Errors error() {
        return error;
    }

    public boolean hasError() {
        return error != Errors.NONE;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "EpochEndOffset{" +
                "error=" + error +
                ", endOffset=" + endOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochEndOffset that = (EpochEndOffset) o;

        if (error != that.error) return false;
        return endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        int result = (int) error.code();
        result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
        return result;
    }
}
