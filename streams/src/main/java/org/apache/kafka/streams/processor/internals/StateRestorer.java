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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreCallback;

public class StateRestorer {
    static final int NO_CHECKPOINT = -1;

    private final long offsetLimit;
    private final boolean persistent;
    private final String storeName;
    private final TopicPartition partition;
    private final StateRestoreCallback stateRestoreCallback;

    private long checkpointOffset;
    private long restoredOffset;
    private long startingOffset;

    StateRestorer(final TopicPartition partition,
                  final StateRestoreCallback stateRestoreCallback,
                  final Long checkpoint,
                  final long offsetLimit,
                  final boolean persistent,
                  final String storeName) {
        this.partition = partition;
        this.stateRestoreCallback = stateRestoreCallback;
        this.checkpointOffset = checkpoint == null ? NO_CHECKPOINT : checkpoint;
        this.offsetLimit = offsetLimit;
        this.persistent = persistent;
        this.storeName = storeName;
    }

    public TopicPartition partition() {
        return partition;
    }

    long checkpoint() {
        return checkpointOffset;
    }

    void setCheckpointOffset(final long checkpointOffset) {
        this.checkpointOffset = checkpointOffset;
    }

    public String storeName() {
        return storeName;
    }

    void restore(final byte[] key, final byte[] value) {
        stateRestoreCallback.restore(key, value);
    }

    boolean isPersistent() {
        return persistent;
    }

    void setRestoredOffset(final long restoredOffset) {
        this.restoredOffset = Math.min(offsetLimit, restoredOffset);
    }

    void setStartingOffset(final long startingOffset) {
        this.startingOffset = Math.min(offsetLimit, startingOffset);
    }

    long startingOffset() {
        return startingOffset;
    }

    boolean hasCompleted(final long recordOffset, final long endOffset) {
        return endOffset == 0 || recordOffset >= readTo(endOffset);
    }

    Long restoredOffset() {
        return restoredOffset;
    }

    long restoredNumRecords() {
        return restoredOffset - startingOffset;
    }

    long offsetLimit() {
        return offsetLimit;
    }

    private Long readTo(final long endOffset) {
        return endOffset < offsetLimit ? endOffset : offsetLimit;
    }
}
