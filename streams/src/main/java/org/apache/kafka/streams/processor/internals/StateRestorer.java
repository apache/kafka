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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.internals.RecordConverter;

import java.util.ArrayList;
import java.util.Collection;

public class StateRestorer {

    static final int NO_CHECKPOINT = -1;

    private final long offsetLimit;
    private final boolean persistent;
    private final String storeName;
    private final TopicPartition partition;
    private final CompositeRestoreListener compositeRestoreListener;
    private final RecordConverter recordConverter;

    private long checkpointOffset;
    private long restoredOffset;
    private long startingOffset;
    private long endingOffset;

    StateRestorer(final TopicPartition partition,
                  final CompositeRestoreListener compositeRestoreListener,
                  final Long checkpoint,
                  final long offsetLimit,
                  final boolean persistent,
                  final String storeName,
                  final RecordConverter recordConverter) {
        this.partition = partition;
        this.compositeRestoreListener = compositeRestoreListener;
        this.checkpointOffset = checkpoint == null ? NO_CHECKPOINT : checkpoint;
        this.offsetLimit = offsetLimit;
        this.persistent = persistent;
        this.storeName = storeName;
        this.recordConverter = recordConverter;
    }

    public TopicPartition partition() {
        return partition;
    }

    public String storeName() {
        return storeName;
    }

    long checkpoint() {
        return checkpointOffset;
    }

    void setCheckpointOffset(final long checkpointOffset) {
        this.checkpointOffset = checkpointOffset;
    }

    void restoreStarted() {
        compositeRestoreListener.onRestoreStart(partition, storeName, startingOffset, endingOffset);
    }

    void restoreDone() {
        compositeRestoreListener.onRestoreEnd(partition, storeName, restoredNumRecords());
    }

    void restoreBatchCompleted(final long currentRestoredOffset, final int numRestored) {
        compositeRestoreListener.onBatchRestored(partition, storeName, currentRestoredOffset, numRestored);
    }

    void restore(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        final Collection<ConsumerRecord<byte[], byte[]>> convertedRecords = new ArrayList<>(records.size());
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            convertedRecords.add(recordConverter.convert(record));
        }
        compositeRestoreListener.restoreBatch(convertedRecords);
    }

    boolean isPersistent() {
        return persistent;
    }

    void setUserRestoreListener(final StateRestoreListener userRestoreListener) {
        this.compositeRestoreListener.setUserRestoreListener(userRestoreListener);
    }

    void setRestoredOffset(final long restoredOffset) {
        this.restoredOffset = Math.min(offsetLimit, restoredOffset);
    }

    void setStartingOffset(final long startingOffset) {
        this.startingOffset = Math.min(offsetLimit, startingOffset);
    }

    void setEndingOffset(final long endingOffset) {
        this.endingOffset = Math.min(offsetLimit, endingOffset);
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
