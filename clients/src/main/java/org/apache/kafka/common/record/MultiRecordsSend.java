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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransferableChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * A set of composite sends with nested {@link RecordsSend}, sent one after another
 */
public class MultiRecordsSend implements Send {
    private static final Logger log = LoggerFactory.getLogger(MultiRecordsSend.class);

    private final Queue<Send> sendQueue;
    private final long size;
    private Map<TopicPartition, RecordConversionStats> recordConversionStats;

    private long totalWritten = 0;
    private Send current;

    /**
     * Construct a MultiRecordsSend from a queue of Send objects. The queue will be consumed as the MultiRecordsSend
     * progresses (on completion, it will be empty).
     */
    public MultiRecordsSend(Queue<Send> sends) {
        this.sendQueue = sends;

        long size = 0;
        for (Send send : sends)
            size += send.size();
        this.size = size;

        this.current = sendQueue.poll();
    }

    public MultiRecordsSend(Queue<Send> sends, long size) {
        this.sendQueue = sends;
        this.size = size;
        this.current = sendQueue.poll();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean completed() {
        return current == null;
    }

    // Visible for testing
    int numResidentSends() {
        int count = 0;
        if (current != null)
            count += 1;
        count += sendQueue.size();
        return count;
    }

    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        if (completed())
            throw new KafkaException("This operation cannot be invoked on a complete request.");

        long totalWrittenPerCall = 0;
        boolean sendComplete;
        do {
            long written = current.writeTo(channel);
            totalWrittenPerCall += written;
            sendComplete = current.completed();
            if (sendComplete) {
                updateRecordConversionStats(current);
                current = sendQueue.poll();
            }
        } while (!completed() && sendComplete);

        totalWritten += totalWrittenPerCall;

        if (completed() && totalWritten != size)
            log.error("mismatch in sending bytes over socket; expected: {} actual: {}", size, totalWritten);

        log.trace("Bytes written as part of multi-send call: {}, total bytes written so far: {}, expected bytes to write: {}",
                totalWrittenPerCall, totalWritten, size);

        return totalWrittenPerCall;
    }

    /**
     * Get any statistics that were recorded as part of executing this {@link MultiRecordsSend}.
     * @return Records processing statistics (could be null if no statistics were collected)
     */
    public Map<TopicPartition, RecordConversionStats> recordConversionStats() {
        return recordConversionStats;
    }

    @Override
    public String toString() {
        return "MultiRecordsSend(" +
            "size=" + size +
            ", totalWritten=" + totalWritten +
            ')';
    }

    private void updateRecordConversionStats(Send completedSend) {
        // The underlying send might have accumulated statistics that need to be recorded. For example,
        // LazyDownConversionRecordsSend accumulates statistics related to the number of bytes down-converted, the amount
        // of temporary memory used for down-conversion, etc. Pull out any such statistics from the underlying send
        // and fold it up appropriately.
        if (completedSend instanceof LazyDownConversionRecordsSend) {
            if (recordConversionStats == null)
                recordConversionStats = new HashMap<>();
            LazyDownConversionRecordsSend lazyRecordsSend = (LazyDownConversionRecordsSend) completedSend;
            recordConversionStats.put(lazyRecordsSend.topicPartition(), lazyRecordsSend.recordConversionStats());
        }
    }
}
