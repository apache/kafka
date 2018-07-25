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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import java.io.Closeable;
import java.time.Duration;

/**
 * A class which is used during rebalance that is strictly for the usage of the mode "enable.parallel.rebalance"
 * It implements Runnable so that it can be run in concurrency with the old KafkaConsumer (which does not implement
 * Runnable or extends Thread: the user spawns the process, while Kafka internals spawns this one.)
 */
public class RebalanceKafkaConsumer<K, V> extends KafkaConsumer implements Runnable, Closeable {
    // these offset ranges needs to be checkpointed somewhere, right now, this is not tenable
    private final Map<TopicPartition, ArrayList<OffsetInterval>> offsetRanges;
    private final Set<TopicPartition> assignedPartitions;
    private volatile ConsumerRequest request;
    private RequestResult result;
    private volatile Duration waitTime;
    private volatile Object inputArgument;
    private volatile Map<TopicPartition, OffsetAndMetadata> offsets;
    private volatile long hashCode1;
    private volatile long hashCode2;
    private volatile TaskCompletionCallback callback;
    private final AtomicBoolean shouldClose;
    private Map<TopicPartition, OffsetInterval> localRangeTokens;

    public RebalanceKafkaConsumer(final Map<String, Object> configs,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer,
                                  final Map<TopicPartition, OffsetAndMetadata> startOffsets,
                                  final Map<TopicPartition, Long> endOffsets) {
        super(configs, keyDeserializer, valueDeserializer);
        this.offsetRanges = new HashMap<>();
        this.assignedPartitions = new HashSet<>();
        addNewOffsets(startOffsets, endOffsets);
        this.request = null;
        this.result = null;
        this.waitTime = null;
        this.inputArgument = null;
        this.offsets = null;
        this.hashCode1 = 0;
        this.hashCode2 = 0;
        this.callback = null;
        this.shouldClose = new AtomicBoolean(false);
    }

    public void setNewQueue(PriorityBlockingQueue<ConsumerCoordinator.OffsetCommitCompletion> queue) {
        coordinator.setNewQueue(queue);
    }

    public RequestResult getResult() {
        return result;
    }

    public void sendRequest(Duration waitTime,
                            ConsumerRequest request,
                            Object inputArgument,
                            TaskCompletionCallback callback) {
        this.request = request;
        this.waitTime = waitTime;
        this.inputArgument = inputArgument;
        this.callback = callback;
    }

    public void setOptionalInputArgument(Map<TopicPartition, OffsetAndMetadata> metadata,
                                         final long hashCode1,
                                         final long hashCode2) {
        this.offsets = metadata;
        this.hashCode1 = hashCode1;
        this.hashCode2 = hashCode2;
    }

    public void addNewOffsets(Map<TopicPartition, OffsetAndMetadata> startOffsets,
                              Map<TopicPartition, Long> endOffsets) {
        final Set<TopicPartition> newPartitions = new HashSet<>();
        final HashMap<TopicPartition, OffsetInterval> rangeTokens = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : startOffsets.entrySet()) {
            if (entry.getValue().offset() == endOffsets.get(entry.getKey())) {
                continue;
            }
            final OffsetInterval offsetInterval = new OffsetInterval(entry.getValue().offset(),
                    endOffsets.get(entry.getKey()));
            if (!offsetRanges.containsKey(entry.getKey())) {
                assignedPartitions.add(entry.getKey());
                newPartitions.add(entry.getKey());
                offsetRanges.put(entry.getKey(), new ArrayList<>());
            }
            rangeTokens.put(entry.getKey(), offsetInterval);
            offsetRanges.get(entry.getKey()).add(offsetInterval);
        }
        super.assign(assignedPartitions);
        // go to assigned positions i.e. last committed offset
        for (TopicPartition partition : newPartitions) {
            final long pos = offsetRanges.get(partition).get(0).startOffset;
            super.seek(partition, pos <= -1 ? 0 : pos);
        }
        localRangeTokens = rangeTokens;
    }

    private Set<TopicPartition> findUnfinished() {
        final HashSet<TopicPartition> stillUnfinished = new HashSet<>();
        final HashSet<TopicPartition> finishedPartitions = new HashSet<>();
        for (final Map.Entry<TopicPartition, ArrayList<OffsetInterval>> entry : offsetRanges.entrySet()) {
            final long position = super.position(entry.getKey());
            while (!entry.getValue().isEmpty() && position >= entry.getValue().get(0).endOffset) {
                entry.getValue().remove(0);
            }
            if (!entry.getValue().isEmpty()) {
                stillUnfinished.add(entry.getKey());
                final long startPos = entry.getValue().get(0).startOffset;
                if (position < startPos) {
                    super.seek(entry.getKey(), startPos);
                }
            } else {
                finishedPartitions.add(entry.getKey());
            }
        }
        assignedPartitions.removeAll(finishedPartitions);
        super.assign(assignedPartitions);
        for (TopicPartition partition : finishedPartitions) {
            offsetRanges.remove(partition);
        }
        return stillUnfinished;
    }

    public boolean terminated() {
        return findUnfinished().size() == 0;
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (localRangeTokens != null) {
            //do something to get it checkpointed
        }
        if (!terminated()) {
            return super.poll(timeout.toMillis(), true, false);
        }
        return ConsumerRecords.empty();
    }

    @Override
    public void run() {
        while (!shouldClose.get()) {
            if (request == null) {
                continue;
            }

            //sleep for a little to confirm that last result went through
            try {
                Thread.sleep(10);
            } catch (InterruptedException exc) { }

            // Cases which have no return value will have their result be marked as a boolean value: true.
            // This is intended as a marker to represent that a particular operation has succeeded or has finished.
            switch (request) {
                case RESUME:
                    super.resume((Collection<TopicPartition>) inputArgument, true);
                    result = new RequestResult<>(true);
                    break;
                case END_OFFSETS:
                    result = new RequestResult<>(super.endOffsets((Collection<TopicPartition>) inputArgument, waitTime, true));
                    break;
                case BEGINNING_OFFSETS:
                    result = new RequestResult<>(super.beginningOffsets((Collection<TopicPartition>) inputArgument, waitTime, true));
                    break;
                case WAKE_UP:
                    super.wakeup(true);
                    result = new RequestResult<>(true);
                    break;
                case CLOSE:
                    super.close(waitTime);
                    result = new RequestResult<>(true);
                    break;
                case PAUSE:
                    super.pause((Collection<TopicPartition>) inputArgument, true);
                    result = new RequestResult<>(true);
                    break;
                case COMMIT_ASYNC:
                    super.commitAsyncWithHashCodes(offsets,
                            (OffsetCommitCallback) inputArgument,
                            hashCode1,
                            hashCode2);
                    result = new RequestResult<>(true);
                    break;
                case COMMIT_SYNC:
                    super.commitSync((Map<TopicPartition, OffsetAndMetadata>) inputArgument, waitTime, true);
                    result = new RequestResult<>(true);
                    break;
                case COMMITTED:
                    result = new RequestResult<>(super.committed((TopicPartition) inputArgument, waitTime, true));
                    break;
                case POLL:
                    ConsumerRecords<K, V> records = poll(waitTime);
                    result = new RequestResult<>(records);
                    break;
                default:
                    continue;
            }
            if (callback != null) {
                callback.onTaskComplete(result);
            }
            request = null;
        }
    }

    public void close(Duration timeout, TaskCompletionCallback callback) {
        sendRequest(timeout, ConsumerRequest.CLOSE, null, callback);
        this.shouldClose.set(true);
    }

    public enum ConsumerRequest {
        POLL,
        COMMITTED,
        COMMIT_SYNC,
        COMMIT_ASYNC,
        PAUSE,
        RESUME,
        CLOSE,
        WAKE_UP,
        BEGINNING_OFFSETS,
        END_OFFSETS }

    public class RequestResult<T> {
        public final T value;

        RequestResult(T value) {
            this.value = value;
        }
    }

    private static class OffsetInterval implements Serializable {
        public final long startOffset;
        public final long endOffset;

        public OffsetInterval(final long startOffset,
                              final long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
    }

    public static class OffsetInclusion {
        private final HashMap<TopicPartition, OffsetAndMetadata> parentConsumerMetadata;
        private final HashMap<TopicPartition, OffsetAndMetadata> childConsumerMetadata;

        public OffsetInclusion(final HashMap<TopicPartition, OffsetAndMetadata> parentConsumerMetadata,
                               final HashMap<TopicPartition, OffsetAndMetadata> childConsumerMetadata) {
            this.parentConsumerMetadata = parentConsumerMetadata;
            this.childConsumerMetadata = childConsumerMetadata;
        }

        public HashMap<TopicPartition, OffsetAndMetadata> getParentConsumerMetadata() {
            return parentConsumerMetadata;
        }

        public HashMap<TopicPartition, OffsetAndMetadata> getChildConsumerMetadata() {
            return childConsumerMetadata;
        }
    }

    public static OffsetInclusion getRanges(final Map<TopicPartition, OffsetAndMetadata> consumedParentMetadata,
                                            final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        final HashMap<TopicPartition, OffsetAndMetadata> parentConsumerMetadata = new HashMap<>();
        final HashMap<TopicPartition, OffsetAndMetadata> childConsumerMetadata = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsToCommit.entrySet()) {
            if (consumedParentMetadata.containsKey(entry.getKey()) && consumedParentMetadata.get(entry.getKey()).equals(entry.getValue())) {
                parentConsumerMetadata.put(entry.getKey(), entry.getValue());
            } else {
                childConsumerMetadata.put(entry.getKey(), entry.getValue());
            }
        }
        return new OffsetInclusion(parentConsumerMetadata, childConsumerMetadata);
    }

    private class OffsetRangeToken extends OffsetAndMetadata {
        private final OffsetInterval range;

        public OffsetRangeToken(final OffsetInterval range, OffsetAndMetadata metadata) {
            super(metadata.offset(), metadata.metadata());
            this.range = range;
        }

        public OffsetInterval getRange() {
            return range;
        }

        @Override
        public String toString() {
            return "OffsetRangeToken{" +
                    "offset=" + offset() +
                    ", metadata=" + metadata() +
                    ", range=(" + range.startOffset + ", " + range.endOffset + ")" +
                    '}';
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }
    }
}
