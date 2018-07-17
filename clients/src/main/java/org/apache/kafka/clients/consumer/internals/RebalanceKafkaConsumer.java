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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
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
    private volatile TaskCompletionCallback callback;
    private final AtomicBoolean shouldClose;

    public RebalanceKafkaConsumer(final Map<String, Object> configs,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer,
                                  final Map<TopicPartition, Long> startOffsets,
                                  final Map<TopicPartition, Long> endOffsets) {
        super(configs, keyDeserializer, valueDeserializer);
        this.offsetRanges = new HashMap<>();
        this.assignedPartitions = new HashSet<>();
        addNewOffsets(startOffsets, endOffsets);
        this.request = null;
        this.result = null;
        this.waitTime = null;
        this.inputArgument = null;
        this.callback = null;
        this.shouldClose = new AtomicBoolean(false);
    }

    public Map<TopicPartition, ArrayList<OffsetInterval>> getOffsetRanges() {
        return offsetRanges;
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

    public void addNewOffsets(Map<TopicPartition, Long> startOffsets,
                              Map<TopicPartition, Long> endOffsets) {
        final Set<TopicPartition> newPartitions = new HashSet<>();
        for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
            if (entry.getValue().equals(endOffsets.get(entry.getKey()))) {
                continue;
            }
            if (!offsetRanges.containsKey(entry.getKey())) {
                assignedPartitions.add(entry.getKey());
                newPartitions.add(entry.getKey());
                offsetRanges.put(entry.getKey(), new ArrayList<>());
            }
            offsetRanges.get(entry.getKey()).add(new OffsetInterval(entry.getValue(), endOffsets.get(entry.getKey())));
        }
        super.assign(assignedPartitions);
        // go to assigned positions i.e. last committed offset
        for (TopicPartition partition : newPartitions) {
            super.seek(partition, offsetRanges.get(partition).get(0).startOffset);
        }
    }

    private Set<TopicPartition> findUnfinished() {
        final HashSet<TopicPartition> stillUnfinished = new HashSet<>();
        final HashSet<TopicPartition> finishedPartitions = new HashSet<>();
        for (final Map.Entry<TopicPartition, ArrayList<OffsetInterval>> entry : offsetRanges.entrySet()) {
            if (super.position(entry.getKey()) >= entry.getValue().get(0).endOffset) {
                if (entry.getValue().size() > 1) {
                    entry.getValue().remove(0);
                    super.seek(entry.getKey(), entry.getValue().get(0).startOffset);
                    stillUnfinished.add(entry.getKey());
                } else {
                    finishedPartitions.add(entry.getKey());
                }
            } else {
                stillUnfinished.add(entry.getKey());
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
        if (!terminated()) {
            return super.poll(timeout.toMillis(), true);
        }
        return null;
    }

    @Override
    public void run() {
        while (!shouldClose.get()) {
            if (request == null) {
                continue;
            }

            // Cases which have no return value will have their result be marked as a boolean value: true.
            // This is intended as a marker to represent that a particular operation has succeeded or has finished.
            switch (request) {
                case WAKE_UP:
                    super.wakeup();
                    result = new RequestResult<>(true);
                    break;
                case CLOSE:
                    super.close(waitTime);
                    result = new RequestResult<>(true);
                    break;
                case PAUSE:
                    pause((Collection<TopicPartition>) inputArgument);
                    result = new RequestResult<>(true);
                    break;
                case COMMIT_ASYNC:
                    super.commitAsync((OffsetCommitCallback) inputArgument);
                    result = new RequestResult<>(true);
                    break;
                case COMMIT_SYNC:
                    super.commitSync(waitTime);
                    result = new RequestResult<>(true);
                    break;
                case COMMITTED:
                    result = new RequestResult<>(super.committed((TopicPartition) inputArgument));
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
        WAKE_UP }

    public class RequestResult<T> {
        public final T value;

        RequestResult(T value) {
            this.value = value;
        }
    }

    private static class OffsetInterval {
        public final long startOffset;
        public final long endOffset;

        public OffsetInterval(final long startOffset,
                              final long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public boolean containsOffset(final long offset) {
            return offset >= startOffset && offset <= endOffset;
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

    public static OffsetInclusion getRanges(final RebalanceKafkaConsumer consumer, final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        final HashMap<TopicPartition, OffsetAndMetadata> parentConsumerMetadata = new HashMap<>();
        final HashMap<TopicPartition, OffsetAndMetadata> childConsumerMetadata = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsToCommit.entrySet()) {
            if (consumer.getOffsetRanges().containsKey(entry.getKey())) {
                boolean added = false;
                for (final OffsetInterval interval : (ArrayList<OffsetInterval>) consumer.getOffsetRanges().get(entry.getKey())) {
                    if (interval.containsOffset(entry.getValue().offset())) {
                        added = true;
                        childConsumerMetadata.put(entry.getKey(), entry.getValue());
                        break;
                    }
                }
                if (!added) {
                    parentConsumerMetadata.put(entry.getKey(), entry.getValue());
                }
            } else {
                parentConsumerMetadata.put(entry.getKey(), entry.getValue());
            }
        }
        return new OffsetInclusion(parentConsumerMetadata, childConsumerMetadata);
    }
}
