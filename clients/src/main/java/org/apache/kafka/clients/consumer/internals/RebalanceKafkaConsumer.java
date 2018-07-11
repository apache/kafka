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
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * A class which is used during rebalance that is strictly for the usage of the mode "enable.parallel.rebalance"
 * It implements Runnable so that it can be run in concurrency with the old KafkaConsumer (which does not implement
 * Runnable or extends Thread: the user spawns the process, while Kafka internals spawns this one.)
 */
public class RebalanceKafkaConsumer<K, V> extends KafkaConsumer implements Runnable, Closeable {
    private final Map<TopicPartition, Long> endOffsets;
    private final Set<TopicPartition> unfinished;
    private volatile ConsumerRequest request;
    private RequestResult result;
    private volatile Duration waitTime;
    private volatile InputArgument inputArgument;
    private volatile TaskCompletionCallback callback;
    private volatile boolean shouldClose;

    public RebalanceKafkaConsumer(final Map<String, Object> configs,
                                  final Map<TopicPartition, Long> startOffsets,
                                  final Map<TopicPartition, Long> endOffsets) {
        super(configs, null, null);
        this.endOffsets = endOffsets;
        this.unfinished = new HashSet<>(startOffsets.keySet());
        //go to start positions i.e. the last committed offsets of the parent consumer
        for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
            super.seek(entry.getKey(), entry.getValue());
        }
        this.request = null;
        this.result = null;
        this.waitTime = null;
        this.inputArgument = null;
        this.callback = null;
        this.shouldClose = false;
    }

    public RequestResult getResult() {
        return result;
    }

    public void sendRequest(Duration waitTime,
                            ConsumerRequest request,
                            InputArgument inputArgument,
                            TaskCompletionCallback callback) {
        this.request = request;
        this.waitTime = waitTime;
        this.inputArgument = inputArgument;
        this.callback = callback;
    }

    public Set<TopicPartition> unfinishedPartitions() {
        return unfinished;
    }

    private Set<TopicPartition> findUnfinished() {
        final HashSet<TopicPartition> stillUnfinished = new HashSet<>();
        for (TopicPartition partition : unfinished) {
            if (position(partition) == endOffsets.get(partition)) {
                continue;
            }
            stillUnfinished.add(partition);
        }
        unfinished.retainAll(stillUnfinished);
        return unfinished;
    }

    public boolean terminated() {
        return findUnfinished().size() == 0;
    }

    public ConsumerRecords<K, V> storedRecords() {
        return offsetBuffer.pollOffsets();
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (!terminated()) {
            return super.poll(timeout);
        }
        return null;
    }

    @Override
    public void run() {
        while (!shouldClose) {
            switch (request) {
                case PAUSE:
                    pause((Collection<TopicPartition>) inputArgument.value);
                    result = null;
                    break;
                case OFFSETS_FOR_TIMES:
                    result = new RequestResult<>(super.offsetsForTimes((Map<TopicPartition, Long>) inputArgument.value,
                                                                       waitTime));
                    break;
                case END_OFFSETS:
                    result = new RequestResult<>(super.endOffsets((Collection<TopicPartition>) inputArgument.value,
                                                                  waitTime));
                    break;
                case BEGINNING_OFFSETS:
                    result = new RequestResult<>(super.beginningOffsets((Collection<TopicPartition>) inputArgument.value,
                                                                        waitTime));
                    break;
                case COMMIT_ASYNC:
                    super.commitAsync((OffsetCommitCallback) inputArgument.value);
                    result = null;
                    break;
                case COMMIT_SYNC:
                    super.commitSync(waitTime);
                    result = null;
                    break;
                case COMMITTED:
                    result = new RequestResult<>(super.committed((TopicPartition) inputArgument.value));
                    break;
                case POLL:
                    ConsumerRecords<K, V> records = poll(waitTime);
                    result = new RequestResult<>(records);
                    offsetBuffer.insertOffsets(records);
                    break;
                default:
                    continue;
            }
            callback.onTaskCompete(result);
            request = null;
        }
    }

    @Override
    public void close() {
        this.shouldClose = true;
    }

    public enum ConsumerRequest { POLL,
                                  COMMITTED,
                                  COMMIT_SYNC,
                                  COMMIT_ASYNC,
                                  BEGINNING_OFFSETS,
                                  END_OFFSETS,
                                  OFFSETS_FOR_TIMES,
                                  PAUSE,
                                  RESUME }

    public class RequestResult<T> {
        final T value;

        RequestResult(T value) {
            this.value = value;
        }
    }

    public class InputArgument<T> {
        final T value;

        InputArgument(T value) {
            this.value = value;
        }
    }

    public abstract class TaskCompletionCallback {
        /**
         * Can be defined by methods of KafkaConsumer to suit their purposes.
         */
        public abstract void onTaskCompete(RequestResult result);
    }
}
