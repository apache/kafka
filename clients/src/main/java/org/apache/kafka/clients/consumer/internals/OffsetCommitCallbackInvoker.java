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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Utility class that helps the application thread to invoke user registered {@link OffsetCommitCallback} amd
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}s. This is
 * achieved by having the background thread register a {@link OffsetCommitCallbackTask} to the invoker upon the
 * future completion, and execute the callbacks when user polls/commits/closes the consumer.
 */
public class OffsetCommitCallbackInvoker {
    private final ConsumerInterceptors<?, ?> interceptors;

    OffsetCommitCallbackInvoker(ConsumerInterceptors<?, ?> interceptors) {
        this.interceptors = interceptors;
    }

    // Thread-safe queue to store user-defined callbacks and interceptors to be executed
    private final BlockingQueue<OffsetCommitCallbackTask> callbackQueue = new LinkedBlockingQueue<>();

    public void enqueueInterceptorInvocation(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!interceptors.isEmpty()) {
            callbackQueue.add(new OffsetCommitCallbackTask(
                (offsetsParam, exception) -> interceptors.onCommit(offsetsParam),
                offsets,
                null
            ));
        }
    }

    public void enqueueUserCallbackInvocation(final OffsetCommitCallback callback,
                                              final Map<TopicPartition, OffsetAndMetadata> offsets,
                                              final Exception exception) {
        callbackQueue.add(new OffsetCommitCallbackTask(callback, offsets, exception));
    }

    public void executeCallbacks() {
        while (!callbackQueue.isEmpty()) {
            OffsetCommitCallbackTask task = callbackQueue.poll();
            if (task != null) {
                task.callback.onComplete(task.offsets, task.exception);
            }
        }
    }

    private static class OffsetCommitCallbackTask {
        public final Map<TopicPartition, OffsetAndMetadata> offsets;
        public final Exception exception;
        public final OffsetCommitCallback callback;

        public OffsetCommitCallbackTask(final OffsetCommitCallback callback,
                                        final Map<TopicPartition, OffsetAndMetadata> offsets,
                                        final Exception exception) {
            this.offsets = offsets;
            this.exception = exception;
            this.callback = callback;
        }
    }
}