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

package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class DelayedFuturePurgatory {
    private final DelayedOperationPurgatory<DelayedFuture<?>> purgatory;
    private final ThreadPoolExecutor executor;
    private final DelayedOperationKey purgatoryKey;

    public DelayedFuturePurgatory(String purgatoryName, int brokerId) {
        this.purgatory = new DelayedOperationPurgatory<>(purgatoryName, brokerId);
        this.executor = new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            r -> new KafkaThread("DelayedExecutor-" + purgatoryName, r, true));
        this.purgatoryKey = () -> "delayed-future-key";
    }

    public <T> DelayedFuture<T> tryCompleteElseWatch(
        long timeoutMs,
        List<CompletableFuture<T>> futures,
        Runnable responseCallback
    ) {
        DelayedFuture<T> delayedFuture = new DelayedFuture<>(timeoutMs, futures, responseCallback);
        boolean done = purgatory.tryCompleteElseWatch(delayedFuture, List.of(purgatoryKey));
        if (!done) {
            BiConsumer<Void, Throwable> callbackAction = (result, exception) -> delayedFuture.forceComplete();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).whenCompleteAsync(callbackAction, executor);
        }
        return delayedFuture;
    }

    public void shutdown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        purgatory.shutdown();
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }
}
