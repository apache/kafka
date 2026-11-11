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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * The default share group DLQ manager responsible for processing
 * incoming messages and writing them to the appropriate dlq topic.
 */
public class DefaultShareGroupDLQManager implements ShareGroupDLQManager {
    /**
     * Reference to state manager responsible for actually sending
     * the relevant RPCs and writing records.
     */
    private final ShareGroupDLQStateManager stateManager;

    /**
     * Executor on which the state manager reads the source records of DLQ requests from the log, so
     * that the read never blocks the thread which submitted the request. Owned by this manager.
     */
    private final ExecutorService fetchExecutor;

    private static final long FETCH_EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5_000L;

    private static final Logger log = LoggerFactory.getLogger(DefaultShareGroupDLQManager.class);

    public static ShareGroupDLQManager instance(
        KafkaClient client,
        ShareGroupDLQMetadataCacheHelper cacheHelper,
        Time time,
        Timer timer,
        ShareGroupMetrics shareGroupMetrics,
        IntSupplier shareGroupDLQManagerFetcherNumThreads,
        LogReader logReader
    ) {
        DefaultShareGroupDLQManager instance = new DefaultShareGroupDLQManager(client, cacheHelper, time, timer, shareGroupMetrics, shareGroupDLQManagerFetcherNumThreads, logReader);
        instance.start();
        return instance;
    }

    private DefaultShareGroupDLQManager(
        KafkaClient client,
        ShareGroupDLQMetadataCacheHelper cacheHelper,
        Time time,
        Timer timer,
        ShareGroupMetrics shareGroupMetrics,
        IntSupplier shareGroupDLQManagerFetcherNumThreads,
        LogReader logReader
    ) {
        AtomicInteger threadId = new AtomicInteger();
        fetchExecutor = Executors.newFixedThreadPool(
            shareGroupDLQManagerFetcherNumThreads.getAsInt(),
            runnable -> KafkaThread.daemon("share-group-dlq-record-fetcher-" + threadId.getAndIncrement(), runnable));
        stateManager = new ShareGroupDLQStateManager(client, cacheHelper, time, timer, shareGroupMetrics, logReader, fetchExecutor);
    }

    private void start() {
        stateManager.start();
    }

    // Visibility for tests
    ExecutorService fetchExecutor() {
        return fetchExecutor;
    }

    @Override
    public CompletableFuture<Void> enqueue(ShareGroupDLQRecordParameter param) {
        try {
            ShareGroupDLQValidator.validateParam(param);
            return stateManager.dlq(param);
        } catch (Exception e) {
            log.error("Unable to enqueue DLQ request", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void stop() {
        // Drain the reads first - they enqueue onto the state manager when they finish. Queued reads
        // still run after shutdown(). shutdownNow() is deliberately not used: it would interrupt a
        // thread which may be reading a log segment, and an interrupt during a FileChannel operation
        // closes the channel for every other user of that segment. A read which does not finish in
        // time is abandoned and left to complete on its daemon thread. The futures of requests that
        // never complete are not awaited by anything on the shutdown path.
        fetchExecutor.shutdown();
        try {
            if (!fetchExecutor.awaitTermination(FETCH_EXECUTOR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                log.warn("DLQ fetch executor did not finish its pending reads in {} ms.", FETCH_EXECUTOR_SHUTDOWN_TIMEOUT_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Utils.swallow(log, Level.ERROR, "Unable to stop DLQ state manager", stateManager::stop);
    }
}
