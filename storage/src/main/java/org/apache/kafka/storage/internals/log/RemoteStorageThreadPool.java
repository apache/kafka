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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_STORAGE_THREAD_POOL_METRICS;

public final class RemoteStorageThreadPool extends ThreadPoolExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteStorageThreadPool.class);
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    public RemoteStorageThreadPool(String threadNamePrefix,
                                   int numThreads,
                                   int maxPendingTasks) {
        super(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(maxPendingTasks),
                new RemoteStorageThreadFactory(threadNamePrefix));
        metricsGroup.newGauge(REMOTE_LOG_READER_TASK_QUEUE_SIZE_METRIC.getName(),
                () -> getQueue().size());
        metricsGroup.newGauge(REMOTE_LOG_READER_AVG_IDLE_PERCENT_METRIC.getName(),
                () -> 1 - (double) getActiveCount() / (double) getCorePoolSize());
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable th) {
        if (th != null) {
            if (th instanceof FatalExitError) {
                LOGGER.error("Stopping the server as it encountered a fatal error.");
                Exit.exit(((FatalExitError) th).statusCode());
            } else {
                if (!isShutdown())
                    LOGGER.error("Error occurred while executing task: {}", runnable, th);
            }
        }
    }

    private static class RemoteStorageThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        RemoteStorageThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, namePrefix + threadNumber.getAndIncrement());
        }

    }

    public void removeMetrics() {
        REMOTE_STORAGE_THREAD_POOL_METRICS.forEach(metricsGroup::removeMetric);
    }
}
