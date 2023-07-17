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

import com.yammer.metrics.core.Gauge;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteStorageThreadPool extends ThreadPoolExecutor {
    public static final String TASK_QUEUE_SIZE = "TaskQueueSize";
    public static final String AVG_IDLE_PERCENT = "AvgIdlePercent";
    public static final Set<String> METRIC_SUFFIXES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(TASK_QUEUE_SIZE, AVG_IDLE_PERCENT)));
    private final Logger logger;
    private final String metricsNamePrefix;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    public RemoteStorageThreadPool(String threadNamePrefix,
                                   int numThreads,
                                   int maxPendingTasks,
                                   String metricsNamePrefix) {
        super(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(maxPendingTasks),
                new RemoteStorageThreadFactory(threadNamePrefix));
        logger = new LogContext() {
            @Override
            public String logPrefix() {
                return "[" + Thread.currentThread().getName() + "]";
            }
        }.logger(RemoteStorageThreadPool.class);

        this.metricsNamePrefix = metricsNamePrefix;
        metricsGroup.newGauge(metricsNamePrefix.concat(TASK_QUEUE_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return RemoteStorageThreadPool.this.getQueue().size();
            }
        });
        metricsGroup.newGauge(metricsNamePrefix.concat(AVG_IDLE_PERCENT), new Gauge<Double>() {
            @Override
            public Double value() {
                return 1 - (double) RemoteStorageThreadPool.this.getActiveCount() / (double) RemoteStorageThreadPool.this.getCorePoolSize();
            }
        });
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable th) {
        if (th != null) {
            if (th instanceof FatalExitError) {
                logger.error("Stopping the server as it encountered a fatal error.");
                Exit.exit(((FatalExitError) th).statusCode());
            } else {
                if (!isShutdown())
                    logger.error("Error occurred while executing task: {}", runnable, th);
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
        METRIC_SUFFIXES.forEach(metric -> metricsGroup.removeMetric(metricsNamePrefix.concat(metric)));
    }
}
