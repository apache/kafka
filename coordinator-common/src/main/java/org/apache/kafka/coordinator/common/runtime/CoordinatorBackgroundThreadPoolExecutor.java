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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ThreadPoolExecutor} that reports metrics.
 */
public class CoordinatorBackgroundThreadPoolExecutor extends ThreadPoolExecutor {

    /**
     * The time.
     */
    private final Time time;

    /**
     * The coordinator runtime metrics.
     */
    private final CoordinatorRuntimeMetrics metrics;

    public CoordinatorBackgroundThreadPoolExecutor(
        String threadPrefix,
        int numThreads,
        Time time,
        CoordinatorRuntimeMetrics metrics
    ) {
        super(
            numThreads,
            numThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            ThreadUtils.createThreadFactory(Objects.requireNonNull(threadPrefix) + "%d", false)
        );
        this.time = Objects.requireNonNull(time);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void execute(Runnable command) {
        var queuedTimeMs = time.milliseconds();

        super.execute(() -> {
            var dequeuedTimeMs = time.milliseconds();
            metrics.recordBackgroundQueueTime(dequeuedTimeMs - queuedTimeMs);

            try {
                command.run();
            } finally {
                long processingTimeMs = time.milliseconds() - dequeuedTimeMs;
                metrics.recordBackgroundProcessingTime(processingTimeMs);
                metrics.recordBackgroundThreadBusyTime((double) processingTimeMs / getCorePoolSize());
            }
        });
    }
}
