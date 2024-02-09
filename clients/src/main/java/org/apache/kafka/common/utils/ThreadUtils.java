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

package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utilities for working with threads.
 */
public class ThreadUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);
    /**
     * Create a new ThreadFactory.
     *
     * @param pattern       The pattern to use.  If this contains %d, it will be
     *                      replaced with a thread number.  It should not contain more
     *                      than one %d.
     * @param daemon        True if we want daemon threads.
     * @return              The new ThreadFactory.
     */
    public static ThreadFactory createThreadFactory(final String pattern,
                                                    final boolean daemon) {
        return new ThreadFactory() {
            private final AtomicLong threadEpoch = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                String threadName;
                if (pattern.contains("%d")) {
                    threadName = String.format(pattern, threadEpoch.addAndGet(1));
                } else {
                    threadName = pattern;
                }
                Thread thread = new Thread(r, threadName);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    /**
     * Shuts down an executor service in two phases, first by calling shutdown to reject incoming tasks,
     * and then calling shutdownNow, if necessary, to cancel any lingering tasks.
     * After the timeout/on interrupt, the service is forcefully closed.
     * @param executorService The service to shut down.
     * @param timeout The timeout of the shutdown.
     * @param timeUnit The time unit of the shutdown timeout.
     */
    public static void shutdownExecutorServiceQuietly(ExecutorService executorService,
                                                      long timeout, TimeUnit timeUnit) {
        if (executorService == null) {
            return;
        }
        executorService.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(timeout, timeUnit)) {
                    log.error("Executor {} did not terminate in time", executorService);
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
