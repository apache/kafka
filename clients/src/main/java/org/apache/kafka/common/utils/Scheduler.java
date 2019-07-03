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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An interface for scheduling tasks for the future.
 *
 * Implementations of this class should be thread-safe.
 */
public interface Scheduler {
    Scheduler SYSTEM = new SystemScheduler();

    /**
     * Get the timekeeper associated with this scheduler.
     */
    Time time();

    /**
     * Schedule a callable to be executed in the future on a
     * ScheduledExecutorService.  Note that the Callable may not be queued on
     * the executor until the designated time arrives.
     *
     * @param executor      The executor to use.
     * @param callable      The callable to execute.
     * @param delayMs       The delay to use, in milliseconds.
     * @param <T>           The return type of the callable.
     * @return              A future which will complete when the callable is finished.
     */
    <T> Future<T> schedule(final ScheduledExecutorService executor,
                           final Callable<T> callable, long delayMs);
}
