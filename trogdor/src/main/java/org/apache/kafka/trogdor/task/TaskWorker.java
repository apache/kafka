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

package org.apache.kafka.trogdor.task;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.trogdor.common.Platform;

/**
 * The agent-side interface for implementing tasks.
 */
public interface TaskWorker {
    /**
     * Starts the TaskWorker.
     *
     * We do not hold any locks or block the WorkerManager state machine on this call.
     * However, createTask requests to the agent call this function directly.
     * Therefore, your start() implementation may take a little while, but not too long.
     * While you can perform short blocking tasks in this function, it is better to
     * start a background thread to do something time-consuming.
     *
     * If the start() function throws an exception, the Agent will assume that the TaskWorker
     * never started.  Therefore, stop() will never be invoked.  On the other hand, if the
     * errorFuture is completed, either by a background task or by the start function itself,
     * the Agent will invoke the stop() method to clean up the worker.
     *
     *
     * @param platform          The platform to use.
     * @param status            The current status.  The TaskWorker can update
     *                          this at any time to provide an updated status.
     * @param haltFuture        A future which the worker should complete if it halts.
     *                          If it is completed with an empty string, that means the task
     *                          halted with no error.  Otherwise, the string is treated as the error.
     *                          If you start a background thread, you may pass haltFuture
     *                          to that thread.  Then, the thread can use this future to indicate
     *                          that the worker should be stopped.
     *
     * @throws Exception        If the TaskWorker failed to start.  stop() will not be invoked.
     */
    void start(Platform platform, WorkerStatusTracker status, KafkaFutureImpl<String> haltFuture)
        throws Exception;

    /**
     * Stops the TaskWorker.
     *
     * A TaskWorker may be stopped because it has run for its assigned duration, or because a
     * request arrived instructing the Agent to stop the worker.  The TaskWorker will
     * also be stopped if errorFuture was completed to indicate that there was an error.
     *
     * Regardless of why the TaskWorker was stopped, the stop() function should release all
     * resources and stop all threads before returning.  The stop() function can block for
     * as long as it wants.  It is run in a background thread which will not block other
     * agent operations.  All tasks will be stopped when the Agent cleanly shuts down.
     *
     * @param platform          The platform to use.
     *
     * @throws Exception        If there was an error cleaning up the TaskWorker.
     *                          If there is no existing TaskWorker error, the worker will be
     *                          treated as having failed with the given error.
     */
    void stop(Platform platform) throws Exception;
}
