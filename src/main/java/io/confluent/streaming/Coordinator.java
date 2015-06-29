/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.streaming;

/**
 * Coordinators are provided to the process methods of {@link StreamProcessor} implementations to allow
 * the user code to request actions from the framework.
 * <p>
 * This interface may evolve over time.
 * </p>
 */
public interface Coordinator {

    /**
     * Requests that the stream processor flush all it's state as well as any buffered output and commit the offsets.
     *
     * <p>
     * If <code>CURRENT_TASK</code> is given, a checkpoint is only written for the current task. If
     * <code>ALL_TASKS_IN_CONTAINER</code> is given, a checkpoint is written for all tasks in the current container.
     *
     * <p>
     * Note that if you also have also configured your job to commit in regular intervals (using the
     * <code>task.commit.ms</code> property), those time-based commits are not affected by calling this method. Any
     * commits you request explicitly are in addition to timer-based commits. You can set <code>task.commit.ms=-1</code>
     * if you don't want commits to happen automatically.
     *
     * @param scope Which tasks are being asked to commit.
     */
    public void commit(RequestScope scope);

    /**
     * Requests that the container should be shut down.
     *
     * <p>
     * If <code>CURRENT_TASK</code> is given, that indicates a willingness of the current task to shut down. All tasks
     * in the container (including the one that requested shutdown) will continue processing messages. Only when every
     * task in the container has called <code>shutdown(CURRENT_TASK)</code>, the container is shut down. Once a task has
     * called <code>shutdown(CURRENT_TASK)</code>, it cannot change its mind (i.e. it cannot revoke its willingness to
     * shut down).
     *
     * <p>
     * If <code>ALL_TASKS_IN_CONTAINER</code> is given, the container will shut down immediately after it has finished
     * processing the current message. Any buffers of pending writes are flushed, but no further messages will be
     * processed in this container.
     *
     * @param scope The approach we should use for shutting down the container.
     */
    public void shutdown(RequestScope scope);

    /**
     * A task can make requests to the Samza framework while processing messages, such as
     * {@link Coordinator#commit(RequestScope)} and {@link Coordinator#shutdown(RequestScope)}. This enum is used to
     * indicate whether those requests apply only to the current task, or to all tasks in the current container.
     */
    public enum RequestScope {
        /**
         * Indicates that a request applies only to the task making the call.
         */
        CURRENT_TASK,

        /**
         * Indicates that a request applies to all tasks in the current container.
         */
        ALL_TASKS_IN_CONTAINER;
    }
}
