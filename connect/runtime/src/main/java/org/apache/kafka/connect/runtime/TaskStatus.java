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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.util.ConnectorTaskId;

public class TaskStatus extends AbstractStatus<ConnectorTaskId> {

    public TaskStatus(ConnectorTaskId id, State state, String workerUrl, int generation, String trace) {
        super(id, state, workerUrl, generation, trace);
    }

    public TaskStatus(ConnectorTaskId id, State state, String workerUrl, int generation) {
        super(id, state, workerUrl, generation, null);
    }

    public interface Listener {

        /**
         * Invoked after successful startup of the task.
         * @param id The id of the task
         */
        void onStartup(ConnectorTaskId id);

        /**
         * Invoked after the task has been paused.
         * @param id The id of the task
         */
        void onPause(ConnectorTaskId id);

        /**
         * Invoked after the task has been resumed.
         * @param id The id of the task
         */
        void onResume(ConnectorTaskId id);

        /**
         * Invoked if the task raises an error. No shutdown event will follow.
         * @param id The id of the task
         * @param cause The error raised by the task.
         */
        void onFailure(ConnectorTaskId id, Throwable cause);

        /**
         * Invoked after successful shutdown of the task.
         * @param id The id of the task
         */
        void onShutdown(ConnectorTaskId id);

        /**
         * Invoked after the task has been deleted. Can be called if the
         * connector tasks have been reduced, or if the connector itself has
         * been deleted.
         * @param id The id of the task
         */
        void onDeletion(ConnectorTaskId id);

        /**
         * Invoked when the task is restarted asynchronously by the herder on processing a restart request.
         * @param id The id of the task
         */
        void onRestart(ConnectorTaskId id);
    }
}
