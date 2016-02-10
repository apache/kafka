/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.util.ConnectorTaskId;

public class TaskStatus extends AbstractStatus {

    public TaskStatus(State state, String msg, String workerUrl, int generation) {
        super(state, msg, workerUrl, generation);
    }

    public TaskStatus(State state, String workerUrl, int generation) {
        super(state, null, workerUrl, generation);
    }

    public interface Listener {

        /**
         * Invoked after successful startup of the task.
         * @param id The id of the task
         */
        void onStartup(ConnectorTaskId id);

        /**
         * Invoked if the task raises an error. No shutdown event will follow.
         * @param id The id of the task
         * @param t The error raised by the task.
         */
        void onFailure(ConnectorTaskId id, Throwable t);

        /**
         * Invoked after successful shutdown of the task.
         * @param id The id of the task
         */
        void onShutdown(ConnectorTaskId id);

    }
}
