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
package org.apache.kafka.streams.errors;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.processor.TaskId;


/**
 * Indicates that a task failed to make progress within the allotted timeout.
 * @see org.apache.kafka.streams.StreamsConfig#TASK_TIMEOUT_MS_CONFIG
 */
public class TaskTimeoutException extends StreamsException {

    private static final long serialVersionUID = 1L;

    public TaskTimeoutException(final TaskId taskId) {
        super(new TimeoutException(), taskId);
    }

    public TaskTimeoutException(final TaskId taskId, final Throwable cause) {
        super(new TimeoutException(cause), taskId);
    }

    public TaskTimeoutException(final String message, final TaskId taskId) {
        super(new TimeoutException(message), taskId);
    }

    public TaskTimeoutException(final String message, final Throwable cause, final TaskId taskId) {
        super(new TimeoutException(message, cause), taskId);
    }

}
