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
import org.apache.kafka.streams.processor.internals.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TaskTimeoutExceptions extends StreamsException {
    private static final long serialVersionUID = 1L;

    private final TimeoutException timeoutException;
    private final Map<Task, TimeoutException> exceptions;

    public TaskTimeoutExceptions() {
        super("");
        timeoutException = null;
        exceptions = new HashMap<>();
    }

    public TaskTimeoutExceptions(final TimeoutException timeoutException) {
        super("");
        this.timeoutException = timeoutException;
        exceptions = null;
    }

    public void recordException(final Task task,
                                final TimeoutException timeoutException) {
        Objects.requireNonNull(exceptions)
            .put(task, timeoutException);
    }

    public Map<Task, TimeoutException> exceptions() {
        return exceptions;
    }

    public TimeoutException timeoutException() {
        return timeoutException;
    }

}
