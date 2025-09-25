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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * This class encapsulates the invocation of the callback methods defined in the {@link StreamsRebalanceListener}
 * interface. When streams group task assignment changes, these methods are invoked. This class wraps those
 * callback calls with some logging and error handling.
 */
public class StreamsRebalanceListenerInvoker {

    private final Logger log;

    private final StreamsRebalanceData streamsRebalanceData;
    private Optional<StreamsRebalanceListener> listener;

    StreamsRebalanceListenerInvoker(LogContext logContext, StreamsRebalanceData streamsRebalanceData) {
        this.log = logContext.logger(getClass());
        this.listener = Optional.empty();
        this.streamsRebalanceData = streamsRebalanceData;
    }

    public void setRebalanceListener(StreamsRebalanceListener streamsRebalanceListener) {
        Objects.requireNonNull(streamsRebalanceListener, "StreamsRebalanceListener cannot be null");
        this.listener = Optional.of(streamsRebalanceListener);
    }

    public Exception invokeAllTasksRevoked() {
        if (listener.isEmpty()) {
            return null;
        }
        return invokeTasksRevoked(streamsRebalanceData.reconciledAssignment().activeTasks());
    }

    public Exception invokeTasksAssigned(final StreamsRebalanceData.Assignment assignment) {
        if (listener.isEmpty()) {
            return null;
        }
        log.info("Invoking tasks assigned callback for new assignment: {}", assignment);
        try {
            listener.get().onTasksAssigned(assignment);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error(
                "Streams rebalance listener failed on invocation of onTasksAssigned for tasks {}",
                assignment,
                e
            );
            return e;
        }
        return null;
    }

    public Exception invokeTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks) {
        if (listener.isEmpty()) {
            return null;
        }
        log.info("Invoking task revoked callback for revoked active tasks {}", tasks);
        try {
            listener.get().onTasksRevoked(tasks);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error(
                "Streams rebalance listener failed on invocation of onTasksRevoked for tasks {}",
                tasks,
                e
            );
            return e;
        }
        return null;
    }

    public Exception invokeAllTasksLost() {
        if (listener.isEmpty()) {
            return null;
        }
        log.info("Invoking tasks lost callback for all tasks");
        try {
            listener.get().onAllTasksLost();
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error(
                "Streams rebalance listener failed on invocation of onTasksLost.",
                e
            );
            return e;
        }
        return null;
    }
}
