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

package org.apache.kafka.connect.health;

import java.util.Objects;

import org.apache.kafka.common.utils.Utils;

/**
 * Provides the current status along with identifier for Connect worker and tasks.
 */
public abstract class AbstractState {

    private final String state;
    private final String traceMessage;
    private final String workerId;

    /**
     * Construct a state for connector or task.
     *
     * @param state  the status of connector or task; may not be null or empty
     * @param workerId  the workerId associated with the connector or the task; may not be null or empty
     * @param traceMessage  any error trace message associated with the connector or the task; may be null or empty
     */
    public AbstractState(String state, String workerId, String traceMessage) {
        if (Utils.isBlank(state)) {
            throw new IllegalArgumentException("State must not be null or empty");
        }
        if (Utils.isBlank(workerId)) {
            throw new IllegalArgumentException("Worker ID must not be null or empty");
        }
        this.state = state;
        this.workerId = workerId;
        this.traceMessage = traceMessage;
    }

    /**
     * Provides the current state of the connector or task.
     *
     * @return state, never {@code null} or empty
     */
    public String state() {
        return state;
    }

    /**
     * The identifier of the worker associated with the connector or the task.
     *
     * @return workerId, never {@code null} or empty.
     */
    public String workerId() {
        return workerId;
    }

    /**
     * The error message associated with the connector or task.
     *
     * @return traceMessage, can be {@code null} or empty.
     */
    public String traceMessage() {
        return traceMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AbstractState that = (AbstractState) o;
        return state.equals(that.state)
            && Objects.equals(traceMessage, that.traceMessage)
            && workerId.equals(that.workerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, traceMessage, workerId);
    }
}
