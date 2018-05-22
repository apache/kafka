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

/**
 * {@link AbstractState} provides the current status along with identifier for Connect worker and
 * tasks
 */
public abstract class AbstractState {

    private final String state;
    private final String trace;
    private final String workerId;

    /**
     *
     * @param state - the status of connector or task. Can't be NULL or EMPTY.
     * @param workerId - the workerId associated with the connector or the task.Can't be NULL or EMPTY.
     * @param trace - any error trace associated with the connector or the task. Can be NULL or
     *              EMPTY
     */
    public AbstractState(String state, String workerId, String trace) {
        assert state != null && !state.isEmpty();
        assert workerId != null && !workerId.isEmpty();
        this.state = state;
        this.workerId = workerId;
        this.trace = trace;
    }

    /**
     * Return the state of the connector or task as a String. Returned string will not ne NULL or
     * EMPTY
     * @return state of the connector or task as a String.
     */
    public String state() {
        return state;
    }

    /**
     * The Id of the worker associated with the connector or the task. Returned string will not ne
     * NULL or
     * EMPTY
     * @return workerId
     */
    public String workerId() {
        return workerId;
    }

    /**
     * The error message associated with the connector or task. It can be NULL or EMPTY.
     * @return trace
     */
    public String trace() {
        return trace;
    }
}
