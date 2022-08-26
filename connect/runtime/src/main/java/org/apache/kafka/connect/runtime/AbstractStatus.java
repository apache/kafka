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

import java.util.Objects;

public abstract class AbstractStatus<T> {

    public enum State {
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED,
        RESTARTING,
    }

    private final T id;
    private final State state;
    private final String trace;
    private final String workerId;
    private final int generation;

    public AbstractStatus(T id,
                          State state,
                          String workerId,
                          int generation,
                          String trace) {
        this.id = id;
        this.state = state;
        this.workerId = workerId;
        this.generation = generation;
        this.trace = trace;
    }

    public T id() {
        return id;
    }

    public State state() {
        return state;
    }

    public String trace() {
        return trace;
    }

    public String workerId() {
        return workerId;
    }

    public int generation() {
        return generation;
    }

    @Override
    public String toString() {
        return "Status{" +
                "id=" + id +
                ", state=" + state +
                ", workerId='" + workerId + '\'' +
                ", generation=" + generation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractStatus<?> that = (AbstractStatus<?>) o;

        return generation == that.generation
                && Objects.equals(id, that.id)
                && state == that.state
                && Objects.equals(trace, that.trace)
                && Objects.equals(workerId, that.workerId);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (trace != null ? trace.hashCode() : 0);
        result = 31 * result + (workerId != null ? workerId.hashCode() : 0);
        result = 31 * result + generation;
        return result;
    }
}
