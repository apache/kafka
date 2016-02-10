/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

public abstract class AbstractStatus {

    public enum State {
        UNASSIGNED,
        RUNNING,
        FAILED,
        DESTROYED,
    }

    private final State state;
    private final String msg;
    private final String workerId;
    private final int generation;

    public AbstractStatus(State state,
                          String msg,
                          String workerId,
                          int generation) {
        this.state = state;
        this.workerId = workerId;
        this.msg = msg;
        this.generation = generation;
    }

    public State state() {
        return state;
    }

    public String msg() {
        return msg;
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
                "state=" + state +
                ", msg='" + msg + '\'' +
                ", workerId='" + workerId + '\'' +
                ", generation=" + generation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractStatus that = (AbstractStatus) o;

        if (generation != that.generation) return false;
        if (state != that.state) return false;
        if (msg != null ? !msg.equals(that.msg) : that.msg != null) return false;
        return workerId != null ? workerId.equals(that.workerId) : that.workerId == null;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (msg != null ? msg.hashCode() : 0);
        result = 31 * result + (workerId != null ? workerId.hashCode() : 0);
        result = 31 * result + generation;
        return result;
    }
}
