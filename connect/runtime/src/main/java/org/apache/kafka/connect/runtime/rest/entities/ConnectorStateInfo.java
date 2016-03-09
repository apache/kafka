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
package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ConnectorStateInfo {

    private final String name;
    private final ConnectorState connector;
    private final List<TaskState> tasks;

    @JsonCreator
    public ConnectorStateInfo(@JsonProperty("name") String name,
                              @JsonProperty("connector") ConnectorState connector,
                              @JsonProperty("tasks") List<TaskState> tasks) {
        this.name = name;
        this.connector = connector;
        this.tasks = tasks;
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public ConnectorState connector() {
        return connector;
    }

    @JsonProperty
    public List<TaskState> tasks() {
        return tasks;
    }

    public abstract static class AbstractState {
        private final String state;
        private final String trace;
        private final String workerId;

        public AbstractState(String state, String workerId, String trace) {
            this.state = state;
            this.workerId = workerId;
            this.trace = trace;
        }

        @JsonProperty
        public String state() {
            return state;
        }

        @JsonProperty("worker_id")
        public String workerId() {
            return workerId;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public String trace() {
            return trace;
        }
    }

    public static class ConnectorState extends AbstractState {
        public ConnectorState(String state, String worker, String msg) {
            super(state, worker, msg);
        }
    }

    public static class TaskState extends AbstractState implements Comparable<TaskState> {
        private final int id;

        public TaskState(int id, String state, String worker, String msg) {
            super(state, worker, msg);
            this.id = id;
        }

        @JsonProperty
        public int id() {
            return id;
        }

        @Override
        public int compareTo(TaskState that) {
            return Integer.compare(this.id, that.id);
        }
    }

}
