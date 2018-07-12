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

package org.apache.kafka.soak.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.TaskStartAction;
import org.apache.kafka.soak.action.TaskStatusAction;
import org.apache.kafka.soak.action.TaskStopAction;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

/**
 * A role which runs tasks inside Trogdor.
 */
public class TaskRole implements Role {
    private final int initialDelayMs;

    private final TreeMap<String, TaskSpec> taskSpecs;

    @JsonCreator
    public TaskRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                    @JsonProperty("taskSpecs") TreeMap<String, TaskSpec> taskSpecs) {
        this.initialDelayMs = initialDelayMs;
        this.taskSpecs = taskSpecs == null ? new TreeMap<String, TaskSpec>() : taskSpecs;
    }

    @Override
    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public TreeMap<String, TaskSpec> taskSpecs() {
        return taskSpecs;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new TaskStartAction(nodeName, this));
        actions.add(new TaskStatusAction(nodeName, this));
        actions.add(new TaskStopAction(nodeName, this));
        return actions;
    }
}
