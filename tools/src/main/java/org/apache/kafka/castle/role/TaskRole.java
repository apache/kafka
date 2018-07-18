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

package org.apache.kafka.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.castle.action.Action;
import org.apache.kafka.castle.action.TaskStartAction;
import org.apache.kafka.castle.action.TaskStatusAction;
import org.apache.kafka.castle.action.TaskStopAction;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A role which runs tasks inside Trogdor.
 */
public class TaskRole implements Role {
    private final int initialDelayMs;

    private final Map<String, TaskSpec> taskSpecs;

    private final NavigableSet<String> waitFor;

    @JsonCreator
    public TaskRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                    @JsonProperty("taskSpecs") TreeMap<String, TaskSpec> taskSpecs,
                    @JsonProperty("waitFor") List<String> waitFor) {
        this.initialDelayMs = initialDelayMs;
        this.taskSpecs = Collections.unmodifiableMap(taskSpecs == null ?
            Collections.emptyMap() : new TreeMap<>(taskSpecs));
        if ((waitFor == null) || (waitFor.isEmpty())) {
            this.waitFor = Collections.unmodifiableNavigableSet(
                new TreeSet<>(this.taskSpecs.keySet()));
        } else {
            for (String taskId : waitFor) {
                if (!taskSpecs.containsKey(taskId)) {
                    throw new RuntimeException("waitFor contains task ID " +
                        taskId + ", but only task ID(s) " + Utils.mkList(taskSpecs.keySet(), ", ") +
                        " appear in taskSpecs.");
                }
            }
            this.waitFor = Collections.unmodifiableNavigableSet(
                new TreeSet<>(waitFor));
        }
    }

    @Override
    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public Map<String, TaskSpec> taskSpecs() {
        return taskSpecs;
    }

    @JsonProperty
    public NavigableSet<String> waitFor() {
        return waitFor;
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
