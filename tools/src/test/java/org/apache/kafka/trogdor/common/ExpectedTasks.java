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

package org.apache.kafka.trogdor.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.WorkerState;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class ExpectedTasks {
    private static final Logger log = LoggerFactory.getLogger(ExpectedTasks.class);

    private final TreeMap<String, ExpectedTask> expected = new TreeMap<>();

    public static class ExpectedTaskBuilder {
        private final String id;
        private TaskSpec taskSpec = null;
        private TaskState taskState = null;
        private WorkerState workerState = null;

        public ExpectedTaskBuilder(String id) {
            this.id = id;
        }

        public ExpectedTaskBuilder taskSpec(TaskSpec taskSpec) {
            this.taskSpec = taskSpec;
            return this;
        }

        public ExpectedTaskBuilder taskState(TaskState taskState) {
            this.taskState = taskState;
            return this;
        }

        public ExpectedTaskBuilder workerState(WorkerState workerState) {
            this.workerState = workerState;
            return this;
        }

        public ExpectedTask build() {
            return new ExpectedTask(id, taskSpec, taskState, workerState);
        }
    }

    static class ExpectedTask {
        private final String id;
        private final TaskSpec taskSpec;
        private final TaskState taskState;
        private final WorkerState workerState;

        @JsonCreator
        private ExpectedTask(@JsonProperty("id") String id,
                     @JsonProperty("taskSpec") TaskSpec taskSpec,
                     @JsonProperty("taskState") TaskState taskState,
                     @JsonProperty("workerState") WorkerState workerState) {
            this.id = id;
            this.taskSpec = taskSpec;
            this.taskState = taskState;
            this.workerState = workerState;
        }

        String compare(TaskState actual) {
            if (actual == null) {
                return "Did not find task " + id + "\n";
            }
            if ((taskSpec != null) && (!actual.spec().equals(taskSpec))) {
                return "Invalid spec for task " + id + ": expected " + taskSpec +
                    ", got " + actual.spec();
            }
            if ((taskState != null) && (!actual.equals(taskState))) {
                return "Invalid state for task " + id + ": expected " + taskState +
                    ", got " + actual;
            }
            return null;
        }

        String compare(WorkerState actual) {
            if ((workerState != null) && (!workerState.equals(actual))) {
                if (actual == null) {
                    return "Did not find worker " + id + "\n";
                }
                return "Invalid state for task " + id + ": expected " + workerState +
                    ", got " + actual;
            }
            return null;
        }

        @JsonProperty
        public String id() {
            return id;
        }

        @JsonProperty
        public TaskSpec taskSpec() {
            return taskSpec;
        }

        @JsonProperty
        public TaskState taskState() {
            return taskState;
        }

        @JsonProperty
        public WorkerState workerState() {
            return workerState;
        }
    }

    public ExpectedTasks addTask(ExpectedTask task) {
        expected.put(task.id, task);
        return this;
    }

    public ExpectedTasks waitFor(final CoordinatorClient client) throws InterruptedException {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                TasksResponse tasks = null;
                try {
                    tasks = client.tasks(new TasksRequest(null, 0, 0, 0, 0));
                } catch (Exception e) {
                    log.info("Unable to get coordinator tasks", e);
                    throw new RuntimeException(e);
                }
                StringBuilder errors = new StringBuilder();
                for (Map.Entry<String, ExpectedTask> entry : expected.entrySet()) {
                    String id = entry.getKey();
                    ExpectedTask task = entry.getValue();
                    String differences = task.compare(tasks.tasks().get(id));
                    if (differences != null) {
                        errors.append(differences);
                    }
                }
                String errorString = errors.toString();
                if (!errorString.isEmpty()) {
                    log.info("EXPECTED TASKS: {}", JsonUtil.toJsonString(expected));
                    log.info("ACTUAL TASKS  : {}", JsonUtil.toJsonString(tasks.tasks()));
                    log.info(errorString);
                    return false;
                }
                return true;
            }
        }, "Timed out waiting for expected tasks " + JsonUtil.toJsonString(expected));
        return this;
    }

    public ExpectedTasks waitFor(final AgentClient client) throws InterruptedException {
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                AgentStatusResponse status = null;
                try {
                    status = client.status();
                } catch (Exception e) {
                    log.info("Unable to get agent status", e);
                    throw new RuntimeException(e);
                }
                StringBuilder errors = new StringBuilder();
                HashMap<String, WorkerState> taskIdToWorkerState = new HashMap<>();
                for (WorkerState state : status.workers().values()) {
                    taskIdToWorkerState.put(state.taskId(), state);
                }
                for (Map.Entry<String, ExpectedTask> entry : expected.entrySet()) {
                    String id = entry.getKey();
                    ExpectedTask worker = entry.getValue();
                    String differences = worker.compare(taskIdToWorkerState.get(id));
                    if (differences != null) {
                        errors.append(differences);
                    }
                }
                String errorString = errors.toString();
                if (!errorString.isEmpty()) {
                    log.info("EXPECTED WORKERS: {}", JsonUtil.toJsonString(expected));
                    log.info("ACTUAL WORKERS  : {}", JsonUtil.toJsonString(status.workers()));
                    log.info(errorString);
                    return false;
                }
                return true;
            }
        }, "Timed out waiting for expected workers " + JsonUtil.toJsonString(expected));
        return this;
    }
};
