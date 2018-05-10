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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This object will contain all the runtime context for an error which occurs in the Connect framework while
 * processing a record.
 */
public class ProcessingContext implements Structable {

    private final String taskId;
    private final Map<String, Object> workerConfig;
    private final List<Stage> stages;
    private final List<ErrorReporter> reporters;

    private Exception exception;
    private int current = 0;
    private int attempt;
    private ConnectRecord record;
    private long timetstamp;

    public static Builder newBuilder(ConnectorTaskId taskId, Map<String, Object> workerConfig) {
        Objects.requireNonNull(taskId);
        Objects.requireNonNull(workerConfig);

        return new Builder(taskId.toString(), workerConfig);
    }

    // VisibleForTesting
    public static ProcessingContext noop(String taskId) {
        return new ProcessingContext(taskId, Collections.<String, Object>emptyMap(), Collections.<Stage>emptyList(), Collections.<ErrorReporter>emptyList());
    }

    private ProcessingContext(String taskId, Map<String, Object> workerConfig, List<Stage> stages, List<ErrorReporter> reporters) {
        Objects.requireNonNull(taskId);
        Objects.requireNonNull(workerConfig);
        Objects.requireNonNull(stages);
        Objects.requireNonNull(reporters);

        this.taskId = taskId;
        this.workerConfig = workerConfig;
        this.stages = stages;
        this.reporters = reporters;
    }

    /**
     * @return the configuration of the Connect worker
     */
    public Map<String, Object> workerConfig() {
        return workerConfig;
    }

    /**
     * @return which task reported this error
     */
    public String taskId() {
        return taskId;
    }

    /**
     * @return an ordered list of stages. Connect will start with executing stage 0 and then move up the list.
     */
    public List<Stage> stages() {
        return stages;
    }

    /**
     * @return at what stage did this operation fail (0 indicates first stage)
     */
    public int index() {
        return current;
    }

    /**
     * @return which attempt was this (first error will be 0)
     */
    public int attempt() {
        return attempt;
    }

    /**
     * @return the (epoch) time of failure
     */
    public long timeOfError() {
        return timetstamp;
    }

    /**
     * The exception accompanying this failure (if any)
     */
    public Exception exception() {
        return exception;
    }

    /**
     * @return the record which when input the current stage caused the failure.
     */
    public ConnectRecord record() {
        return record;
    }

    public void setRecord(ConnectRecord record) {
        this.record = record;
    }

    public void setException(Exception ex) {
        this.exception = ex;
    }

    public void report() {
        Struct report = this.toStruct();
        for (ErrorReporter reporter : reporters) {
            reporter.report(report);
        }
    }

    @Override
    public Struct toStruct() {
        return null;
    }

    public void reset() {
        current = 0;
        attempt = 0;
    }

    /**
     * Position index to the first stage of the given type
     *
     * @param type the given type
     */
    public void position(StageType type) {
        reset();
        for (int i = 0; i < stages.size(); i++) {
            if (type == stages.get(i).type()) {
                current = i;
                break;
            }
        }
    }

    public void incrementAttempt() {
        attempt++;
    }

    public static class Builder {
        private final Map<String, Object> workerConfig;
        private final String taskId;

        private List<ErrorReporter> reporters = new ArrayList<>();
        private LinkedList<Stage> stages = new LinkedList<>();

        private Builder(String taskId, Map<String, Object> workerConfig) {
            this.taskId = taskId;
            this.workerConfig = workerConfig;
        }

        public void prependStage(Stage stage) {
            stages.addFirst(stage);
        }

        public void appendStage(Stage stage) {
            stages.addLast(stage);
        }

        public ProcessingContext build() {
            return new ProcessingContext(taskId, workerConfig, stages, reporters);
        }
    }
}
