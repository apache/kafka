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

import org.apache.kafka.common.utils.Base64;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.errors.impl.StructUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    private final ErrorReporter[] reporters;

    private Exception exception;
    private int current = 0;
    private int attempt;
    private ConnectRecord record;
    private long timetstamp;

    private JsonConverter valueConverter = new JsonConverter();

    public static Builder newBuilder(ConnectorTaskId taskId, Map<String, Object> workerConfig) {
        Objects.requireNonNull(taskId);
        Objects.requireNonNull(workerConfig);

        return new Builder(taskId.toString(), workerConfig);
    }

    // VisibleForTesting
    public static ProcessingContext noop(String taskId) {
        return new ProcessingContext(taskId, Collections.<String, Object>emptyMap(), Collections.<Stage>emptyList(), new ErrorReporter[]{});
    }

    private ProcessingContext(String taskId, Map<String, Object> workerConfig, List<Stage> stages, ErrorReporter[] reporters) {
        Objects.requireNonNull(taskId);
        Objects.requireNonNull(workerConfig);
        Objects.requireNonNull(stages);
        Objects.requireNonNull(reporters);

        this.taskId = taskId;
        this.workerConfig = workerConfig;
        this.stages = stages;
        this.reporters = reporters;

        Map<String, Object> config = new HashMap<>();
        config.put("schemas.enable", false);
        config.put("converter.type", "value");
        valueConverter.configure(config);
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
    @Field("task_id")
    public String taskId() {
        return taskId;
    }

    /**
     * @return an ordered list of stages. Connect will start with executing stage 0 and then move up the list.
     */
    public List<Stage> stages() {
        return stages;
    }

    public Stage current() {
        return stages.get(current);
    }

    /**
     * @return at what stage did this operation fail (0 indicates first stage)
     */
    @Field("index")
    public int index() {
        return current;
    }

    /**
     * @return which attempt was this (first error will be 0)
     */
    @Field("attempt")
    public int attempt() {
        return attempt;
    }

    /**
     * @return the (epoch) time of failure
     */
    @Field("time_of_error")
    public long timeOfError() {
        return timetstamp;
    }

    public void setTimeOfError(long timetstamp) {
        this.timetstamp = timetstamp;
    }

    /**
     * The exception accompanying this failure (if any)
     */
    @Field("exception")
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
        for (ErrorReporter reporter : reporters) {
            reporter.report(this);
        }
    }

    @Override
    public Struct toStruct() {
        Struct metadata = StructUtil.toStruct(this);

        Schema valueSchema = new SchemaBuilder(Schema.Type.STRUCT)
                .field("schema", Schema.STRING_SCHEMA)
                .field("object", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("schema", record.valueSchema().type().toString());
        value.put("object", Base64.encoder().encodeToString((byte[]) record.value()));

        SchemaBuilder recordSchema = new SchemaBuilder(Schema.Type.STRUCT);
        recordSchema.field("topic", Schema.STRING_SCHEMA);
        recordSchema.field("timestamp", Schema.INT64_SCHEMA);
        recordSchema.field("offset", Schema.INT64_SCHEMA);
        recordSchema.field("partition", Schema.INT32_SCHEMA);
        recordSchema.field("value", value.schema());

        Struct recordStruct = new Struct(recordSchema);
        recordStruct.put("topic", record().topic());
        recordStruct.put("timestamp", record().timestamp());
        recordStruct.put("partition", record().kafkaPartition());
        recordStruct.put("offset", ((SinkRecord) record()).kafkaOffset());
        recordStruct.put("value", value);

        Struct stage = StructUtil.toStruct(current());

        SchemaBuilder finalSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                .field("record", recordSchema)
                .field("stage", stage.schema());
        for (org.apache.kafka.connect.data.Field f : metadata.schema().fields()) {
            finalSchemaBuilder.field(f.name(), f.schema());
        }

        finalSchemaBuilder.field("stages", SchemaBuilder.array(stage.schema()).build());
        Struct struct = new Struct(finalSchemaBuilder.build());

        for (org.apache.kafka.connect.data.Field f : metadata.schema().fields()) {
            struct.put(f.name(), metadata.get(f));
        }
        struct.put("record", recordStruct);
        struct.put("stage", stage);

        List<Struct> ll = new ArrayList<>();
        for (Stage st : stages()) {
            ll.add(StructUtil.toStruct(st));
        }
        struct.put("stages", ll);

        return struct;
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

    public void next() {
        current++;
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

        public Builder prependStage(Stage stage) {
            stages.addFirst(stage);
            return this;
        }

        public Builder appendStage(Stage stage) {
            stages.addLast(stage);
            return this;
        }

        public Builder addReporters(Collection<ErrorReporter> reporters) {
            this.reporters.addAll(reporters);
            return this;
        }

        public ProcessingContext build() {
            return new ProcessingContext(taskId, workerConfig, stages, reporters.toArray(new ErrorReporter[0]));
        }
    }
}
