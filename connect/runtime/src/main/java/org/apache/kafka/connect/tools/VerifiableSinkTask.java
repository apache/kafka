/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Counterpart to {@link VerifiableSourceTask} that consumes records and logs information about each to stdout. This
 * allows validation of processing of messages by sink tasks on distributed workers even in the face of worker restarts
 * and failures. This task relies on the offset management provided by the Kafka Connect framework and therefore can detect
 * bugs in its implementation.
 */
public class VerifiableSinkTask extends SinkTask {
    public static final String NAME_CONFIG = "name";
    public static final String ID_CONFIG = "id";

    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    private String name; // Connector name
    private int id; // Task ID

    private ArrayList<Map<String, Object>> unflushed = new ArrayList<>();

    @Override
    public String version() {
        return new VerifiableSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            name = props.get(NAME_CONFIG);
            id = Integer.parseInt(props.get(ID_CONFIG));
        } catch (NumberFormatException e) {
            throw new ConnectException("Invalid VerifiableSourceTask configuration", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        long nowMs = System.currentTimeMillis();
        for (SinkRecord record : records) {
            Map<String, Object> data = new HashMap<>();
            data.put("name", name);
            data.put("task", record.key()); // VerifiableSourceTask's input task (source partition)
            data.put("sinkTask", id);
            data.put("topic", record.topic());
            data.put("time_ms", nowMs);
            data.put("seqno", record.value());
            data.put("offset", record.kafkaOffset());
            String dataJson;
            try {
                dataJson = JSON_SERDE.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                dataJson = "Bad data can't be written as json: " + e.getMessage();
            }
            System.out.println(dataJson);
            unflushed.add(data);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        long nowMs = System.currentTimeMillis();
        for (Map<String, Object> data : unflushed) {
            data.put("time_ms", nowMs);
            data.put("flushed", true);
            String dataJson;
            try {
                dataJson = JSON_SERDE.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                dataJson = "Bad data can't be written as json: " + e.getMessage();
            }
            System.out.println(dataJson);
        }
        unflushed.clear();
    }

    @Override
    public void stop() {

    }
}
