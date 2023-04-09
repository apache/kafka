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
package org.apache.kafka.connect.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.tools.ThroughputThrottler;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A connector primarily intended for system tests. The connector simply generates as many tasks as requested. The
 * tasks print metadata in the form of JSON to stdout for each message generated, making externally visible which
 * messages have been sent. Each message is also assigned a unique, increasing seqno that is passed to Kafka Connect; when
 * tasks are started on new nodes, this seqno is used to resume where the task previously left off, allowing for
 * testing of distributed Kafka Connect.
 *
 * If logging is left enabled, log output on stdout can be easily ignored by checking whether a given line is valid JSON.
 */
public class VerifiableSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(VerifiableSourceTask.class);

    public static final String NAME_CONFIG = "name";
    public static final String ID_CONFIG = "id";
    public static final String TOPIC_CONFIG = "topic";
    public static final String THROUGHPUT_CONFIG = "throughput";
    public static final String COMPLETE_RECORD_DATA_CONFIG = "complete.record.data";

    private static final String ID_FIELD = "id";
    private static final String SEQNO_FIELD = "seqno";

    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    private String name; // Connector name
    private int id; // Task ID
    private String topic;
    private Map<String, Integer> partition;
    private long startingSeqno;
    private long seqno;
    private ThroughputThrottler throttler;
    private boolean completeRecordData;

    private static final Schema COMPLETE_VALUE_SCHEMA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("task", Schema.INT32_SCHEMA)
        .field("topic", Schema.STRING_SCHEMA)
        .field("time_ms", Schema.INT64_SCHEMA)
        .field("seqno", Schema.INT64_SCHEMA)
        .build();

    @Override
    public String version() {
        return new VerifiableSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        final long throughput;
        try {
            name = props.get(NAME_CONFIG);
            id = Integer.parseInt(props.get(ID_CONFIG));
            topic = props.get(TOPIC_CONFIG);
            throughput = Long.parseLong(props.get(THROUGHPUT_CONFIG));
        } catch (NumberFormatException e) {
            throw new ConnectException("Invalid VerifiableSourceTask configuration", e);
        }

        partition = Collections.singletonMap(ID_FIELD, id);
        Map<String, Object> previousOffset = this.context.offsetStorageReader().offset(partition);
        if (previousOffset != null)
            seqno = (Long) previousOffset.get(SEQNO_FIELD) + 1;
        else
            seqno = 0;
        startingSeqno = seqno;
        throttler = new ThroughputThrottler(throughput, System.currentTimeMillis());
        completeRecordData = "true".equalsIgnoreCase(props.get(COMPLETE_RECORD_DATA_CONFIG));

        log.info("Started VerifiableSourceTask {}-{} producing to topic {} resuming from seqno {}", name, id, topic, startingSeqno);
    }

    @Override
    public List<SourceRecord> poll() {
        long sendStartMs = System.currentTimeMillis();
        if (throttler.shouldThrottle(seqno - startingSeqno, sendStartMs))
            throttler.throttle();

        long nowMs = System.currentTimeMillis();

        Map<String, Object> data = new HashMap<>();
        data.put("name", name);
        data.put("task", id);
        data.put("topic", this.topic);
        data.put("time_ms", nowMs);
        data.put("seqno", seqno);
        String dataJson;
        try {
            dataJson = JSON_SERDE.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            dataJson = "Bad data can't be written as json: " + e.getMessage();
        }
        System.out.println(dataJson);

        Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
        Schema valueSchema = completeRecordData ? COMPLETE_VALUE_SCHEMA : Schema.INT64_SCHEMA;
        Object value = completeRecordData ? completeValue(data) : seqno;
        SourceRecord srcRecord = new SourceRecord(partition, ccOffset, topic, Schema.INT32_SCHEMA, id, valueSchema, value);
        List<SourceRecord> result = Collections.singletonList(srcRecord);
        seqno++;
        return result;
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        Map<String, Object> data = new HashMap<>();
        data.put("name", name);
        data.put("task", id);
        data.put("topic", this.topic);
        data.put("time_ms", System.currentTimeMillis());
        data.put("seqno", record.value());
        data.put("committed", true);

        String dataJson;
        try {
            dataJson = JSON_SERDE.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            dataJson = "Bad data can't be written as json: " + e.getMessage();
        }
        System.out.println(dataJson);
    }

    @Override
    public void stop() {
        if (throttler != null)
            throttler.wakeup();
    }

    private Object completeValue(Map<String, Object> data) {
        Struct result = new Struct(COMPLETE_VALUE_SCHEMA);
        Stream.of("name", "task", "topic", "time_ms", "seqno").forEach(
            field -> result.put(field, data.get(field))
        );
        return result;
    }
}
