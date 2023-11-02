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
package org.apache.kafka.connect.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SourceRecordTest {

    private static final Map<String, ?> SOURCE_PARTITION = Collections.singletonMap("src", "abc");
    private static final Map<String, ?> SOURCE_OFFSET = Collections.singletonMap("offset", "1");
    private static final String TOPIC_NAME = "myTopic";
    private static final Integer PARTITION_NUMBER = 0;
    private static final Long KAFKA_TIMESTAMP = 0L;

    private SourceRecord record;

    @BeforeEach
    public void beforeEach() {
        record = new SourceRecord(SOURCE_PARTITION, SOURCE_OFFSET, TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key",
                                  Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP, null);
    }

    @Test
    public void shouldCreateSinkRecordWithHeaders() {
        Headers headers = new ConnectHeaders().addString("h1", "hv1").addBoolean("h2", true);
        record = new SourceRecord(SOURCE_PARTITION, SOURCE_OFFSET, TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key",
                                  Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP, headers);
        assertNotNull(record.headers());
        assertSame(headers, record.headers());
        assertFalse(record.headers().isEmpty());
    }

    @Test
    public void shouldCreateSinkRecordWithEmptyHeaders() {
        assertEquals(SOURCE_PARTITION, record.sourcePartition());
        assertEquals(SOURCE_OFFSET, record.sourceOffset());
        assertEquals(TOPIC_NAME, record.topic());
        assertEquals(PARTITION_NUMBER, record.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, record.keySchema());
        assertEquals("key", record.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, record.valueSchema());
        assertEquals(false, record.value());
        assertEquals(KAFKA_TIMESTAMP, record.timestamp());
        assertNotNull(record.headers());
        assertTrue(record.headers().isEmpty());
    }

    @Test
    public void shouldDuplicateRecordAndCloneHeaders() {
        SourceRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                  KAFKA_TIMESTAMP);

        assertEquals(SOURCE_PARTITION, duplicate.sourcePartition());
        assertEquals(SOURCE_OFFSET, duplicate.sourceOffset());
        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertNotNull(duplicate.headers());
        assertTrue(duplicate.headers().isEmpty());
        assertNotSame(record.headers(), duplicate.headers());
        assertEquals(record.headers(), duplicate.headers());
    }

    @Test
    public void shouldDuplicateRecordUsingNewHeaders() {
        Headers newHeaders = new ConnectHeaders().addString("h3", "hv3");
        SourceRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                  KAFKA_TIMESTAMP, newHeaders);

        assertEquals(SOURCE_PARTITION, duplicate.sourcePartition());
        assertEquals(SOURCE_OFFSET, duplicate.sourceOffset());
        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertNotNull(duplicate.headers());
        assertEquals(newHeaders, duplicate.headers());
        assertSame(newHeaders, duplicate.headers());
        assertNotSame(record.headers(), duplicate.headers());
        assertNotEquals(record.headers(), duplicate.headers());
    }

    @Test
    public void shouldModifyRecordHeader() {
        assertTrue(record.headers().isEmpty());
        record.headers().addInt("intHeader", 100);
        assertEquals(1, record.headers().size());
        Header header = record.headers().lastWithName("intHeader");
        assertEquals(100, (int) Values.convertToInteger(header.schema(), header.value()));
    }
}