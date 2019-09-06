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
package org.apache.kafka.connect.sink;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SinkRecordTest {

    private static final String TOPIC_NAME = "myTopic";
    private static final Integer PARTITION_NUMBER = 0;
    private static final long KAFKA_OFFSET = 0L;
    private static final Long KAFKA_TIMESTAMP = 0L;
    private static final TimestampType TS_TYPE = TimestampType.CREATE_TIME;

    private SinkRecord record;

    @Before
    public void beforeEach() {
        record = new SinkRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false, KAFKA_OFFSET,
                                KAFKA_TIMESTAMP, TS_TYPE, null);
    }

    @Test
    public void shouldCreateSinkRecordWithHeaders() {
        Headers headers = new ConnectHeaders().addString("h1", "hv1").addBoolean("h2", true);
        record = new SinkRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false, KAFKA_OFFSET,
                                KAFKA_TIMESTAMP, TS_TYPE, headers);
        assertNotNull(record.headers());
        assertSame(headers, record.headers());
        assertFalse(record.headers().isEmpty());
    }

    @Test
    public void shouldCreateSinkRecordWithEmptyHeaders() {
        assertEquals(TOPIC_NAME, record.topic());
        assertEquals(PARTITION_NUMBER, record.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, record.keySchema());
        assertEquals("key", record.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, record.valueSchema());
        assertEquals(false, record.value());
        assertEquals(KAFKA_OFFSET, record.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, record.timestamp());
        assertEquals(TS_TYPE, record.timestampType());
        assertNotNull(record.headers());
        assertTrue(record.headers().isEmpty());
    }

    @Test
    public void shouldDuplicateRecordAndCloneHeaders() {
        SinkRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_OFFSET, duplicate.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertEquals(TS_TYPE, duplicate.timestampType());
        assertNotNull(duplicate.headers());
        assertTrue(duplicate.headers().isEmpty());
        assertNotSame(record.headers(), duplicate.headers());
        assertEquals(record.headers(), duplicate.headers());
    }


    @Test
    public void shouldDuplicateRecordUsingNewHeaders() {
        Headers newHeaders = new ConnectHeaders().addString("h3", "hv3");
        SinkRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                KAFKA_TIMESTAMP, newHeaders);

        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_OFFSET, duplicate.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertEquals(TS_TYPE, duplicate.timestampType());
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