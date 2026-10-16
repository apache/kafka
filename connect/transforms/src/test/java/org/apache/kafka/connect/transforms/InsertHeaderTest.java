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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InsertHeaderTest {

    private final InsertHeader<SourceRecord> xform = new InsertHeader<>();

    private Map<String, ?> config(String header, String valueLiteral) {
        return config(header, valueLiteral, null);
    }

    private Map<String, ?> config(String header, String valueLiteral, String valueType) {
        Map<String, String> result = new HashMap<>();
        result.put(InsertHeader.HEADER_FIELD, header);
        result.put(InsertHeader.VALUE_LITERAL_FIELD, valueLiteral);
        if (valueType != null) {
            result.put(InsertHeader.VALUE_TYPE_FIELD, valueType);
        }
        return result;
    }

    @Test
    public void insertionWithExistingOtherHeader() {
        xform.configure(config("inserted", "inserted-value"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");
        Headers expect = headers.duplicate().addString("inserted", "inserted-value");

        SourceRecord original = sourceRecord(headers);
        SourceRecord xformed = xform.apply(original);
        assertNonHeaders(original, xformed);
        assertEquals(expect, xformed.headers());
    }

    @Test
    public void insertionWithExistingSameHeader() {
        xform.configure(config("existing", "inserted-value"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "preexisting-value");
        Headers expect = headers.duplicate().addString("existing", "inserted-value");

        SourceRecord original = sourceRecord(headers);
        SourceRecord xformed = xform.apply(original);
        assertNonHeaders(original, xformed);
        assertEquals(expect, xformed.headers());
    }

    @Test
    public void insertionWithByteHeader() {
        xform.configure(config("inserted", "1"));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");
        Headers expect = headers.duplicate().addByte("inserted", (byte) 1);

        SourceRecord original = sourceRecord(headers);
        SourceRecord xformed = xform.apply(original);
        assertNonHeaders(original, xformed);
        assertEquals(expect, xformed.headers());
    }

    @Test
    public void insertionWithExplicitTypes() {
        assertInsertionWithExplicitType("int8", "1", Schema.INT8_SCHEMA, (byte) 1);
        assertInsertionWithExplicitType("int16", "2", Schema.INT16_SCHEMA, (short) 2);
        assertInsertionWithExplicitType("int32", "3", Schema.INT32_SCHEMA, 3);
        assertInsertionWithExplicitType("int64", "4", Schema.INT64_SCHEMA, 4L);
        assertInsertionWithExplicitType("float32", "1.5", Schema.FLOAT32_SCHEMA, 1.5f);
        assertInsertionWithExplicitType("float64", "2.5", Schema.FLOAT64_SCHEMA, 2.5d);
        assertInsertionWithExplicitType("boolean", "true", Schema.BOOLEAN_SCHEMA, true);
        assertInsertionWithExplicitType("string", "1", Schema.STRING_SCHEMA, "1");
        assertInsertionWithExplicitType("bytes", "bytes-value", Schema.BYTES_SCHEMA,
                "bytes-value".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void configRejectsNullHeaderKey() {
        assertThrows(ConfigException.class, () -> xform.configure(config(null, "1")));
    }

    @Test
    public void configRejectsNullHeaderValue() {
        assertThrows(ConfigException.class, () -> xform.configure(config("inserted", null)));
    }

    @Test
    public void configRejectsInvalidValueType() {
        assertThrows(ConfigException.class, () -> xform.configure(config("inserted", "1", "invalid")));
    }

    private void assertInsertionWithExplicitType(String valueType, String valueLiteral, Schema schema,
                                                Object expectedValue) {
        xform.configure(config("inserted", valueLiteral, valueType));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");

        SourceRecord original = sourceRecord(headers);
        SourceRecord xformed = xform.apply(original);
        assertNonHeaders(original, xformed);
        assertEquals("existing-value", xformed.headers().lastWithName("existing").value());

        Header inserted = xformed.headers().lastWithName("inserted");
        assertEquals(schema, inserted.schema());
        if (expectedValue instanceof byte[] expectedBytes) {
            assertArrayEquals(expectedBytes, (byte[]) inserted.value());
        } else {
            assertEquals(expectedValue, inserted.value());
        }
    }

    private void assertNonHeaders(SourceRecord original, SourceRecord xformed) {
        assertEquals(original.sourcePartition(), xformed.sourcePartition());
        assertEquals(original.sourceOffset(), xformed.sourceOffset());
        assertEquals(original.topic(), xformed.topic());
        assertEquals(original.kafkaPartition(), xformed.kafkaPartition());
        assertEquals(original.keySchema(), xformed.keySchema());
        assertEquals(original.key(), xformed.key());
        assertEquals(original.valueSchema(), xformed.valueSchema());
        assertEquals(original.value(), xformed.value());
        assertEquals(original.timestamp(), xformed.timestamp());
    }

    private SourceRecord sourceRecord(ConnectHeaders headers) {
        Map<String, ?> sourcePartition = Map.of("foo", "bar");
        Map<String, ?> sourceOffset = Map.of("baz", "quxx");
        String topic = "topic";
        Integer partition = 0;
        Schema keySchema = null;
        Object key = "key";
        Schema valueSchema = null;
        Object value = "value";
        Long timestamp = 0L;

        return new SourceRecord(sourcePartition, sourceOffset, topic, partition,
                keySchema, key, valueSchema, value, timestamp, headers);
    }

    @Test
    public void testInsertHeaderVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }
}
