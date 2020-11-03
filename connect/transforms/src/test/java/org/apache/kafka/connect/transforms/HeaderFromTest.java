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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HeaderFromTest {

    private final boolean keyTransform;

    static class RecordBuilder {
        private final List<String> fields = new ArrayList<>(2);
        private final List<Schema> fieldSchemas = new ArrayList<>(2);
        private final List<Object> fieldValues = new ArrayList<>(2);
        private final ConnectHeaders headers = new ConnectHeaders();

        public RecordBuilder() {
        }

        public RecordBuilder withField(String name, Schema schema, Object value) {
            fields.add(name);
            fieldSchemas.add(schema);
            fieldValues.add(value);
            return this;
        }

        public RecordBuilder addHeader(String name, Schema schema, Object value) {
            headers.add(name, new SchemaAndValue(schema, value));
            return this;
        }

        public SourceRecord schemaless(boolean keyTransform) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < this.fields.size(); i++) {
                String fieldName = this.fields.get(i);
                map.put(fieldName, this.fieldValues.get(i));

            }
            return sourceRecord(keyTransform, null, map);
        }

        private Schema schema() {
            SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
            for (int i = 0; i < this.fields.size(); i++) {
                String fieldName = this.fields.get(i);
                schemaBuilder.field(fieldName, this.fieldSchemas.get(i));

            }
            return schemaBuilder.build();
        }

        private Struct struct(Schema schema) {
            Struct struct = new Struct(schema);
            for (int i = 0; i < this.fields.size(); i++) {
                String fieldName = this.fields.get(i);
                struct.put(fieldName, this.fieldValues.get(i));
            }
            return struct;
        }

        public SourceRecord withSchema(boolean keyTransform) {
            Schema schema = schema();
            Struct struct = struct(schema);
            return sourceRecord(keyTransform, schema, struct);
        }

        private SourceRecord sourceRecord(boolean keyTransform, Schema keyOrValueSchema, Object keyOrValue) {
            Map<String, ?> sourcePartition = singletonMap("foo", "bar");
            Map<String, ?> sourceOffset = singletonMap("baz", "quxx");
            String topic = "topic";
            Integer partition = 0;
            Long timestamp = 0L;

            ConnectHeaders headers = this.headers;
            if (keyOrValueSchema == null) {
                // When doing a schemaless transformation we don't expect the header to have a schema
                headers = new ConnectHeaders();
                for (Header header : this.headers) {
                    headers.add(header.key(), new SchemaAndValue(null, header.value()));
                }
            }
            return new SourceRecord(sourcePartition, sourceOffset, topic, partition,
                    keyTransform ? keyOrValueSchema : null,
                    keyTransform ? keyOrValue : "key",
                    !keyTransform ? keyOrValueSchema : null,
                    !keyTransform ? keyOrValue : "value",
                    timestamp, headers);
        }

        @Override
        public String toString() {
            return "RecordBuilder(" +
                    "fields=" + fields +
                    ", fieldSchemas=" + fieldSchemas +
                    ", fieldValues=" + fieldValues +
                    ", headers=" + headers +
                    ')';
        }
    }

    @Parameterized.Parameters(name = "{0}: testKey={1}, xformFields={3}, xformHeaders={4}, operation={5}")
    public static Collection<Object[]> data() {

        List<Object[]> result = new ArrayList<>();



        for (Boolean testKeyTransform : asList(true, false)) {
            result.add(
                new Object[]{
                    "basic copy",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.COPY,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                });
            result.add(
                new Object[]{
                    "basic move",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.MOVE,
                    new RecordBuilder()
                        // field1 got moved
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                });
            result.add(
                new Object[]{
                    "copy with preexisting header",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.COPY,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                });
            result.add(
                new Object[]{
                    "move with preexisting header",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.MOVE,
                    new RecordBuilder()
                        // field1 got moved
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                });
            Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("foo", STRING_SCHEMA).build();
            Struct struct = new Struct(schema).put("foo", "foo-value");
            result.add(
                new Object[]{
                    "copy with struct value",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", schema, struct)
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.COPY,
                    new RecordBuilder()
                        .withField("field1", schema, struct)
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", schema, struct)
                });
            result.add(
                new Object[]{
                    "move with struct value",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", schema, struct)
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    singletonList("field1"), singletonList("inserted1"), HeaderFrom.Operation.MOVE,
                    new RecordBuilder()
                        // field1 got moved
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", schema, struct)
                });
            result.add(
                new Object[]{
                    "two headers from same field",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    // two headers from the same field
                    asList("field1", "field1"), asList("inserted1", "inserted2"), HeaderFrom.Operation.MOVE,
                    new RecordBuilder()
                        // field1 got moved
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                        .addHeader("inserted2", STRING_SCHEMA, "field1-value")
                });
            result.add(
                new Object[]{
                    "two fields to same header",
                    testKeyTransform,
                    new RecordBuilder()
                        .withField("field1", STRING_SCHEMA, "field1-value")
                        .withField("field2", STRING_SCHEMA, "field2-value")
                        .addHeader("header1", STRING_SCHEMA, "existing-value"),
                    // two headers from the same field
                    asList("field1", "field2"), asList("inserted1", "inserted1"), HeaderFrom.Operation.MOVE,
                    new RecordBuilder()
                        // field1 and field2 got moved
                        .addHeader("header1", STRING_SCHEMA, "existing-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                        .addHeader("inserted1", STRING_SCHEMA, "field2-value")
                });
        }
        return result;
    }

    private final HeaderFrom<SourceRecord> xform;

    private final RecordBuilder originalRecordBuilder;
    private final RecordBuilder expectedRecordBuilder;
    private final List<String> transformFields;
    private final List<String> headers;
    private final HeaderFrom.Operation operation;

    public HeaderFromTest(String description,
                          boolean keyTransform,
                          RecordBuilder originalBuilder,
                          List<String> transformFields, List<String> headers, HeaderFrom.Operation operation,
                          RecordBuilder expectedBuilder) {
        this.keyTransform = keyTransform;
        this.xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();
        this.originalRecordBuilder = originalBuilder;
        this.expectedRecordBuilder = expectedBuilder;
        this.transformFields = transformFields;
        this.headers = headers;
        this.operation = operation;
    }

    private Map<String, Object> config() {
        Map<String, Object> result = new HashMap<>();
        result.put(HeaderFrom.HEADERS_FIELD, headers);
        result.put(HeaderFrom.FIELDS_FIELD, transformFields);
        result.put(HeaderFrom.OPERATION_FIELD, operation.toString());
        return result;
    }

    @Test
    public void schemaless() {
        xform.configure(config());
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");

        SourceRecord originalRecord = originalRecordBuilder.schemaless(keyTransform);
        SourceRecord expectedRecord = expectedRecordBuilder.schemaless(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }

    @Test
    public void withSchema() {
        xform.configure(config());
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");
        Headers expect = headers.duplicate();
        for (int i = 0; i < this.headers.size(); i++) {
            expect.add(this.headers.get(i), originalRecordBuilder.fieldValues.get(i), originalRecordBuilder.fieldSchemas.get(i));
        }

        SourceRecord originalRecord = originalRecordBuilder.withSchema(keyTransform);
        SourceRecord expectedRecord = expectedRecordBuilder.withSchema(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }

    @Test(expected = ConfigException.class)
    public void invalidConfig() {
        Map<String, Object> config = config();
        List<String> headers = new ArrayList<>(this.headers);
        headers.add("unexpected");
        config.put(HeaderFrom.HEADERS_FIELD, headers);
        xform.configure(config);
    }

    private static void assertSameRecord(SourceRecord expected, SourceRecord xformed) {
        assertEquals(expected.sourcePartition(), xformed.sourcePartition());
        assertEquals(expected.sourceOffset(), xformed.sourceOffset());
        assertEquals(expected.topic(), xformed.topic());
        assertEquals(expected.kafkaPartition(), xformed.kafkaPartition());
        assertEquals(expected.keySchema(), xformed.keySchema());
        assertEquals(expected.key(), xformed.key());
        assertEquals(expected.valueSchema(), xformed.valueSchema());
        assertEquals(expected.value(), xformed.value());
        assertEquals(expected.timestamp(), xformed.timestamp());
        assertEquals(expected.headers(), xformed.headers());
    }

}

