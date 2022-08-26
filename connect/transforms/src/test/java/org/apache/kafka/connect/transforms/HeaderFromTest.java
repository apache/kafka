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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HeaderFromTest {

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

    public static List<Arguments> data() {

        List<Arguments> result = new ArrayList<>();

        for (Boolean testKeyTransform : asList(true, false)) {
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
            Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("foo", STRING_SCHEMA).build();
            Struct struct = new Struct(schema).put("foo", "foo-value");
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
            result.add(
                Arguments.of(
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
                ));
        }
        return result;
    }

    private Map<String, Object> config(List<String> headers, List<String> transformFields, HeaderFrom.Operation operation) {
        Map<String, Object> result = new HashMap<>();
        result.put(HeaderFrom.HEADERS_FIELD, headers);
        result.put(HeaderFrom.FIELDS_FIELD, transformFields);
        result.put(HeaderFrom.OPERATION_FIELD, operation.toString());
        return result;
    }

    @ParameterizedTest
    @MethodSource("data")
    public void schemaless(String description,
                           boolean keyTransform,
                           RecordBuilder originalBuilder,
                           List<String> transformFields, List<String> headers1, HeaderFrom.Operation operation,
                           RecordBuilder expectedBuilder) {
        HeaderFrom<SourceRecord> xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();

        xform.configure(config(headers1, transformFields, operation));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");

        SourceRecord originalRecord = originalBuilder.schemaless(keyTransform);
        SourceRecord expectedRecord = expectedBuilder.schemaless(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void withSchema(String description,
                           boolean keyTransform,
                           RecordBuilder originalBuilder,
                           List<String> transformFields, List<String> headers1, HeaderFrom.Operation operation,
                           RecordBuilder expectedBuilder) {
        HeaderFrom<SourceRecord> xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();
        xform.configure(config(headers1, transformFields, operation));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");
        Headers expect = headers.duplicate();
        for (int i = 0; i < headers1.size(); i++) {
            expect.add(headers1.get(i), originalBuilder.fieldValues.get(i), originalBuilder.fieldSchemas.get(i));
        }

        SourceRecord originalRecord = originalBuilder.withSchema(keyTransform);
        SourceRecord expectedRecord = expectedBuilder.withSchema(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigExtraHeaderConfig(boolean keyTransform) {
        Map<String, Object> config = config(singletonList("foo"), asList("foo", "bar"), HeaderFrom.Operation.COPY);
        HeaderFrom<?> xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigExtraFieldConfig(boolean keyTransform) {
        Map<String, Object> config = config(asList("foo", "bar"), singletonList("foo"), HeaderFrom.Operation.COPY);
        HeaderFrom<?> xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigEmptyHeadersAndFieldsConfig(boolean keyTransform) {
        Map<String, Object> config = config(emptyList(), emptyList(), HeaderFrom.Operation.COPY);
        HeaderFrom<?> xform = keyTransform ? new HeaderFrom.Key<>() : new HeaderFrom.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
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

