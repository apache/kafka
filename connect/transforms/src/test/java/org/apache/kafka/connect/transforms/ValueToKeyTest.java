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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValueToKeyTest {
    private final ValueToKey<SinkRecord> xform = new ValueToKey<>();

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, 42)
        );
    }

    @AfterEach
    void teardown() {
        xform.close();
    }

    @Test
    void schemaless() {
        xform.configure(Collections.singletonMap("fields", "a,b"));

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("a", 1);
        value.put("b", 2);
        value.put("c", 3);

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final HashMap<String, Integer> expectedKey = new HashMap<>();
        expectedKey.put("a", 1);
        expectedKey.put("b", 2);

        assertNull(transformedRecord.keySchema());
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    void withSchema() {
        xform.configure(Collections.singletonMap("fields", "a,b"));

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);
        value.put("b", 2);
        value.put("c", 3);

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();

        final Struct expectedKey = new Struct(expectedKeySchema)
                .put("a", 1)
                .put("b", 2);

        assertEquals(expectedKeySchema, transformedRecord.keySchema());
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    void nonExistingField() {
        xform.configure(Collections.singletonMap("fields", "not_exist"));

        final Schema valueSchema = SchemaBuilder.struct()
            .field("a", Schema.INT32_SCHEMA)
            .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);

        DataException actual = assertThrows(DataException.class, () -> xform.apply(sinkRecord));
        assertEquals("Field does not exist: not_exist", actual.getMessage());
    }

    @Test
    void testValueToKeyVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }

    @ParameterizedTest
    @MethodSource("data")
    void testReplaceNullWithDefaultConfig(boolean replaceNullWithDefault, Object expectedValue) {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "optional_with_default");
        config.put("replace.null.with.default", replaceNullWithDefault);
        xform.configure(config);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("optional_with_default", SchemaBuilder.int32().optional().defaultValue(42).build())
                .build();
        final Struct value = new Struct(valueSchema).put("optional_with_default", null);

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertEquals(expectedValue, ((Struct) transformedRecord.key()).getWithoutDefault("optional_with_default"));
    }

    @Test
    void schemalessNestedFieldV2() {
        Map<String, String> configs = new HashMap<>();
        configs.put("fields", "name,address.city");
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        xform.configure(configs);

        final Map<String, Object> address = new HashMap<>();
        address.put("city", "New York");
        address.put("state", "NY");
        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Franz");
        value.put("address", address);

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertNull(transformedRecord.keySchema());

        final HashMap<String, String> expectedKey = new HashMap<>();
        expectedKey.put("name", "Franz");
        expectedKey.put("address.city", "New York");

        @SuppressWarnings("unchecked")
        final Map<String, Object> key = (Map<String, Object>) transformedRecord.key();
        assertEquals(expectedKey, key);
    }

    @Test
    void withSchemaNestedFieldV2() {
        Map<String, String> configs = new HashMap<>();
        configs.put("fields", "name,address.city");
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        xform.configure(configs);

        final Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("state", Schema.STRING_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("name", "Franz")
                .put("address", new Struct(addressSchema)
                        .put("city", "New York")
                        .put("state", "NY"));

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address.city", Schema.STRING_SCHEMA)
                .build();

        assertEquals(expectedKeySchema, transformedRecord.keySchema());
        assertEquals("Franz", ((Struct) transformedRecord.key()).get("name"));
        assertEquals("New York", ((Struct) transformedRecord.key()).get("address.city"));
    }

    @Test
    void withSchemaMultipleNestedFieldsV2() {
        Map<String, String> configs = new HashMap<>();
        configs.put("fields", "address.city,address.state");
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        xform.configure(configs);

        final Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("state", Schema.STRING_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("name", "Franz")
                .put("address", new Struct(addressSchema)
                        .put("city", "New York")
                        .put("state", "NY"));

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("address.city", Schema.STRING_SCHEMA)
                .field("address.state", Schema.STRING_SCHEMA)
                .build();

        assertEquals(expectedKeySchema, transformedRecord.keySchema());
        final Struct key = (Struct) transformedRecord.key();
        assertEquals("New York", key.get("address.city"));
        assertEquals("NY", key.get("address.state"));
    }

    @Test
    void nonExistentNestedFieldWithSchemaV2() {
        Map<String, String> configs = new HashMap<>();
        configs.put("fields", "address.zip");
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        xform.configure(configs);

        final Schema addressSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("address", addressSchema)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("address", new Struct(addressSchema).put("city", "New York"));

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);

        DataException actual = assertThrows(DataException.class, () -> xform.apply(sinkRecord));
        assertEquals("Field does not exist: address.zip", actual.getMessage());
    }

    @Test
    void schemalessNestedFieldNotFoundV2() {
        Map<String, String> configs = new HashMap<>();
        configs.put("fields", "address.zip");
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        xform.configure(configs);

        final Map<String, Object> value = new HashMap<>();
        value.put("address", Collections.singletonMap("city", "New York"));

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertNull(transformedRecord.keySchema());
        @SuppressWarnings("unchecked")
        final Map<String, Object> key = (Map<String, Object>) transformedRecord.key();
        assertNull(key.get("address.zip"));
    }

    @Test
    void v1BackwardCompatibilityDottedFieldName() {
        // With V1 (default), a dotted field name is treated as a literal field name
        xform.configure(Collections.singletonMap("fields", "address.city"));

        final Schema valueSchema = SchemaBuilder.struct()
                .field("address.city", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema).put("address.city", "New York");

        final SinkRecord sinkRecord = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("address.city", Schema.STRING_SCHEMA)
                .build();

        assertEquals(expectedKeySchema, transformedRecord.keySchema());
        assertEquals("New York", ((Struct) transformedRecord.key()).get("address.city"));
    }
}
