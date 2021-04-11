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
import org.apache.kafka.connect.sink.SinkRecord;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class HeaderToTest {

    private HeaderTo<SinkRecord> toKey = new HeaderTo.Key<>();
    private HeaderTo<SinkRecord> toValue = new HeaderTo.Value<>();

    private static final Schema VALUE_SCHEMA = SchemaBuilder
            .struct()
            .name("HeaderToTestValue")
            .version(1)
            .field("value_field1", Schema.STRING_SCHEMA)
            .field("value_field2", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder
            .struct()
            .name("HeaderToTestKey")
            .version(1)
            .field("key_field1", Schema.STRING_SCHEMA)
            .field("key_field2", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private Struct testStruct(Object struct) {
        return requireStructOrNull(struct, "test purpose");
    }

    private SinkRecord testRecord(String valueField1, String keyField1, Map<String, String> headerMap) {
        Struct key = new Struct(KEY_SCHEMA);
        Struct value = new Struct(VALUE_SCHEMA);
        key.put("key_field1", keyField1);
        value.put("value_field1", valueField1);

        ConnectHeaders headers = new ConnectHeaders();
        headerMap.forEach((headerKey, headerValue) ->
                headers.add(headerKey, new SchemaAndValue(Schema.STRING_SCHEMA, headerValue))
        );

        return new SinkRecord(
                "test-topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L, 0L, NO_TIMESTAMP_TYPE, headers
        );
    }

    @Before
    public void setUp() {
        toKey = new HeaderTo.Key<>();
        toValue = new HeaderTo.Value<>();
    }

    @After
    public void tearDown() {
        toKey.close();
        toValue.close();
    }

    @Test
    public void createValueFieldsFromHeadersTest() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("headers", "header1");
        configMap.put("fields", "new_field1");

        toValue.configure(configMap);
        SinkRecord record = testRecord("test-key", "test-value", Collections.singletonMap("header1", "data"));

        SinkRecord result = toValue.apply(record);

        Assert.assertEquals("Expected an additional value field", 3, result.valueSchema().fields().size());
        Assert.assertEquals(
                "Expected the additional value field to have a specific data type",
                Schema.STRING_SCHEMA, requireStructOrNull(result.value(), "test").schema().field("new_field1").schema()
        );
        Assert.assertEquals(
                "Expected the additional value field to have a specific value",
                "data", testStruct(result.value()).get("new_field1").toString()
        );
        Assert.assertEquals(
                "Default operation being copy, header1 was still expected in the headers",
                "data", result.headers().lastWithName("header1").value().toString()
        );
    }

    @Test
    public void createKeyFieldsFromHeadersTest() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("headers", "header2");
        configMap.put("fields", "new_field2");

        toKey.configure(configMap);
        SinkRecord record = testRecord("test-key", "test-value", Collections.singletonMap("header2", "bladibla"));

        SinkRecord result = toKey.apply(record);

        Assert.assertEquals(
                "Expected the additional value field to have a specific value",
                "bladibla", testStruct(result.key()).get("new_field2").toString()
        );
    }

    @Test
    public void createFieldsWithTheMoveOperator() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("headers", "header1");
        configMap.put("fields", "new_field1");
        configMap.put("operation", "move");

        toValue.configure(configMap);
        SinkRecord record = testRecord("test-key", "test-value", Collections.singletonMap("header1", "data"));

        SinkRecord result = toValue.apply(record);

        Assert.assertNull(result.headers().lastWithName("header1"));

        Assert.assertEquals(
                "Expected the additional value field to have a specific value",
                "data", testStruct(result.value()).get("new_field1").toString()
        );
    }

    @Test(expected = ConfigException.class)
    public void createValuesFromImbalancedSourceHeaders() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1, value_field2, value_field3");
        configMap.put("headers", "header1");

        toValue.configure(configMap);
    }

    @Test(expected = ConfigException.class)
    public void createValuesFromAnInvalidOperation() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1");
        configMap.put("headers", "new_header1");
        configMap.put("operation", "weirdCopyPaste");

        toValue.configure(configMap);
    }

    @Test(expected = ConfigException.class)
    public void createValuesFromAnInvalidConfigList() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "field1;field2;field3;field4");
        configMap.put("headers", "header1;header2;header3;header4");

        toKey.configure(configMap);
    }

    @Test
    public void hasAWellSpecifiedConfigurationDefTest() {
        assertThat(
                Arrays.asList("fields", "headers", "operation").toArray(),
                IsEqual.equalTo(toValue.config().configKeys().keySet().toArray())

        );
    }

    @Test
    public void performNoModificationWithOutSchemaTest() {
        final Map<String, Object> keyConfigMap = new HashMap<>();
        keyConfigMap.put("fields", "key_field1, key_field2, key_field1");
        keyConfigMap.put("headers", "header1, header2, header3");

        toKey.configure(keyConfigMap);

        final Map<String, Object> valueConfigMap = new HashMap<>();
        valueConfigMap.put("fields", "value_field1");
        valueConfigMap.put("headers", "new_header");

        toValue.configure(valueConfigMap);

        SinkRecord record = new SinkRecord("test-topic", 0, null, emptyMap(), null, emptyMap(), 0L);

        SinkRecord result = toValue.apply(toKey.apply(record));

        Assert.assertEquals(record, result);
    }

    @Test
    public void performNoModificationOnNullRecordsTest() {
        final Map<String, Object> keyConfigMap = new HashMap<>();
        keyConfigMap.put("fields", "key_field1, key_field2, key_field1");
        keyConfigMap.put("headers", "header1, header2, header3");

        toKey.configure(keyConfigMap);

        final Map<String, Object> valueConfigMap = new HashMap<>();
        valueConfigMap.put("fields", "value_field1");
        valueConfigMap.put("headers", "header5");

        toValue.configure(valueConfigMap);

        SinkRecord record = null;

        SinkRecord result = toValue.apply(toKey.apply(record));

        Assert.assertNull(result);
    }
}