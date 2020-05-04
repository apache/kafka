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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class HeaderFromTest {

    private HeaderFrom<SourceRecord> fromKey = new HeaderFrom.Key<>();
    private HeaderFrom<SourceRecord> fromValue = new HeaderFrom.Value<>();

    private static final Schema VALUE_SCHEMA = SchemaBuilder
            .struct()
            .name("HeaderFromTestValue")
            .version(1)
            .field("value_field1", Schema.STRING_SCHEMA)
            .field("value_field2", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder
            .struct()
            .name("HeaderFromTestKey")
            .version(1)
            .field("key_field1", Schema.INT64_SCHEMA)
            .field("key_field2", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private Struct testStruct(Object struct) {
        return requireStructOrNull(struct, "test purpose");
    }

    private SourceRecord testRecord(Long keyField1, String valueField1, Map<String, String> headerMap) {
        Struct key = new Struct(KEY_SCHEMA);
        Struct value = new Struct(VALUE_SCHEMA);
        key.put("key_field1", keyField1);
        value.put("value_field1", valueField1);

        ConnectHeaders headers = new ConnectHeaders();
        headerMap.forEach((headerKey, headerValue) ->
                headers.add(headerKey, new SchemaAndValue(Schema.STRING_SCHEMA, headerValue))
        );

        return new SourceRecord(
                null, null, "test-topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L, headers
        );
    }

    private static Stream<Header> allHeadersAsStream(Headers headers, String headerName) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(headers.allWithName(headerName), 0), false);
    }

    @Before
    public void setUp() {
        fromKey = new HeaderFrom.Key<>();
        fromValue = new HeaderFrom.Value<>();
    }

    @After
    public void tearDown() {
        fromKey.close();
        fromValue.close();
    }

    @Test
    public void createHeadersFromValueFieldTest() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1");
        configMap.put("headers", "new_header");

        fromValue.configure(configMap);
        SourceRecord record = testRecord(-1L, "test-value", Collections.singletonMap("old_header", "mutmut"));

        SourceRecord resultRecord = fromValue.apply(record);

        Assert.assertEquals("Expected an additional header", 2, resultRecord.headers().size());

        Assert.assertEquals(
                "Expected the additional header to have a specific value",
                "test-value", resultRecord.headers().lastWithName("new_header").value()
        );
        Assert.assertEquals(
                "Default operation being copy, value_field1 was still expected in the value",
                "test-value", testStruct(resultRecord.value()).get("value_field1").toString()
        );
    }

    @Test
    public void createHeadersFromKeyFieldTest() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "key_field1");
        configMap.put("headers", "new_header");

        fromKey.configure(configMap);
        SourceRecord record = testRecord(42L, "test-value", Collections.singletonMap("old_header", "mutmut"));

        SourceRecord resultRecord = fromKey.apply(record);

        Assert.assertEquals(
                "Expected the additional header to have a specific value",
                42L, resultRecord.headers().lastWithName("new_header").value()
        );
    }

    @Test(expected = DataException.class)
    public void createHeadersWithMoveOperation() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1");
        configMap.put("headers", "new_header");
        configMap.put("operation", "move");

        fromValue.configure(configMap);
        SourceRecord record = testRecord(0L, "test-bladibla", Collections.singletonMap("old_header", "mutmut"));

        SourceRecord resultRecord = fromValue.apply(record);

        testStruct(resultRecord.value()).get("value_field1");
    }

    @Test(expected = ConfigException.class)
    public void createHeadersFromImbalancedSourceFields() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1");
        configMap.put("headers", "header1, header2, header3, header4");

        fromValue.configure(configMap);
    }

    @Test(expected = ConfigException.class)
    public void createHeadersFromAnInvalidOperation() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "value_field1");
        configMap.put("headers", "new_header1");
        configMap.put("operation", "weirdCopyPaste");

        fromValue.configure(configMap);
    }

    @Test(expected = ConfigException.class)
    public void createHeadersFromAnInvalidConfigList() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("fields", "field1/field2/field3/field4");
        configMap.put("headers", "header1/header2/header3/header4");

        fromValue.configure(configMap);
    }

    @Test
    public void hasAWellSpecifiedConfigurationDefTest() {
        assertThat(
                Arrays.asList("fields", "headers", "operation").toArray(),
                IsEqual.equalTo(fromValue.config().configKeys().keySet().toArray())

        );
    }

    @Test
    public void createHeadersWithMultipleFieldsFromKeyAndValueTest() {
        final Map<String, Object> keyConfigMap = new HashMap<>();
        keyConfigMap.put("fields", "key_field1, key_field2, key_field1");
        keyConfigMap.put("headers", "old_header, new_header1, other_header");

        fromKey.configure(keyConfigMap);

        final Map<String, Object> valueConfigMap = new HashMap<>();
        valueConfigMap.put("fields", "value_field1, value_field2, value_field1");
        valueConfigMap.put("headers", "old_header, new_header2, other_header");

        fromValue.configure(valueConfigMap);

        SourceRecord record = testRecord(-1L, "test-value", Collections.singletonMap("old_header", "mutmut"));

        SourceRecord resultRecord = fromValue.apply(fromKey.apply(record));

        assertThat(
                allHeadersAsStream(resultRecord.headers(), "old_header").map(Header::value).toArray(),
                IsEqual.equalTo(Stream.of("mutmut", -1L, "test-value").toArray())
        );

        assertThat(
                allHeadersAsStream(resultRecord.headers(), "new_header1").map(Header::value).toArray(),
                IsEqual.equalTo(Stream.of((Object) null).toArray())
        );

        assertThat(
                allHeadersAsStream(resultRecord.headers(), "new_header2").map(Header::value).toArray(),
                IsEqual.equalTo(Stream.of((Object) null).toArray())
        );

        assertThat(
                allHeadersAsStream(resultRecord.headers(), "other_header").map(Header::value).toArray(),
                IsEqual.equalTo(Stream.of(-1L, "test-value").toArray())
        );
    }

    @Test
    public void performNoModificationWithOutSchemaTest() {
        final Map<String, Object> keyConfigMap = new HashMap<>();
        keyConfigMap.put("fields", "key_field1, key_field2, key_field1");
        keyConfigMap.put("headers", "old_header, new_header1, other_header");

        fromKey.configure(keyConfigMap);

        final Map<String, Object> valueConfigMap = new HashMap<>();
        valueConfigMap.put("fields", "value_field1");
        valueConfigMap.put("headers", "new_header");

        fromValue.configure(valueConfigMap);

        SourceRecord record = new SourceRecord(null, null, "test-topic", null, emptyMap(), null, emptyMap());

        SourceRecord result = fromValue.apply(fromKey.apply(record));

        Assert.assertEquals(record, result);
    }

    @Test
    public void performNoModificationOnNullRecordsTest() {
        final Map<String, Object> keyConfigMap = new HashMap<>();
        keyConfigMap.put("fields", "key_field1, key_field2, key_field1");
        keyConfigMap.put("headers", "old_header, new_header1, other_header");

        fromKey.configure(keyConfigMap);

        final Map<String, Object> valueConfigMap = new HashMap<>();
        valueConfigMap.put("fields", "value_field1");
        valueConfigMap.put("headers", "new_header");

        fromValue.configure(valueConfigMap);

        SourceRecord record = null;

        SourceRecord result = fromValue.apply(fromKey.apply(record));

        Assert.assertNull(result);
    }
}