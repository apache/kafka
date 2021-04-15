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
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class HeaderToTest {
    private HeaderTo<SourceRecord> xform = new HeaderTo.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithEmptyHeadersConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "");
        props.put("fields", "AAA");

        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithBlankHeadersConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "    ");
        props.put("fields", "AAA");

        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithEmptyFieldsConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "AAA");
        props.put("fields", "");

        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithBlankFieldsConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "AAA");
        props.put("fields", "    ");

        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithInvalidOperationConfig() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "AAA");
        props.put("fields", "AAA");
        props.put("operation", "AAA");

        xform.configure(props);
    }

    @Test
    public void insertHeaderAsFieldWithCopy() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "header");
        props.put("fields", "field");

        xform.configure(props);

        final Headers headers = new ConnectHeaders().add("header", "value", null);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        assertNotNull(transformedRecord.value());

        final Map<String, Object> expectedValue = new HashMap<>();
        expectedValue.put("field", "value");

        final Map<String, Object> actualValue = requireMap(transformedRecord.value(), "");

        assertEquals(1, actualValue.size());
        assertEquals(expectedValue, actualValue);
        assertEquals(headers, transformedRecord.headers());
    }

    @Test
    public void insertHeaderAsFieldWithMove() {
        final Map<String, Object> props = new HashMap<>();
        props.put("headers", "header");
        props.put("fields", "field");
        props.put("operation", "move");

        xform.configure(props);

        final Headers headers = new ConnectHeaders().add("header", "value", null);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        assertNotNull(transformedRecord.value());

        final Map<String, Object> expectedValue = new HashMap<>();
        expectedValue.put("field", "value");

        final Map<String, Object> actualValue = requireMap(transformedRecord.value(), "");

        final Headers expectedHeaders = new ConnectHeaders();

        assertEquals(1, actualValue.size());
        assertEquals(expectedValue, actualValue);
        assertEquals(expectedHeaders, transformedRecord.headers());
    }
}
