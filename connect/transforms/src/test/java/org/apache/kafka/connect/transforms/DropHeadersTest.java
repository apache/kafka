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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DropHeadersTest {
    private DropHeaders<SourceRecord> xform = new DropHeaders<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithEmptyHeaderNames() {
        Map<String, String> props = new HashMap<>();
        props.put("names", "");
        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithBlankHeaderNames() {
        Map<String, String> props = new HashMap<>();
        props.put("names", "a, ");
        xform.configure(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailWithBlankHeaderNameAndValidHeaderNames() {
        Map<String, String> props = new HashMap<>();
        props.put("names", "a,,d");
        xform.configure(props);
    }

    @Test
    public void dropAllHeaders() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("AAA"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("AAA", "dummy value");
        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(0, transformedRecord.headers().size());
    }

    @Test
    public void dropOneHeaderOutOfTwo() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("BBB"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("AAA", "dummy value");
        headers.addString("BBB", "dummy value");

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addString("AAA", "dummy value");
        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void dropOneHeaderOutOfZero() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("BBB"));

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(0, transformedRecord.headers().size());
    }

    @Test
    public void dropHeadersOnNullRecord() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("AAA"));

        xform.configure(props);

        final SourceRecord transformedRecord = xform.apply(null);

        assertNull(transformedRecord);
    }

    @Test
    public void dropOneHeaderHasNoMatch() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("AAA"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("BBB", "dummy value");
        headers.addString("CCC", "dummy value");

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addString("BBB", "dummy value")
            .addString("CCC", "dummy value");

        assertEquals(2, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void dropSeveralHeaders() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Arrays.asList("AAA", "BBB"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("AAA", "dummy value")
            .addString("BBB", "dummy value")
            .addString("AAA", "dummy value")
            .addString("CCC", "dummy value")
            .addString("DDD", "dummy value")
            .addString("DDD", "dummy value");
        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addString("CCC", "dummy value")
            .addString("DDD", "dummy value")
            .addString("DDD", "dummy value");

        assertEquals(3, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

}
