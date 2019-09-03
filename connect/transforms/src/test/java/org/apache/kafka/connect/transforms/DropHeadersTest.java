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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DropHeadersTest {
    private DropHeaders<SourceRecord> xform = new DropHeaders<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void dropAllHeaders() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("dummy header"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("dummy header", "dummy value");
        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(0, transformedRecord.headers().size());
    }

    @Test
    public void dropOneHeaderOutOfTwo() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("dummy header 2"));

        xform.configure(props);

        Headers headers = new ConnectHeaders();
        headers.addString("dummy header", "dummy value");
        headers.addString("dummy header 2", "dummy value 2");

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, headers);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(1, transformedRecord.headers().size());

        Headers expected = new ConnectHeaders().addString("dummy header", "dummy value");
        assertEquals(expected, transformedRecord.headers());
    }

    @Test(expected = DataException.class)
    public void dropOneHeaderOutOfZero() {
        final Map<String, Object> props = new HashMap<>();
        props.put("names", Collections.singletonList("dummy header 2"));

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);
    }

}
