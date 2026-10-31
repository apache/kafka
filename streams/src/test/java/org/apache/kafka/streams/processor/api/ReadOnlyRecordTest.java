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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReadOnlyRecordTest {

    private static Headers headers() {
        return new RecordHeaders().add(new RecordHeader("k", "v".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void recordShouldBeAssignableToReadOnlyRecordAndExposeSameAccessors() {
        final Headers headers = headers();
        final Record<String, String> record = new Record<>("key", "value", 123L, headers);

        // The assignment itself proves Record is a ReadOnlyRecord (source/binary compatible change).
        final ReadOnlyRecord<String, String> readOnly = record;

        assertEquals("key", readOnly.key());
        assertEquals("value", readOnly.value());
        assertEquals(123L, readOnly.timestamp());
        assertEquals(headers, readOnly.headers());
    }

    @Test
    public void readOnlyHeadersShouldBeNonNullAndEmptyWhenConstructedWithoutHeaders() {
        final ReadOnlyRecord<String, String> readOnly = new Record<>("key", "value", 123L);
        assertEquals(new RecordHeaders(), readOnly.headers());
    }

    @Test
    public void readOnlyAccessorsShouldTolerateNullKeyAndValue() {
        final ReadOnlyRecord<String, String> readOnly = new Record<>(null, null, 0L);
        assertNull(readOnly.key());
        assertNull(readOnly.value());
        assertEquals(0L, readOnly.timestamp());
        assertEquals(new RecordHeaders(), readOnly.headers());
    }
}
