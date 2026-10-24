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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VersionedRecordWithHeadersTest {

    @Test
    public void shouldCreateRecordWithDefaultEmptyHeaders() {
        final VersionedRecord<String> record = new VersionedRecord<>("value", 42L);

        assertEquals("value", record.value());
        assertEquals(42L, record.timestamp());
        assertFalse(record.validTo().isPresent());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
    }

    @Test
    public void shouldCreateRecordWithValidToAndDefaultEmptyHeaders() {
        final VersionedRecord<String> record = new VersionedRecord<>("value", 42L, 100L);

        assertEquals("value", record.value());
        assertEquals(42L, record.timestamp());
        assertTrue(record.validTo().isPresent());
        assertEquals(100L, record.validTo().get());
        assertNotNull(record.headers());
        assertFalse(record.headers().iterator().hasNext());
    }

    @Test
    public void shouldCreateRecordWithExplicitHeaders() {
        final Headers headers = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("key1", "val1".getBytes(StandardCharsets.UTF_8))
        });
        final VersionedRecord<String> record = new VersionedRecord<>("value", 42L, headers);

        assertEquals("value", record.value());
        assertEquals(42L, record.timestamp());
        assertFalse(record.validTo().isPresent());
        assertEquals(headers, record.headers());
        assertEquals("val1", new String(record.headers().lastHeader("key1").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldCreateRecordWithValidToAndExplicitHeaders() {
        final Headers headers = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("traceId", "abc".getBytes(StandardCharsets.UTF_8))
        });
        final VersionedRecord<String> record = new VersionedRecord<>("value", 42L, 100L, headers);

        assertEquals("value", record.value());
        assertEquals(42L, record.timestamp());
        assertTrue(record.validTo().isPresent());
        assertEquals(100L, record.validTo().get());
        assertEquals(headers, record.headers());
    }

    @Test
    public void shouldRejectNullValue() {
        assertThrows(NullPointerException.class, () -> new VersionedRecord<>(null, 42L));
    }

    @Test
    public void shouldRejectNullHeaders() {
        assertThrows(NullPointerException.class, () -> new VersionedRecord<>("value", 42L, null));
    }

    @Test
    public void shouldRejectNullHeadersWithValidTo() {
        assertThrows(NullPointerException.class, () -> new VersionedRecord<>("value", 42L, 100L, null));
    }

    @Test
    public void shouldNotBeEqualForRecordsWithDifferentHeaders() {
        final Headers h1 = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("k", "v1".getBytes(StandardCharsets.UTF_8))
        });
        final Headers h2 = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("k", "v2".getBytes(StandardCharsets.UTF_8))
        });
        final VersionedRecord<String> r1 = new VersionedRecord<>("val", 10L, h1);
        final VersionedRecord<String> r2 = new VersionedRecord<>("val", 10L, h2);

        assertNotEquals(r1, r2);
    }

    @Test
    public void shouldBeEqualForRecordsWithSameHeaders() {
        final Headers h1 = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("k", "v1".getBytes(StandardCharsets.UTF_8))
        });
        final Headers h2 = new RecordHeaders(new RecordHeader[]{
            new RecordHeader("k", "v1".getBytes(StandardCharsets.UTF_8))
        });
        final VersionedRecord<String> r1 = new VersionedRecord<>("val", 10L, h1);
        final VersionedRecord<String> r2 = new VersionedRecord<>("val", 10L, h2);

        assertEquals(r1, r2);
        assertEquals(r1.hashCode(), r2.hashCode());
    }

    @Test
    public void shouldReturnCorrectToString() {
        final VersionedRecord<String> record = new VersionedRecord<>("value", 42L);
        final String str = record.toString();
        assertTrue(str.startsWith("<value,42,Optional.empty,"));
        assertTrue(str.contains("RecordHeaders"));
    }
}
