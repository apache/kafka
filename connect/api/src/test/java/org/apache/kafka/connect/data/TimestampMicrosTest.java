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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampMicrosTest {
    private static final long MILLIS = 1621543687123L;
    private static final long MICROS = MILLIS * 1000 + 456;
    private static final Instant INSTANT = Instant.ofEpochSecond(MILLIS / 1000, (MILLIS % 1000) * 1_000_000 + 456_000);

    @Test
    public void testBuilder() {
        Schema schema = TimestampMicros.builder().build();
        assertEquals(TimestampMicros.LOGICAL_NAME, schema.name());
        assertEquals(Schema.Type.INT64, schema.type());
    }

    @Test
    public void testFromLogical() {
        assertEquals(MICROS, TimestampMicros.fromLogical(TimestampMicros.SCHEMA, INSTANT));
    }

    @Test
    public void testToLogical() {
        assertEquals(INSTANT, TimestampMicros.toLogical(TimestampMicros.SCHEMA, MICROS));
    }

    @Test
    public void testInvalidSchema() {
        Schema wrongSchema = SchemaBuilder.int64().name("not-timestamp-micros").build();
        assertThrows(DataException.class, () -> TimestampMicros.fromLogical(wrongSchema, INSTANT));
        assertThrows(DataException.class, () -> TimestampMicros.toLogical(wrongSchema, MICROS));
    }

    @Test
    public void testConversionBetweenMillisAndMicros() {
        assertEquals(MILLIS * 1000, TimestampMicros.fromMillis(MILLIS));
        assertEquals(MILLIS, TimestampMicros.toMillis(MILLIS * 1000));

        // Test precision loss during conversion
        long originalMicros = MILLIS * 1000 + 456;
        long millis = TimestampMicros.toMillis(originalMicros);
        long reconvertedMicros = TimestampMicros.fromMillis(millis);
        assertEquals(MILLIS * 1000, reconvertedMicros); // should lose the microsecond part
    }
}
