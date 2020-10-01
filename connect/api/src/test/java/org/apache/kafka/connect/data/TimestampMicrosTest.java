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
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;

public class TimestampMicrosTest {

    @Test
    public void testBuilder() {
        Schema plain = TimestampMicros.SCHEMA;
        assertEquals(TimestampMicros.LOGICAL_NAME, plain.name());
        assertEquals(1, (Object) plain.version());
    }

    @Test
    public void testFromLogical() {
        assertEquals(0L, TimestampMicros.fromLogical(TimestampMicros.SCHEMA, Instant.EPOCH));
        assertEquals(100000, TimestampMicros.fromLogical(TimestampMicros.SCHEMA, Instant.EPOCH.plus(100000, ChronoUnit.MICROS)));
    }

    @Test(expected = DataException.class)
    public void testFromLogicalInvalidSchema() {
        TimestampMicros.fromLogical(TimestampMicros.builder().name("invalid").build(), Instant.EPOCH);
    }

    @Test
    public void testToLogical() {
        assertEquals(Instant.EPOCH, TimestampMicros.toLogical(TimestampMicros.SCHEMA, 0L));
        assertEquals(Instant.EPOCH.plus(100000, ChronoUnit.MICROS), TimestampMicros.toLogical(TimestampMicros.SCHEMA, 100000));
    }

    @Test(expected = DataException.class)
    public void testToLogicalInvalidSchema() {
        TimestampMicros.toLogical(Date.builder().name("invalid").build(), 0);
    }

}
