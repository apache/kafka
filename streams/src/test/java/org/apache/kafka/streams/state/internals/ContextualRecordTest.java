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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ContextualRecordTest {

    @Test
    public void testEquals() {
        final byte[] value1 = {1, 2, 3};
        final byte[] value2 = {1, 2, 3};
        final long timestamp = Time.SYSTEM.milliseconds();
        final ProcessorRecordContext context1 = new ProcessorRecordContext(timestamp, 12345L, 0, "test-topic", new RecordHeaders());
        final ProcessorRecordContext context2 = new ProcessorRecordContext(timestamp, 12345L, 0, "test-topic", new RecordHeaders());

        final ContextualRecord record1 = new ContextualRecord(value1, context1);
        final ContextualRecord record2 = new ContextualRecord(value2, context2);

        assertEquals(record1, record2);
    }

    @Test
    public void testNotEqualsDifferentContent() {
        final byte[] value1 = {1, 2, 3};
        final byte[] value2 = {4, 5, 6};
        final long timestamp = Time.SYSTEM.milliseconds();
        final ProcessorRecordContext context1 = new ProcessorRecordContext(timestamp, 12345L, 0, "test-topic", new RecordHeaders());
        final ProcessorRecordContext context2 = new ProcessorRecordContext(timestamp, 12345L, 0, "test-topic", new RecordHeaders());

        final ContextualRecord record1 = new ContextualRecord(value1, context1);
        final ContextualRecord record2 = new ContextualRecord(value2, context2);

        assertNotEquals(record1, record2);
    }

    @Test
    public void testEqualsSameInstance() {
        final byte[] value = {1, 2, 3};
        final ProcessorRecordContext context = new ProcessorRecordContext(Time.SYSTEM.milliseconds(), 12345L, 0, "test-topic", new RecordHeaders());

        final ContextualRecord record = new ContextualRecord(value, context);

        // Test equals method for same instance
        assertEquals(record, record);
    }

    @Test
    public void testHashCodeThrowsException() {
        final byte[] value = {1, 2, 3};
        final ProcessorRecordContext context = new ProcessorRecordContext(Time.SYSTEM.milliseconds(), 12345L, 0, "test-topic", new RecordHeaders());
        final ContextualRecord record = new ContextualRecord(value, context);

        assertThrows(UnsupportedOperationException.class, record::hashCode);
    }
}
