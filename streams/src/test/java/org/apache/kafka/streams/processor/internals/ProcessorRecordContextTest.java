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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProcessorRecordContextTest {
    // timestamp + offset + partition: 8 + 8 + 4
    private final static long MIN_SIZE = 20L;

    @Test
    public void shouldEstimateNullTopicAndNullHeadersAsZeroLength() {
        final Headers headers = new RecordHeaders();
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            null
        );

        assertEquals(MIN_SIZE, context.sizeBytes());
    }

    @Test
    public void shouldEstimateEmptyHeaderAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.sizeBytes());
    }

    @Test
    public void shouldEstimateTopicLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            null
        );

        assertEquals(MIN_SIZE + 5L, context.sizeBytes());
    }

    @Test
    public void shouldEstimateHeadersLength() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L + 12L, context.sizeBytes());
    }

    @Test
    public void shouldEstimateNullValueInHeaderAsZero() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", null);
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L, context.sizeBytes());
    }
}
