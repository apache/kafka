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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LRUCacheEntryTest {

    private static final byte[] VALUE = new byte[] {1, 2, 3};

    private static LRUCacheEntry entry(final byte[] rawKey, final byte[] rawValue) {
        return new LRUCacheEntry(VALUE, new RecordHeaders(), true, 1L, 1L, 0, "topic", rawKey, rawValue);
    }

    @Test
    public void shouldCountSourceRawKeyAndValueInSize() {
        final long withoutRawRecord = entry(null, null).size();

        assertEquals(withoutRawRecord + 10, entry(new byte[10], null).size());
        assertEquals(withoutRawRecord + 40_000, entry(null, new byte[40_000]).size());
        assertEquals(withoutRawRecord + 40_010, entry(new byte[10], new byte[40_000]).size());
    }
}
