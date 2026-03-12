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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for time index.
 */
public class TimeIndexTest {
    private final int maxEntries = 30;
    private final long baseOffset = 45L;
    private final TimeIndex idx = assertDoesNotThrow(() -> new TimeIndex(nonExistentTempFile(), baseOffset, maxEntries * 12));

    @AfterEach
    public void teardown() {
        if (this.idx != null)
            this.idx.file().delete();
    }

    @Test
    public void testLookUp() {
        // Empty time index
        assertEquals(new TimestampOffset(-1L, baseOffset), idx.lookup(100L));

        // Add several time index entries.
        appendEntries(maxEntries - 1);

        // look for timestamp smaller than the earliest entry
        assertEquals(new TimestampOffset(-1L, baseOffset), idx.lookup(9));
        // look for timestamp in the middle of two entries.
        assertEquals(new TimestampOffset(20L, 65L), idx.lookup(25));
        // look for timestamp same as the one in the entry
        assertEquals(new TimestampOffset(30L, 75L), idx.lookup(30));
    }

    @Test
    public void testEntry() {
        appendEntries(maxEntries - 1);
        assertEquals(new TimestampOffset(10L, 55L), idx.entry(0));
        assertEquals(new TimestampOffset(20L, 65L), idx.entry(1));
        assertEquals(new TimestampOffset(30L, 75L), idx.entry(2));
        assertEquals(new TimestampOffset(40L, 85L), idx.entry(3));
    }

    @Test
    public void testEntryOverflow() {
        assertThrows(IllegalArgumentException.class, () -> idx.entry(0));
    }

    @Test
    public void testTruncate() {
        appendEntries(maxEntries - 1);
        idx.truncate();
        assertEquals(0, idx.entries());

        appendEntries(maxEntries - 1);
        idx.truncateTo(10 + baseOffset);
        assertEquals(0, idx.entries());
    }

    @Test
    public void testAppend() {
        appendEntries(maxEntries - 1);
        assertThrows(IllegalArgumentException.class, () -> idx.maybeAppend(10000L, 1000L));
        assertThrows(InvalidOffsetException.class, () -> idx.maybeAppend(10000L, (maxEntries - 2) * 10, true));
        idx.maybeAppend(10000L, 1000L, true);
    }

    @Test
    public void testSanityCheck() throws IOException {
        idx.sanityCheck();
        appendEntries(5);
        TimestampOffset firstEntry = idx.entry(0);
        idx.sanityCheck();
        idx.close();

        class MockTimeIndex extends TimeIndex {
            boolean shouldCorruptOffset = false;
            boolean shouldCorruptTimestamp = false;
            boolean shouldCorruptLength = false;

            MockTimeIndex(File file, long baseOffset, int maxEntries) throws IOException {
                super(file, baseOffset, maxEntries);
            }

            @Override
            public TimestampOffset lastEntry() {
                TimestampOffset superLastEntry = super.lastEntry();
                long offset = shouldCorruptOffset ? this.baseOffset() - 1 : superLastEntry.offset();
                long timestamp = shouldCorruptTimestamp ? firstEntry.timestamp() - 1 : superLastEntry.timestamp();
                return new TimestampOffset(timestamp, offset);
            }
            @Override
            public long length() {
                long superLength = super.length();
                return shouldCorruptLength ? superLength - 1 : superLength;
            }

            public void setShouldCorruptOffset(boolean shouldCorruptOffset) {
                this.shouldCorruptOffset = shouldCorruptOffset;
            }

            public void setShouldCorruptTimestamp(boolean shouldCorruptTimestamp) {
                this.shouldCorruptTimestamp = shouldCorruptTimestamp;
            }

            public void setShouldCorruptLength(boolean shouldCorruptLength) {
                this.shouldCorruptLength = shouldCorruptLength;
            }
        }
        try (MockTimeIndex mockIdx = new MockTimeIndex(idx.file(), baseOffset, maxEntries * 12)) {
            mockIdx.setShouldCorruptOffset(true);
            assertThrows(CorruptIndexException.class, mockIdx::sanityCheck);
            mockIdx.setShouldCorruptOffset(false);

            mockIdx.setShouldCorruptTimestamp(true);
            assertThrows(CorruptIndexException.class, mockIdx::sanityCheck);
            mockIdx.setShouldCorruptTimestamp(false);

            mockIdx.setShouldCorruptLength(true);
            assertThrows(CorruptIndexException.class, mockIdx::sanityCheck);
            mockIdx.setShouldCorruptLength(false);

            mockIdx.sanityCheck();
        }
    }

    @Test
    public void testIsFull() {
        assertFalse(idx.isFull());
        appendEntries(maxEntries - 1);
        assertTrue(idx.isFull());
    }

    @Test
    public void testLastEntry() {
        assertEquals(new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset), idx.lastEntry());
        idx.maybeAppend(1, 1 + baseOffset);
        assertEquals(new TimestampOffset(1, baseOffset + 1), idx.lastEntry());
    }

    @Test
    public void testResize() throws IOException {
        boolean result = idx.resize(maxEntries * idx.entrySize());
        assertFalse(result);

        result = idx.resize(maxEntries / 2 * idx.entrySize());
        assertTrue(result);

        result = idx.resize(maxEntries * 2 * idx.entrySize());
        assertTrue(result);
    }

    @Test
    public void testEntrySize() {
        assertEquals(12, idx.entrySize());
    }

    @Test
    public void testParseEntry() {
        idx.maybeAppend(1, 1 + baseOffset);
        assertEquals(new TimestampOffset(1, baseOffset + 1), idx.parseEntry(idx.mmap(), 0));
    }

    private void appendEntries(Integer numEntries) {
        IntStream.rangeClosed(1, numEntries).forEach(i -> idx.maybeAppend(i * 10L, i * 10L + baseOffset));
    }

    private File nonExistentTempFile() throws IOException {
        File file = TestUtils.tempFile();
        file.delete();
        return file;
    }

}
