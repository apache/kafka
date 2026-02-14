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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetIndexTest {

    private OffsetIndex index;
    private static final long BASE_OFFSET = 45L;

    @BeforeEach
    public void setup() throws IOException {
        index = new OffsetIndex(nonExistentTempFile(), BASE_OFFSET, 30 * 8);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (Objects.nonNull(index)) {
            this.index.close();
            Files.deleteIfExists(index.file().toPath());
        }
    }

    @Test
    public void randomLookupTest() {
        assertEquals(new OffsetPosition(index.baseOffset(), 0), index.lookup(92L),
                "Not present value should return physical offset 0.");
        int base = (int) (index.baseOffset() + 1);
        int size = index.maxEntries();
        Map<Long, Integer> offsetsToPositions = offsetsToPositions(base, size);
        offsetsToPositions.forEach((offset, position) -> index.append(offset, position));
        // should be able to find all those values
        offsetsToPositions.forEach((offset, position) ->
                assertEquals(new OffsetPosition(offset, position), index.lookup(offset),
                        "Should find the correct position for the offset."));

        // for non-present values we should find the offset of the largest value less than or equal to this
        TreeMap<Long, OffsetPosition> valMap = new TreeMap<>();
        for (Map.Entry<Long, Integer> entry : offsetsToPositions.entrySet()) {
            valMap.put(entry.getKey(), new OffsetPosition(entry.getKey(), entry.getValue()));
        }

        List<Integer> offsets = new ArrayList<>();
        for (long i = index.baseOffset(); i < valMap.lastKey(); i++) {
            offsets.add((int) i);
        }
        Collections.shuffle(offsets);

        for (int offset : offsets.subList(0, 30)) {
            OffsetPosition rightAnswer;
            if (offset < valMap.firstKey()) {
                rightAnswer = new OffsetPosition(index.baseOffset(), 0);
            } else {
                Map.Entry<Long, OffsetPosition> lastEntry = valMap.floorEntry((long) offset);
                rightAnswer = new OffsetPosition(lastEntry.getKey(), lastEntry.getValue().position());
            }
            assertEquals(rightAnswer, index.lookup(offset),
                    "The index should give the same answer as the sorted map");
        }
    }

    @Test
    public void lookupExtremeCases() {
        assertEquals(new OffsetPosition(index.baseOffset(), 0), index.lookup(index.baseOffset()),
                "Lookup on empty file");
        for (int i = 0; i < index.maxEntries(); ++i) {
            index.append(index.baseOffset() + i + 1, i);
        }
        // check first and last entry
        assertEquals(new OffsetPosition(index.baseOffset(), 0), index.lookup(index.baseOffset()));
        assertEquals(new OffsetPosition(index.baseOffset() + index.maxEntries(),
                index.maxEntries() - 1), index.lookup(index.baseOffset() + index.maxEntries()));
    }

    @Test
    public void testEntry() {
        for (int i = 0; i < index.maxEntries(); ++i) {
            index.append(index.baseOffset() + i, i);
        }
        for (int i = 0; i < index.maxEntries(); ++i) {
            assertEquals(new OffsetPosition(index.baseOffset() + i, i), index.entry(i));
        }
    }

    @Test
    public void testEntryOverflow() {
        assertThrows(IllegalArgumentException.class, () -> index.entry(0));
    }

    @Test
    public void appendTooMany() {
        for (int i = 0; i < index.maxEntries(); ++i) {
            long offset = index.baseOffset() + i + 1;
            index.append(offset, i);
        }
        assertWriteFails("Append should fail on a full index",
                index, index.maxEntries() + 1);
    }

    @Test
    public void appendOutOfOrder() {
        index.append(51, 0);
        assertThrows(InvalidOffsetException.class, () -> index.append(50, 1));
    }

    @Test
    public void testFetchUpperBoundOffset() {
        OffsetPosition first = new OffsetPosition(BASE_OFFSET, 0);
        OffsetPosition second = new OffsetPosition(BASE_OFFSET + 1, 10);
        OffsetPosition third = new OffsetPosition(BASE_OFFSET + 2, 23);
        OffsetPosition fourth = new OffsetPosition(BASE_OFFSET + 3, 37);

        assertEquals(Optional.empty(), index.fetchUpperBoundOffset(first, 5));

        Stream.of(first, second, third, fourth)
                .forEach(offsetPosition -> index.append(offsetPosition.offset(), offsetPosition.position()));

        assertEquals(Optional.of(second), index.fetchUpperBoundOffset(first, 5));
        assertEquals(Optional.of(second), index.fetchUpperBoundOffset(first, 10));
        assertEquals(Optional.of(third), index.fetchUpperBoundOffset(first, 23));
        assertEquals(Optional.of(third), index.fetchUpperBoundOffset(first, 22));
        assertEquals(Optional.of(fourth), index.fetchUpperBoundOffset(second, 24));
        assertEquals(Optional.empty(), index.fetchUpperBoundOffset(fourth, 1));
        assertEquals(Optional.empty(), index.fetchUpperBoundOffset(first, 200));
        assertEquals(Optional.empty(), index.fetchUpperBoundOffset(second, 200));
    }

    @Test
    public void testReopen() throws IOException {
        OffsetPosition first = new OffsetPosition(51, 0);
        OffsetPosition sec = new OffsetPosition(52, 1);
        index.append(first.offset(), first.position());
        index.append(sec.offset(), sec.position());
        index.close();
        OffsetIndex idxRo = new OffsetIndex(index.file(), index.baseOffset());
        assertEquals(first, idxRo.lookup(first.offset()));
        assertEquals(sec, idxRo.lookup(sec.offset()));
        assertEquals(sec.offset(), idxRo.lastOffset());
        assertEquals(2, idxRo.entries());
        assertWriteFails("Append should fail on read-only index", idxRo, 53);
    }

    @Test
    public void truncate() throws IOException {
        try (OffsetIndex idx = new OffsetIndex(nonExistentTempFile(), 0L, 10 * 8)) {
            idx.truncate();
            IntStream.range(1, 10).forEach(i -> idx.append(i, i));

            // now check the last offset after various truncate points and validate that we can still append to the index.
            idx.truncateTo(12);
            assertEquals(new OffsetPosition(9, 9), idx.lookup(10),
                    "Index should be unchanged by truncate past the end");
            assertEquals(9, idx.lastOffset(),
                    "9 should be the last entry in the index");

            idx.append(10, 10);
            idx.truncateTo(10);
            assertEquals(new OffsetPosition(9, 9), idx.lookup(10),
                    "Index should be unchanged by truncate at the end");
            assertEquals(9, idx.lastOffset(),
                    "9 should be the last entry in the index");
            idx.append(10, 10);

            idx.truncateTo(9);
            assertEquals(new OffsetPosition(8, 8), idx.lookup(10),
                    "Index should truncate off last entry");
            assertEquals(8, idx.lastOffset(),
                    "8 should be the last entry in the index");
            idx.append(9, 9);

            idx.truncateTo(5);
            assertEquals(new OffsetPosition(4, 4), idx.lookup(10),
                    "4 should be the last entry in the index");
            assertEquals(4, idx.lastOffset(),
                    "4 should be the last entry in the index");
            idx.append(5, 5);

            idx.truncate();
            assertEquals(0, idx.entries(), "Full truncation should leave no entries");
        }

    }

    @Test
    public void forceUnmapTest() throws IOException {
        OffsetIndex idx = new OffsetIndex(nonExistentTempFile(), 0L, 10 * 8);
        idx.forceUnmap();
        // mmap should be null after unmap causing lookup to throw a NPE
        assertThrows(NullPointerException.class, () -> idx.lookup(1));
    }

    @Test
    public void testSanityLastOffsetEqualToBaseOffset() throws IOException {
        // Test index sanity for the case where the last offset appended to the index is equal to the base offset
        long baseOffset = 20L;
        try (OffsetIndex idx = new OffsetIndex(nonExistentTempFile(), baseOffset, 10 * 8)) {
            idx.append(baseOffset, 0);
            idx.sanityCheck();
        }
    }

    private Map<Long, Integer> offsetsToPositions(int base, int len) {
        List<Integer> positions = monotonicSeq(0, len);
        return monotonicSeq(base, len)
                .stream()
                .map(Long::valueOf)
                .collect(TreeMap::new, (m, v) -> m.put(v, positions.remove(0)), Map::putAll);
    }

    private List<Integer> monotonicSeq(int base, int len) {
        Random random = new Random();
        List<Integer> seq = new ArrayList<>(len);
        int last = base;
        for (int i = 0; i < len; i++) {
            last += random.nextInt(15) + 1;
            seq.add(last);
        }
        return seq;
    }

    private File nonExistentTempFile() throws IOException {
        File file = TestUtils.tempFile();
        Files.deleteIfExists(file.toPath());
        return file;
    }

    private void assertWriteFails(String message, OffsetIndex idx, int offset) {
        Exception e = assertThrows(Exception.class, () -> idx.append(offset, 1), message);
        assertEquals(IllegalArgumentException.class, e.getClass(), "Got an unexpected exception.");
    }
}