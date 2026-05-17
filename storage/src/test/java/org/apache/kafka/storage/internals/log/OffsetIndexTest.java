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
import java.nio.ByteBuffer;
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

    /**
     * T4: Verify that OffsetIndex correctly stores and retrieves positions exceeding Integer.MAX_VALUE.
     */
    @Test
    public void testAppendAndLookupWithLargePositions() throws IOException {
        try (OffsetIndex idx = new OffsetIndex(nonExistentTempFile(), 0L, 10 * OffsetIndex.LARGE_ENTRY_SIZE, true, true)) {
            long posAbove2GB = Integer.MAX_VALUE + 1000L;
            idx.append(1, posAbove2GB);
            idx.append(2, posAbove2GB + 4096);
            idx.append(3, posAbove2GB + 8192);

            // Verify round-trip: lookup should return the exact large positions
            OffsetPosition result1 = idx.lookup(1);
            assertEquals(1, result1.offset());
            assertEquals(posAbove2GB, result1.position(),
                    "Position exceeding Integer.MAX_VALUE should be stored and retrieved correctly");

            OffsetPosition result2 = idx.lookup(2);
            assertEquals(2, result2.offset());
            assertEquals(posAbove2GB + 4096, result2.position());

            // Verify entry() accessor
            OffsetPosition entry0 = idx.entry(0);
            assertEquals(1, entry0.offset());
            assertEquals(posAbove2GB, entry0.position());

            OffsetPosition entry2 = idx.entry(2);
            assertEquals(3, entry2.offset());
            assertEquals(posAbove2GB + 8192, entry2.position());
        }
    }

    /**
     * T4 (continued): Verify reopen of index file with positions exceeding Integer.MAX_VALUE.
     */
    @Test
    public void testReopenWithLargePositions() throws IOException {
        File indexFile = nonExistentTempFile();
        OffsetIndex largeIndex = new OffsetIndex(indexFile, BASE_OFFSET, 30 * OffsetIndex.LARGE_ENTRY_SIZE, true, true);
        long posAbove2GB = 3_000_000_000L;
        OffsetPosition first = new OffsetPosition(BASE_OFFSET + 6, posAbove2GB);
        OffsetPosition second = new OffsetPosition(BASE_OFFSET + 7, posAbove2GB + 100);
        largeIndex.append(first.offset(), first.position());
        largeIndex.append(second.offset(), second.position());
        largeIndex.close();

        OffsetIndex reopened = new OffsetIndex(indexFile, BASE_OFFSET, 30 * OffsetIndex.LARGE_ENTRY_SIZE, false, true);
        assertEquals(first, reopened.lookup(first.offset()),
                "Large position should survive close/reopen");
        assertEquals(second, reopened.lookup(second.offset()));
        assertEquals(second.offset(), reopened.lastOffset());
        assertEquals(2, reopened.entries());
        reopened.close();
    }

    /**
     * T1: Verify that legacy 8-byte format indexes can be read correctly when
     * useLargeFormat=false (before MetadataVersion finalization).
     */
    @Test
    public void testLegacyFormatReadWrite() throws IOException {
        File indexFile = nonExistentTempFile();
        // Create an index in legacy 8-byte format
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 10 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            legacyIdx.append(1, 100);
            legacyIdx.append(2, 200);
            legacyIdx.append(3, 300);

            assertEquals(new OffsetPosition(1, 100), legacyIdx.lookup(1));
            assertEquals(new OffsetPosition(2, 200), legacyIdx.lookup(2));
            assertEquals(new OffsetPosition(3, 300), legacyIdx.lookup(3));
            assertEquals(3, legacyIdx.entries());

            // Verify file size is multiple of 8 (legacy format)
            legacyIdx.trimToValidSize();
            assertEquals(0, legacyIdx.sizeInBytes() % OffsetIndex.LEGACY_ENTRY_SIZE,
                    "Legacy format index should have entries as multiples of 8 bytes");
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * T1 (continued): Verify migration from legacy to large format via reset+rebuild.
     * This simulates what happens when MetadataVersion is finalized:
     * indexes are rebuilt from .log files in the new 12-byte format.
     */
    @Test
    public void testLegacyToLargeFormatMigration() throws IOException {
        // Create a legacy-format index
        File indexFile = nonExistentTempFile();
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 10 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            legacyIdx.append(1, 100);
            legacyIdx.append(2, 200);
            legacyIdx.append(3, 300);
        }

        // Phase 2: Simulate MetadataVersion finalization -- delete old index and recreate
        // in large format. This is what LogSegment.recover() does: it calls
        // offsetIndex().reset() which truncates the file to 0, then rebuilds.
        // After reset, the file is empty so the next open uses the requested format.
        Files.deleteIfExists(indexFile.toPath());
        try (OffsetIndex largeIdx = new OffsetIndex(indexFile, 0L, 10 * OffsetIndex.LARGE_ENTRY_SIZE, true, true)) {
            // Rebuild in new format with positions > 2GB
            long posAbove2GB = 3_000_000_000L;
            largeIdx.append(1, posAbove2GB);
            largeIdx.append(2, posAbove2GB + 4096);

            assertEquals(new OffsetPosition(1, posAbove2GB), largeIdx.lookup(1));
            assertEquals(new OffsetPosition(2, posAbove2GB + 4096), largeIdx.lookup(2));
            assertEquals(2, largeIdx.entries());
            largeIdx.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Simulate upgrade: write entries in legacy format, then read them with legacy format
     * (before MetadataVersion finalization). Verify data integrity is preserved.
     */
    @Test
    public void testUpgradeReadLegacyFormatWithNewCode() throws IOException {
        File indexFile = nonExistentTempFile();

        // Write entries using legacy format (simulates old broker)
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            for (int i = 0; i < 10; i++) {
                legacyIdx.append(i, i * 1000);
            }
        }

        // Read with new code but still in legacy mode (MetadataVersion not finalized)
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, false, false)) {
            assertEquals(10, reopened.entries());
            for (int i = 0; i < 10; i++) {
                OffsetPosition pos = reopened.entry(i);
                assertEquals(i, pos.offset());
                assertEquals(i * 1000, pos.position());
            }
            // Sanity check should pass since format matches
            reopened.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Simulate downgrade: write entries in legacy format with new code,
     * then verify they can be read by old code (also legacy format).
     * This proves the new broker doesn't break existing indexes when
     * MetadataVersion is not finalized.
     */
    @Test
    public void testDowngradeCompatibility() throws IOException {
        File indexFile = nonExistentTempFile();

        // Write with new code in legacy mode
        try (OffsetIndex newCodeLegacy = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            newCodeLegacy.append(1, 100);
            newCodeLegacy.append(2, 200);
            newCodeLegacy.append(3, 300);
        }

        // Verify file is in old 8-byte format
        long fileSize = indexFile.length();
        assertEquals(0, fileSize % OffsetIndex.LEGACY_ENTRY_SIZE,
                "File should be a multiple of 8 bytes (legacy format)");

        // Read with legacy format (simulates old broker reading)
        try (OffsetIndex oldBrokerRead = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, false, false)) {
            assertEquals(3, oldBrokerRead.entries());
            assertEquals(new OffsetPosition(1, 100), oldBrokerRead.entry(0));
            assertEquals(new OffsetPosition(2, 200), oldBrokerRead.entry(1));
            assertEquals(new OffsetPosition(3, 300), oldBrokerRead.entry(2));
            oldBrokerRead.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Simulate full migration: legacy → large format via reset + rebuild.
     * This is what happens when MetadataVersion is finalized and indexes
     * are rebuilt from .log files.
     */
    @Test
    public void testFormatMigrationViaReset() throws IOException {
        File indexFile = nonExistentTempFile();

        // Phase 1: Create legacy index with positions < 2GB
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            legacyIdx.append(1, 1000);
            legacyIdx.append(2, 2000);
            legacyIdx.append(3, 3000);
        }

        // Phase 2: Delete old index and recreate in large format
        // (simulates MetadataVersion finalization + LogSegment.recover())
        Files.deleteIfExists(indexFile.toPath());
        try (OffsetIndex largeIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, true, true)) {
            // Rebuild with positions > 2GB (the whole point of the format change)
            long posAbove2GB = 3_000_000_000L;
            largeIdx.append(1, posAbove2GB);
            largeIdx.append(2, posAbove2GB + 4096);
            largeIdx.append(3, posAbove2GB + 8192);

            // Verify round-trip
            assertEquals(new OffsetPosition(1, posAbove2GB), largeIdx.lookup(1));
            assertEquals(new OffsetPosition(3, posAbove2GB + 8192), largeIdx.entry(2));
            assertEquals(3, largeIdx.entries());
            largeIdx.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    // === Format auto-detection tests (mb-1) ===

    /**
     * Verify that opening a legacy 8-byte file with useLargeFormat=true auto-detects the
     * legacy format and reads correctly. This is the key mb-1 scenario: after MetadataVersion
     * finalization, old index files must still be read correctly.
     */
    @Test
    public void testAutoDetectLegacyFormatWhenLargeRequested() throws IOException {
        File indexFile = nonExistentTempFile();
        // Write 5 entries in legacy format (40 bytes, mod12 != 0 -> unambiguous)
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            for (int i = 0; i < 5; i++) {
                legacyIdx.append(i, i * 1000);
            }
        }

        // Open with useLargeFormat=true -- should auto-detect as legacy
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, false, true)) {
            assertEquals(5, reopened.entries());
            for (int i = 0; i < 5; i++) {
                assertEquals(new OffsetPosition(i, i * 1000), reopened.entry(i));
            }
            reopened.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Verify auto-detection with an ambiguous file size (divisible by both 8 and 12).
     * 3 legacy entries = 24 bytes, which is also 2 * 12.
     * The validator should detect it as legacy because reading as 12-byte produces wrong data.
     */
    @Test
    public void testAutoDetectAmbiguousFileSizeLegacy() throws IOException {
        File indexFile = nonExistentTempFile();
        // Write 3 entries in legacy format (24 bytes = divisible by both 8 and 12)
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            legacyIdx.append(1, 100);
            legacyIdx.append(2, 200);
            legacyIdx.append(3, 300);
        }

        // Open with useLargeFormat=true -- should auto-detect as legacy via validation
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, false, true)) {
            assertEquals(3, reopened.entries());
            assertEquals(new OffsetPosition(1, 100), reopened.entry(0));
            assertEquals(new OffsetPosition(2, 200), reopened.entry(1));
            assertEquals(new OffsetPosition(3, 300), reopened.entry(2));
            reopened.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Verify auto-detection for a large-format file opened with useLargeFormat=false.
     * After MetadataVersion is finalized and then hypothetically un-finalized (or reading
     * a large-format file with legacy request), it should still auto-detect correctly.
     */
    @Test
    public void testAutoDetectLargeFormatWhenLegacyRequested() throws IOException {
        File indexFile = nonExistentTempFile();
        // Write entries in large format with positions > Integer.MAX_VALUE
        try (OffsetIndex largeIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, true, true)) {
            long pos = 3_000_000_000L;
            largeIdx.append(1, pos);
            largeIdx.append(2, pos + 4096);
        }

        // Open with useLargeFormat=false -- should auto-detect as large format
        // because 24 bytes / 8 = 3 entries would produce invalid data (negative positions)
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, false, false)) {
            assertEquals(2, reopened.entries());
            assertEquals(new OffsetPosition(1, 3_000_000_000L), reopened.entry(0));
            assertEquals(new OffsetPosition(2, 3_000_000_000L + 4096), reopened.entry(1));
            reopened.sanityCheck();
        }
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * Verify detectEntrySize static method directly.
     */
    @Test
    public void testDetectEntrySizeNewFile() throws IOException {
        File nonExistent = nonExistentTempFile();
        nonExistent.delete(); // ensure it doesn't exist
        assertEquals(OffsetIndex.LEGACY_ENTRY_SIZE,
                OffsetIndex.detectEntrySize(nonExistent, OffsetIndex.LEGACY_ENTRY_SIZE));
        assertEquals(OffsetIndex.LARGE_ENTRY_SIZE,
                OffsetIndex.detectEntrySize(nonExistent, OffsetIndex.LARGE_ENTRY_SIZE));
    }

    @Test
    public void testDetectEntrySizeEmptyFile() throws IOException {
        File emptyFile = nonExistentTempFile();
        emptyFile.createNewFile();
        assertEquals(OffsetIndex.LEGACY_ENTRY_SIZE,
                OffsetIndex.detectEntrySize(emptyFile, OffsetIndex.LEGACY_ENTRY_SIZE));
        Files.deleteIfExists(emptyFile.toPath());
    }

    /**
     * Cross-check against the companion .log file size disambiguates ~all real cases:
     * legacy-read-as-large produces positions > 2^32 for segments < 2GiB; the .log
     * length cross-check rejects those impossible positions.
     */
    @Test
    public void testDetectEntrySizeCrossChecksLogFileSize() throws IOException {
        File indexFile = nonExistentTempFile();
        // Write 3 legacy entries (24 bytes, ambiguous size)
        try (OffsetIndex legacyIdx = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LEGACY_ENTRY_SIZE, true, false)) {
            legacyIdx.append(1, 100);
            legacyIdx.append(2, 200);
            legacyIdx.append(3, 300);
        }

        // Create a companion .log file that is too small for any large-format positions
        // to be valid (positions would have to be <= 1024).
        String indexPath = indexFile.getAbsolutePath();
        File logFile = new File(indexPath.substring(0, indexPath.lastIndexOf('.')) + ".log");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(logFile, "rw")) {
            raf.setLength(1024);
        }

        // Detection should pick legacy thanks to the .log cross-check.
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L, 20 * OffsetIndex.LARGE_ENTRY_SIZE, false, true)) {
            assertEquals(3, reopened.entries());
            assertEquals(new OffsetPosition(1, 100), reopened.entry(0));
        }

        Files.deleteIfExists(logFile.toPath());
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * P0-1 (revised v2): when format is genuinely ambiguous (strict monotonicity passes for both
     * formats AND no .log cross-check disambiguates) for a non-trivially-short file, detection
     * throws {@link CorruptIndexException} so recovery rebuilds from the .log file. Silently
     * picking the wrong format would risk consumer-visible read corruption.
     */
    @Test
    public void testDetectEntrySizeAmbiguousThrowsForNonTrivialFiles() throws IOException {
        File indexFile = nonExistentTempFile();
        ByteBuffer buf = ByteBuffer.allocate(24);
        // Bytes 1,2,3,4,5,6 pass strict monotonicity under both formats.
        buf.putInt(1);
        buf.putInt(2);
        buf.putInt(3);
        buf.putInt(4);
        buf.putInt(5);
        buf.putInt(6);
        Files.write(indexFile.toPath(), buf.array());

        // No companion .log file. Both formats pass strict validation.
        // Detection must throw so recovery rebuilds from .log.
        assertThrows(CorruptIndexException.class,
            () -> OffsetIndex.detectEntrySize(indexFile, OffsetIndex.LARGE_ENTRY_SIZE));
        assertThrows(CorruptIndexException.class,
            () -> OffsetIndex.detectEntrySize(indexFile, OffsetIndex.LEGACY_ENTRY_SIZE));
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * P0-1 (corner case): for very short files (fewer than two large-format entries) the
     * monotonicity check has no power to disambiguate. Detection returns the requested format
     * because the file will be rewritten on the next append anyway.
     */
    @Test
    public void testDetectEntrySizeShortFileFallsBackToRequested() throws IOException {
        File indexFile = nonExistentTempFile();
        // 8 bytes: exactly one legacy entry, less than one large entry (12 bytes).
        // Both formats trivially "validate" but we cannot tell them apart.
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(1);
        buf.putInt(100);
        Files.write(indexFile.toPath(), buf.array());

        // For a single-entry file, fall back to requested format.
        assertEquals(OffsetIndex.LEGACY_ENTRY_SIZE,
            OffsetIndex.detectEntrySize(indexFile, OffsetIndex.LEGACY_ENTRY_SIZE));
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * P0-1 (legitimate-migration case): a non-empty index that fails strict validation under both
     * formats (e.g. corruption, partial-write tail) throws {@link CorruptIndexException}. This is
     * the safer behaviour than silently picking the requested format and mmaping garbage.
     */
    @Test
    public void testDetectEntrySizeCorruptThrows() throws IOException {
        File indexFile = nonExistentTempFile();
        // 24 bytes that fail monotonicity under both 8-byte and 12-byte readings
        // (decreasing relative offsets).
        ByteBuffer buf = ByteBuffer.allocate(24);
        buf.putInt(10);
        buf.putInt(1000);
        buf.putInt(5);  // decreases -- breaks legacy monotonicity at slot 1
        buf.putInt(900);
        buf.putInt(0);  // decreases again
        buf.putInt(800);
        Files.write(indexFile.toPath(), buf.array());

        assertThrows(CorruptIndexException.class,
            () -> OffsetIndex.detectEntrySize(indexFile, OffsetIndex.LARGE_ENTRY_SIZE));
        Files.deleteIfExists(indexFile.toPath());
    }

    /**
     * P0-1 (cross-check disambiguation): when both formats would pass strict monotonicity in
     * isolation but the companion .log file's size rules out one of them, detection picks the
     * other one cleanly. This exercises the LCM(8,12)=24-byte boundary -- one of the realistic
     * migration shapes.
     */
    @Test
    public void testDetectEntrySizeLogSizeCrossCheckPicksLegacy() throws IOException {
        // The index file's name is "<prefix>.index"; companionLogFileSize replaces
        // ".index" with ".log" in the name to find the sibling log file.
        File indexFile = TestUtils.tempFile("offset-idx-cross-check", ".index");
        String idxName = indexFile.getName();
        String logName = idxName.substring(0, idxName.lastIndexOf(".index")) + ".log";
        File logFile = new File(indexFile.getParentFile(), logName);

        // Legacy data: three entries (offset, position) = (1,100),(2,200),(3,300)
        ByteBuffer buf = ByteBuffer.allocate(24);
        buf.putInt(1);
        buf.putInt(100);
        buf.putInt(2);
        buf.putInt(200);
        buf.putInt(3);
        buf.putInt(300);
        Files.write(indexFile.toPath(), buf.array());
        // Companion .log file that is large enough to hold all legacy positions but smaller
        // than what reading the same bytes as large format would imply.
        // Large-format reading of these bytes would produce position=(100L<<32)|2 etc.,
        // astronomically larger than any realistic .log size.
        Files.write(logFile.toPath(), new byte[500]);

        try {
            // Even when caller requests LARGE, the .log cross-check rules it out.
            assertEquals(OffsetIndex.LEGACY_ENTRY_SIZE,
                OffsetIndex.detectEntrySize(indexFile, OffsetIndex.LARGE_ENTRY_SIZE));
        } finally {
            Files.deleteIfExists(indexFile.toPath());
            Files.deleteIfExists(logFile.toPath());
        }
    }

    private void assertWriteFails(String message, OffsetIndex idx, int offset) {
        Exception e = assertThrows(Exception.class, () -> idx.append(offset, 1), message);
        assertEquals(IllegalArgumentException.class, e.getClass(), "Got an unexpected exception.");
    }
}