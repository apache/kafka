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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * <p>The index is stored in a file that is pre-allocated to hold a fixed maximum number of entries (8 or 12 bytes each,
 * depending on whether the large index format is enabled via MetadataVersion).
 *
 * <p>The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * <p>Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * <p>No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * <p>The file format is a series of entries. In legacy mode (default), each entry is a 4-byte "relative" offset and a 4-byte
 * physical file position (8 bytes total). In large mode (after MetadataVersion IBP_4_4_IV1), each entry is a 4-byte "relative"
 * offset and an 8-byte physical file position (12 bytes total), supporting segments larger than 2GB.
 * The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * <p>The frequency of entries is up to the user of this class.
 *
 * <p>All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public final class OffsetIndex extends AbstractIndex {
    private static final Logger log = LoggerFactory.getLogger(OffsetIndex.class);

    // 12-byte entries: 4-byte relative offset + 8-byte physical position (supports >2GB segments)
    public static final int LARGE_ENTRY_SIZE = 12;
    // 8-byte entries: 4-byte relative offset + 4-byte physical position (legacy, <=2GB segments)
    public static final int LEGACY_ENTRY_SIZE = 8;

    private final int indexEntrySize;

    /* the last offset in the index */
    private volatile long lastOffset;

    public OffsetIndex(File file, long baseOffset) throws IOException {
        this(file, baseOffset, -1);
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this(file, baseOffset, maxIndexSize, true);
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize, boolean writable) throws IOException {
        this(file, baseOffset, maxIndexSize, writable, false);
    }

    /**
     * @param useLargeFormat If true, use 12-byte entries (8-byte physical position) for NEW files.
     *                       If false, use legacy 8-byte entries (4-byte physical position) for NEW files.
     *                       For EXISTING files, the format is auto-detected from the file content.
     *                       <p>In production this value comes from
     *                       {@link LogConfig#shouldUseLargeIndexFormat()}, which is updated by the
     *                       broker's metadata publisher when the finalized {@code metadata.version}
     *                       reaches {@code IBP_4_4_IV1} (KIP-1333). Tests may pass {@code true}
     *                       directly to exercise the large-format path without finalization.
     */
    public OffsetIndex(File file, long baseOffset, int maxIndexSize, boolean writable, boolean useLargeFormat) throws IOException {
        this(file, baseOffset, maxIndexSize, writable, detectWithRetry(file, useLargeFormat), useLargeFormat);
    }

    /**
     * Detect entry size, then re-verify after entering the constructor to catch TOCTOU races
     * where the file is truncated/extended between detection and mmap. If the file length
     * changed between detection and verification, we re-detect once and throw if it changes again.
     *
     * <p>This mitigates two race windows:
     * <ol>
     *   <li>A concurrent {@code recover()} / {@code truncateTo()} / {@code reset()} changes
     *       {@code file.length()} between detection and the mmap open in
     *       {@link AbstractIndex#createAndAssignMmap()}.
     *   <li>A partial-write tail from a previous crash makes the on-disk length straddle an
     *       entry boundary.
     * </ol>
     *
     * <p>The mitigation does not require a file lock because the {@link AbstractIndex} constructor
     * opens the file with O_CREAT semantics on a fresh new file (in which case
     * {@code detectEntrySize} returned {@code requestedEntrySize}), and existing files are
     * touched only by single-threaded log recovery. A double-check at construction time is
     * sufficient to detect any in-flight concurrent modification.
     */
    private static int detectWithRetry(File file, boolean useLargeFormat) throws IOException {
        int requested = useLargeFormat ? LARGE_ENTRY_SIZE : LEGACY_ENTRY_SIZE;
        long lengthBefore = file.exists() ? file.length() : 0;
        int detected = detectEntrySize(file, requested);
        long lengthAfter = file.exists() ? file.length() : 0;
        if (lengthBefore != lengthAfter) {
            // File changed during detection; re-detect once.
            int redetected = detectEntrySize(file, requested);
            long lengthFinal = file.exists() ? file.length() : 0;
            if (lengthAfter != lengthFinal || detected != redetected) {
                throw new CorruptIndexException("Index file " + file.getAbsolutePath() +
                    " length changed during format detection (" + lengthBefore + " -> " +
                    lengthAfter + " -> " + lengthFinal + "); refusing to mmap with a possibly-wrong " +
                    "entry size. Recovery will rebuild from the .log file.");
            }
            return redetected;
        }
        return detected;
    }

    private OffsetIndex(File file, long baseOffset, int maxIndexSize, boolean writable,
                        int detectedEntrySize, boolean useLargeFormat) throws IOException {
        super(file, baseOffset, maxIndexSize, writable, detectedEntrySize);
        this.indexEntrySize = detectedEntrySize;

        // Note on TOCTOU: detectWithRetry above re-checks file.length() after detection. If the
        // file size still does not match the detected entry size after mmap (e.g., partial-write
        // tail from a previous crash), sanityCheck() will reject it via the
        // "length() % entrySize() != 0" rule and recovery will rebuild from the .log file.

        lastOffset = lastEntry().offset();

        log.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}, entrySize = {}, useLargeFormat = {}",
            file.getAbsolutePath(), maxEntries(), maxIndexSize, entries(), lastOffset, mmap().position(), indexEntrySize, useLargeFormat);
    }

    // Number of leading entries to inspect when disambiguating index format on read.
    // Scanning more than this is unnecessary because legitimate indexes have strictly
    // monotonic prefixes that disambiguate quickly; corrupt prefixes are caught immediately.
    static final int FORMAT_DETECTION_PREFIX_ENTRIES = 32;

    /**
     * Detect the entry size of an existing index file.
     *
     * <p>For new or empty files, returns the requested format. For existing files with data,
     * auto-detects the format from the file size:
     * <ul>
     *   <li>If file size is only divisible by 12 (not 8): large format
     *   <li>If file size is only divisible by 8 (not 12): legacy format
     *   <li>If divisible by neither: returns requested format (file will fail sanityCheck and be rebuilt)
     *   <li>If divisible by both (ambiguous, ~1/3 of legacy and ~1/2 of large indexes): cross-checks
     *       against the companion .log file size and validates entries with strict monotonicity
     * </ul>
     *
     * <p>On true ambiguity (both formats validate equally and the .log cross-check cannot
     * disambiguate), throws {@link CorruptIndexException} so the caller can trigger a
     * segment rebuild from the .log file. Silently picking the wrong format can corrupt reads.
     */
    static int detectEntrySize(File file, int requestedEntrySize) throws IOException {
        if (!file.exists()) return requestedEntrySize;
        long fileSize = file.length();
        if (fileSize == 0) return requestedEntrySize;

        boolean validAsLegacy = (fileSize % LEGACY_ENTRY_SIZE == 0);
        boolean validAsLarge = (fileSize % LARGE_ENTRY_SIZE == 0);

        if (validAsLarge && !validAsLegacy) return LARGE_ENTRY_SIZE;
        if (validAsLegacy && !validAsLarge) return LEGACY_ENTRY_SIZE;
        if (!validAsLegacy && !validAsLarge) return requestedEntrySize; // corrupt, will fail sanityCheck

        // Ambiguous: file size is divisible by both 8 and 12 (lcm = 24).
        // This is the common case, not an edge case: ~1/3 of legacy and ~1/2 of large indexes.
        // We must NOT silently fall back to requestedEntrySize -- a wrong choice produces
        // garbage OffsetPositions and consumer-visible read corruption.
        return detectEntryByValidation(file, fileSize, requestedEntrySize);
    }

    /**
     * For ambiguous files (size divisible by both 8 and 12), validate entries with each format
     * using strict monotonicity and cross-check positions against the companion .log file.
     * Throws CorruptIndexException if both formats remain plausible -- recovery will rebuild.
     */
    private static int detectEntryByValidation(File file, long fileSize, int requestedEntrySize) throws IOException {
        // Read enough bytes to inspect up to FORMAT_DETECTION_PREFIX_ENTRIES of the LARGE format
        // (which is the bigger of the two, so it bounds the read size for both).
        long maxReadBytes = (long) FORMAT_DETECTION_PREFIX_ENTRIES * LARGE_ENTRY_SIZE;
        int readBytes = (int) Math.min(fileSize, maxReadBytes);
        ByteBuffer buf = ByteBuffer.allocate(readBytes);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            int n = raf.getChannel().read(buf, 0);
            if (n < readBytes) {
                throw new IOException("Short read of index file " + file.getAbsolutePath() +
                    " for format detection: expected " + readBytes + " bytes, got " + n);
            }
        }
        buf.flip();

        // Cross-check against the companion .log file: positions must be <= log length.
        // This single check disambiguates ~all real cases because legacy-read-as-large
        // produces astronomical positions (>2^32) for segments < 2GiB, and
        // large-read-as-legacy produces a stream of zeros for the upper bytes.
        long logFileSize = companionLogFileSize(file);

        boolean largeValid = validateEntries(buf, LARGE_ENTRY_SIZE, fileSize, logFileSize);
        buf.rewind();
        boolean legacyValid = validateEntries(buf, LEGACY_ENTRY_SIZE, fileSize, logFileSize);

        if (largeValid && !legacyValid) return LARGE_ENTRY_SIZE;
        if (legacyValid && !largeValid) return LEGACY_ENTRY_SIZE;

        // If neither passes validation, the file is genuinely corrupt. Throw so recovery
        // rebuilds from .log -- silently picking the requested format would mmap garbage.
        if (!largeValid && !legacyValid) {
            throw new CorruptIndexException("Index file " + file.getAbsolutePath() +
                " (size=" + fileSize + ") failed strict validation against both legacy " +
                "(8-byte) and large (12-byte) entry formats; recovery will rebuild from " +
                "the companion .log file.");
        }

        // Both formats pass strict validation. The legitimate case is a tiny prefix
        // (one or two entries) where the leading 4 bytes look like a valid relative offset
        // either way. For files where the .log cross-check has narrowed the search but
        // both still pass, we cannot distinguish them with confidence -- silently picking
        // the requested format risks reading garbage and producing consumer-visible
        // corruption. Fail loud so recovery rebuilds from .log; the resulting cost
        // (rebuilding one index) is bounded and far cheaper than a corrupt-read incident.
        //
        // Exception: for very short files (less than two full entries in either format)
        // both formats trivially validate and there is no realistic way to disambiguate;
        // fall back to the requested entry size since recovery will overwrite this file
        // anyway as soon as the first batch is appended.
        long minDistinguishingBytes = 2L * LARGE_ENTRY_SIZE; // need >=2 entries to apply monotonicity
        if (fileSize < minDistinguishingBytes) {
            log.debug("Index file {} is too short ({} bytes) to distinguish legacy from " +
                    "large format; using requested entry size {}. The file will be rewritten " +
                    "as soon as a new batch is appended.",
                file.getAbsolutePath(), fileSize, requestedEntrySize);
            return requestedEntrySize;
        }
        throw new CorruptIndexException("Could not unambiguously detect index format for " +
            file.getAbsolutePath() + " (size=" + fileSize + ", logFileSize=" + logFileSize +
            "): both legacy (8-byte) and large (12-byte) formats pass strict validation. " +
            "Recovery will rebuild from the companion .log file. This typically indicates a " +
            "crash mid-write, a partial truncation, or a manually-edited file.");
    }

    /**
     * Return the size of the companion .log file, or -1 if it does not exist (in which case
     * the position cross-check is skipped).
     *
     * <p>Handles the {@code .swap} suffix used by log splitting:
     * <ul>
     *   <li>{@code foo.index}      -> companion {@code foo.log}
     *   <li>{@code foo.index.swap} -> companion {@code foo.log.swap}
     *   <li>{@code foo.index.cleaned} -> companion {@code foo.log.cleaned}
     *   <li>{@code foo.index.deleted} -> companion {@code foo.log.deleted}
     * </ul>
     */
    private static long companionLogFileSize(File indexFile) {
        String name = indexFile.getName();
        int idx = name.indexOf(".index");
        if (idx < 0) return -1;
        // Replace ".index" with ".log", preserving any suffix (.swap, .cleaned, .deleted).
        String companionName = name.substring(0, idx) + ".log" + name.substring(idx + ".index".length());
        File logFile = new File(indexFile.getParentFile(), companionName);
        return logFile.exists() ? logFile.length() : -1;
    }

    /**
     * Check if entries read with the given entry size produce valid data:
     * <ul>
     *   <li>Relative offsets must be non-negative and strictly increasing
     *   <li>Positions must be non-negative and strictly increasing
     *   <li>If logFileSize is known (&gt;= 0), every position must be &lt;= logFileSize
     * </ul>
     * Inspects up to FORMAT_DETECTION_PREFIX_ENTRIES entries from the buffer.
     */
    private static boolean validateEntries(ByteBuffer buf, int entrySize, long indexFileSize, long logFileSize) {
        int availableEntries = buf.limit() / entrySize;
        int totalEntries = (int) Math.min(indexFileSize / entrySize, FORMAT_DETECTION_PREFIX_ENTRIES);
        int numEntries = Math.min(availableEntries, totalEntries);
        if (numEntries == 0) return true;

        int prevRelOffset = -1;
        long prevPosition = -1;
        for (int i = 0; i < numEntries; i++) {
            int relOffset = buf.getInt(i * entrySize);
            long position;
            if (entrySize == LARGE_ENTRY_SIZE) {
                position = buf.getLong(i * entrySize + 4);
            } else {
                // Signed read matches physical() above; legacy positions are always
                // non-negative ints because append() writes them via putInt((int) position)
                // and requires position <= Integer.MAX_VALUE. A signed read therefore
                // returns a non-negative long, and the "position < 0" check below correctly
                // rejects any corrupt file whose 4-byte position has the sign bit set.
                position = buf.getInt(i * entrySize + 4);
            }

            if (relOffset < 0 || position < 0) return false;
            // Strict monotonicity: a legitimate index never repeats either coordinate.
            if (i > 0 && relOffset <= prevRelOffset) return false;
            if (i > 0 && position <= prevPosition) return false;
            // Position cross-check against companion .log file (when available).
            // A legitimate position is bounded by the actual log file size.
            if (logFileSize >= 0 && position > logFileSize) return false;

            prevRelOffset = relOffset;
            prevPosition = position;
        }
        return true;
    }

    @Override
    public void sanityCheck() {
        if (entries() != 0 && lastOffset < baseOffset())
            throw new CorruptIndexException("Corrupt index found, index file " + file().getAbsolutePath() + " has non-zero size " +
                "but the last offset is " + lastOffset + " which is less than the base offset " + baseOffset());
        if (length() % entrySize() != 0)
            throw new CorruptIndexException("Index file " + file().getAbsolutePath() + " is corrupt, found " + length() +
                " bytes which is neither positive nor a multiple of the runtime entry size " + entrySize() +
                " (legacy=" + LEGACY_ENTRY_SIZE + ", large=" + LARGE_ENTRY_SIZE + ")");
        // Note: strict monotonicity is enforced at write time by append() and at format-detection
        // time by validateEntries(). Adding a full-scan monotonicity check here would conflict with
        // pre-allocated index files where unwritten trailing slots are legitimately zero-filled.
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and its corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset
     *         If the target offset is smaller than the least entry in the index (or the index is empty),
     *         the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(long targetOffset) {
        return inRemapReadLock(() -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY);
            if (slot == -1)
                return new OffsetPosition(baseOffset(), 0);
            else
                return parseEntry(idx, slot);
        });
    }

    /**
     * Get the nth offset mapping from the index
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(int n) {
        return inRemapReadLock(() -> {
            if (n >= entries())
                throw new IllegalArgumentException("Attempt to fetch the " + n + "th entry from index " +
                    file().getAbsolutePath() + ", which has size " + entries());
            return parseEntry(mmap(), n);
        });
    }

    /**
     * Find an upper bound offset for the given fetch starting position and size. This is an offset which
     * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
     * such offset.
     */
    public Optional<OffsetPosition> fetchUpperBoundOffset(OffsetPosition fetchOffset, int fetchSize) {
        return inRemapReadLock(() -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = smallestUpperBoundSlotFor(idx, fetchOffset.position() + (long) fetchSize, IndexSearchType.VALUE);
            if (slot == -1)
                return Optional.empty();
            else
                return Optional.of(parseEntry(idx, slot));
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
     * @throws InvalidOffsetException if provided offset is not larger than the last offset
     */
    public void append(long offset, long position) {
        inLock(() -> {
            if (isFull())
                throw new IllegalArgumentException("Attempt to append to a full index (size = " + entries() + ").");

            if (entries() == 0 || offset > lastOffset) {
                log.trace("Adding index entry {} => {} to {}", offset, position, file().getAbsolutePath());
                mmap().putInt(relativeOffset(offset));
                if (indexEntrySize == LARGE_ENTRY_SIZE) {
                    mmap().putLong(position);
                } else {
                    if (position > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Position " + position +
                            " exceeds Integer.MAX_VALUE for legacy 8-byte index format in " +
                            file().getAbsolutePath() + ". This index was opened with " +
                            "useLargeFormat=false. To support physical positions > 2 GiB, " +
                            "the broker must have a finalized metadata.version >= IBP_4_4_IV1 " +
                            "(KIP-1333) so that LogConfig.shouldUseLargeIndexFormat() returns " +
                            "true at segment-roll time, and the segment must be rolled (or the " +
                            "broker restarted) so that newly-created indexes are opened in the " +
                            "12-byte format.");
                    }
                    mmap().putInt((int) position);
                }
                incrementEntries();
                lastOffset = offset;
                if (entries() * indexEntrySize != mmap().position())
                    throw new IllegalStateException(entries() + " entries but file position in index is " + mmap().position());
            } else
                throw new InvalidOffsetException("Attempt to append an offset " + offset + " to position " + entries() +
                    " no larger than the last offset appended (" + lastOffset + ") to " + file().getAbsolutePath());
        });
    }

    @Override
    public void truncateTo(long offset) {
        inLock(() -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY);

            /* There are 3 cases for choosing the new size
             * 1) if there is no entry in the index <= the offset, delete everything
             * 2) if there is an entry for this exact offset, delete it and everything larger than it
             * 3) if there is no entry for this offset, delete everything larger than the next smallest
             */
            int newEntries;
            if (slot < 0)
                newEntries = 0;
            else if (relativeOffset(idx, slot) == offset - baseOffset())
                newEntries = slot;
            else
                newEntries = slot + 1;
            truncateToEntries(newEntries);
        });
    }

    public long lastOffset() {
        return lastOffset;
    }

    @Override
    public void truncate() {
        truncateToEntries(0);
    }

    @Override
    protected int entrySize() {
        return indexEntrySize;
    }

    @Override
    protected OffsetPosition parseEntry(ByteBuffer buffer, int n) {
        return new OffsetPosition(baseOffset() + relativeOffset(buffer, n), physical(buffer, n));
    }

    private int relativeOffset(ByteBuffer buffer, int n) {
        return buffer.getInt(n * indexEntrySize);
    }

    private long physical(ByteBuffer buffer, int n) {
        if (indexEntrySize == LARGE_ENTRY_SIZE) {
            return buffer.getLong(n * indexEntrySize + 4);
        } else {
            // Legacy positions were written via putInt((int) position) in append(), so the
            // on-disk value is a signed int that is always non-negative (positions <=
            // Integer.MAX_VALUE). A signed widening read is therefore correct and matches
            // what append() wrote. validateEntries() applies the same signed read for
            // consistency; do not change one without the other.
            return buffer.getInt(n * indexEntrySize + 4);
        }
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(int entries) {
        inLock(() -> {
            super.truncateToEntries0(entries);
            this.lastOffset = lastEntry().offset();
            log.debug("Truncated index {} to {} entries; position is now {} and last offset is now {}",
                file().getAbsolutePath(), entries, mmap().position(), lastOffset);
        });
    }

    /**
     * The last entry in the index
     */
    private OffsetPosition lastEntry() {
        return inRemapReadLock(() -> {
            int entries = entries();
            if (entries == 0)
                return new OffsetPosition(baseOffset(), 0);
            else
                return parseEntry(mmap(), entries - 1);
        });
    }
}
