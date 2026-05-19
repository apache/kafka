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

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LazyIndexTest {

    @Test
    public void forOffsetReturnsOffsetIndexOnGet() throws IOException {
        File file = nonExistentTempFile();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000)) {
            assertEquals(file, idx.file());
            OffsetIndex loaded = idx.get();
            assertNotNull(loaded);
            assertEquals(0L, loaded.baseOffset());
        }
    }

    @Test
    public void forTimeReturnsTimeIndexOnGet() throws IOException {
        File file = nonExistentTempFile();
        try (LazyIndex<TimeIndex> idx = LazyIndex.forTime(file, 0L, 1500)) {
            assertEquals(file, idx.file());
            TimeIndex loaded = idx.get();
            assertNotNull(loaded);
            assertEquals(0L, loaded.baseOffset());
        }
    }

    @Test
    public void getReturnsSameInstanceOnSubsequentCalls() throws IOException {
        File file = nonExistentTempFile();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000)) {
            OffsetIndex first = idx.get();
            OffsetIndex second = idx.get();
            assertSame(first, second);
        }
    }

    @Test
    public void getDoesNotLoadUntilCalled() throws IOException {
        File file = nonExistentTempFile();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000)) {
            // file() and updateParentDir do not trigger load; the underlying file is not yet created
            assertEquals(file, idx.file());
            assertFalse(file.exists(), "Index file should not exist before get() triggers load");
            idx.get();
            assertTrue(file.exists(), "Index file should exist after get() triggers load");
        }
    }

    @Test
    public void getWrapsIOExceptionAsUncheckedWhenParentDirMissing() throws IOException {
        File badFile = new File(nonExistentTempDir(), "0.index");
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(badFile, 0L, 1000);
        UncheckedIOException ex = assertThrows(UncheckedIOException.class, idx::get);
        assertTrue(ex.getMessage().contains("Error loading index file"),
                "Message should describe the failure, got: " + ex.getMessage());
        assertNotNull(ex.getCause(), "Cause should be the underlying IOException");
        assertInstanceOf(IOException.class, ex.getCause());
    }

    @Test
    public void renameToBeforeLoadMovesFileAndUpdatesPath() throws IOException {
        File initial = nonExistentTempFile();
        File renamed = nonExistentTempFile();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(initial, 0L, 1000)) {
            idx.get(); // create the on-disk file
            idx.renameTo(renamed);
            assertEquals(renamed, idx.file());
            assertTrue(renamed.exists());
        }
    }

    @Test
    public void renameToToleratesAlreadyDeletedSource() throws IOException {
        File initial = nonExistentTempFile();
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(initial, 0L, 1000);
        // Pre-load state is IndexFile; do NOT call get() so the on-disk file is never created.
        File renamed = nonExistentTempFile();
        // Source does not exist; renameTo should swallow NoSuchFileException and still update the path.
        assertDoesNotThrow(() -> idx.renameTo(renamed));
        assertEquals(renamed, idx.file());
    }

    @Test
    public void deleteIfExistsReturnsFalseWhenFileAbsent() throws IOException {
        File file = nonExistentTempFile();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000)) {
            assertFalse(idx.deleteIfExists());
        }
    }

    @Test
    public void deleteIfExistsRemovesFileAfterLoad() throws IOException {
        File file = nonExistentTempFile();
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000);
        idx.get();
        assertTrue(file.exists());
        assertTrue(idx.deleteIfExists());
        assertFalse(file.exists());
    }

    @Test
    public void updateParentDirAdjustsFilePath() throws IOException {
        File initialFile = nonExistentTempFile();
        File newParent = TestUtils.tempDirectory();
        try (LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(initialFile, 0L, 1000)) {
            idx.updateParentDir(newParent);
            assertEquals(new File(newParent, initialFile.getName()), idx.file());
        }
    }

    @Test
    public void closeIsIdempotentBeforeAndAfterLoad() throws IOException {
        File file = nonExistentTempFile();
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(file, 0L, 1000);
        assertDoesNotThrow(idx::close); // close before any get()
        File file2 = nonExistentTempFile();
        LazyIndex<OffsetIndex> idx2 = LazyIndex.forOffset(file2, 0L, 1000);
        idx2.get();
        assertDoesNotThrow(idx2::close); // close after get()
    }

    private static File nonExistentTempFile() throws IOException {
        File file = TestUtils.tempFile();
        Files.deleteIfExists(file.toPath());
        return file;
    }

    private static File nonExistentTempDir() {
        File dir = TestUtils.tempDirectory();
        return new File(dir, "this-subdir-does-not-exist");
    }
}
