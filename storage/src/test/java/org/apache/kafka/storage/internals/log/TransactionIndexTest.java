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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionIndexTest {
    private final File file = assertDoesNotThrow(() -> TestUtils.tempFile());
    private final TransactionIndex index = assertDoesNotThrow(() -> new TransactionIndex(0, file));

    @AfterEach
    public void teardown() throws IOException {
        index.close();
    }

    @Test
    public void testPositionSetCorrectlyWhenOpened() throws IOException {
        List<AbortedTxn> abortedTxns = new ArrayList<>(Arrays.asList(
                new AbortedTxn(0L, 0, 10, 11),
                new AbortedTxn(1L, 5, 15, 13),
                new AbortedTxn(2L, 18, 35, 25),
                new AbortedTxn(3L, 32, 50, 40)));
        abortedTxns.forEach(txn -> assertDoesNotThrow(() -> index.append(txn)));
        index.close();

        TransactionIndex reopenedIndex = new TransactionIndex(0L, file);
        AbortedTxn anotherAbortedTxn = new AbortedTxn(3L, 50, 60, 55);
        reopenedIndex.append(anotherAbortedTxn);
        abortedTxns.add(anotherAbortedTxn);
        assertEquals(abortedTxns, reopenedIndex.allAbortedTxns());
    }

    @Test
    public void testSanityCheck() throws IOException {
        List<AbortedTxn> abortedTxns = Arrays.asList(
                new AbortedTxn(0L, 0, 10, 11),
                new AbortedTxn(1L, 5, 15, 13),
                new AbortedTxn(2L, 18, 35, 25),
                new AbortedTxn(3L, 32, 50, 40));
        abortedTxns.forEach(txn -> assertDoesNotThrow(() -> index.append(txn)));
        index.close();

        // open the index with a different starting offset to fake invalid data
        try (TransactionIndex reopenedIndex = new TransactionIndex(100L, file)) {
            assertThrows(CorruptIndexException.class, reopenedIndex::sanityCheck);
        }
    }

    @Test
    public void testLastOffsetMustIncrease() throws IOException {
        index.append(new AbortedTxn(1L, 5, 15, 13));
        assertThrows(IllegalArgumentException.class, () -> index.append(new AbortedTxn(0L, 0,
                15, 11)));
    }

    @Test
    public void testLastOffsetCannotDecrease() throws IOException {
        index.append(new AbortedTxn(1L, 5, 15, 13));
        assertThrows(IllegalArgumentException.class, () -> index.append(new AbortedTxn(0L, 0,
                10, 11)));
    }

    @Test
    public void testCollectAbortedTransactions() {
        List<AbortedTxn> abortedTransactions = Arrays.asList(
                new AbortedTxn(0L, 0, 10, 11),
                new AbortedTxn(1L, 5, 15, 13),
                new AbortedTxn(2L, 18, 35, 25),
                new AbortedTxn(3L, 32, 50, 40));

        abortedTransactions.forEach(txn -> assertDoesNotThrow(() -> index.append(txn)));

        TxnIndexSearchResult result = index.collectAbortedTxns(0L, 100L);
        assertEquals(abortedTransactions, result.abortedTransactions);
        assertFalse(result.isComplete);

        result = index.collectAbortedTxns(0L, 32);
        assertEquals(abortedTransactions.subList(0, 3), result.abortedTransactions);
        assertTrue(result.isComplete);

        result = index.collectAbortedTxns(0L, 35);
        assertEquals(abortedTransactions, result.abortedTransactions);
        assertTrue(result.isComplete);

        result = index.collectAbortedTxns(10, 35);
        assertEquals(abortedTransactions, result.abortedTransactions);
        assertTrue(result.isComplete);

        result = index.collectAbortedTxns(11, 35);
        assertEquals(abortedTransactions.subList(1, 4), result.abortedTransactions);
        assertTrue(result.isComplete);

        result = index.collectAbortedTxns(20, 41);
        assertEquals(abortedTransactions.subList(2, 4), result.abortedTransactions);
        assertFalse(result.isComplete);
    }

    @Test
    public void testTruncate() throws IOException {
        List<AbortedTxn> abortedTransactions = Arrays.asList(
                new AbortedTxn(0L, 0, 10, 2),
                new AbortedTxn(1L, 5, 15, 16),
                new AbortedTxn(2L, 18, 35, 25),
                new AbortedTxn(3L, 32, 50, 40));

        abortedTransactions.forEach(txn -> assertDoesNotThrow(() -> index.append(txn)));

        index.truncateTo(51);
        assertEquals(abortedTransactions, index.collectAbortedTxns(0L, 100L).abortedTransactions);

        index.truncateTo(50);
        assertEquals(abortedTransactions.subList(0, 3), index.collectAbortedTxns(0L, 100L).abortedTransactions);

        index.reset();
        assertEquals(Collections.emptyList(), index.collectAbortedTxns(0L, 100L).abortedTransactions);
    }

    @Test
    public void testAbortedTxnSerde() {
        long pid = 983493L;
        long firstOffset = 137L;
        long lastOffset = 299L;
        long lastStableOffset = 200L;

        AbortedTxn abortedTxn = new AbortedTxn(pid, firstOffset, lastOffset, lastStableOffset);
        assertEquals(AbortedTxn.CURRENT_VERSION, abortedTxn.version());
        assertEquals(pid, abortedTxn.producerId());
        assertEquals(firstOffset, abortedTxn.firstOffset());
        assertEquals(lastOffset, abortedTxn.lastOffset());
        assertEquals(lastStableOffset, abortedTxn.lastStableOffset());
    }

    @Test
    public void testRenameIndex() throws IOException {
        File renamed = TestUtils.tempFile();
        index.append(new AbortedTxn(0L, 0, 10, 2));

        index.renameTo(renamed);
        index.append(new AbortedTxn(1L, 5, 15, 16));

        List<AbortedTxn> abortedTxns = index.collectAbortedTxns(0L, 100L).abortedTransactions;
        assertEquals(2, abortedTxns.size());
        assertEquals(0, abortedTxns.get(0).firstOffset());
        assertEquals(5, abortedTxns.get(1).firstOffset());
    }

    @Test
    public void testUpdateParentDir() {
        File tmpParentDir = new File(TestUtils.tempDirectory(), "parent");
        tmpParentDir.mkdir();
        assertNotEquals(tmpParentDir, index.file().getParentFile());
        index.updateParentDir(tmpParentDir);
        assertEquals(tmpParentDir, index.file().getParentFile());
    }

    @Test
    public void testFlush() throws IOException {
        File nonExistentFile = TestUtils.tempFile();
        assertTrue(nonExistentFile.delete());
        try (TransactionIndex testIndex = new TransactionIndex(0, nonExistentFile)) {
            testIndex.flush();
            testIndex.append(new AbortedTxn(0L, 0, 10, 2));
            testIndex.flush();
            assertNotEquals(0, testIndex.file().length());
        }
    }

    @Test
    public void testDeleteIfExists() throws IOException {
        assertTrue(file.exists());
        index.deleteIfExists();
        assertFalse(file.exists());
    }

    @Test
    public void testIsEmptyWhenFileDoesNotExist() throws IOException {
        File nonExistentFile = TestUtils.tempFile();
        assertTrue(nonExistentFile.delete());
        try (TransactionIndex testIndex = new TransactionIndex(0, nonExistentFile)) {
            assertTrue(testIndex.isEmpty());
        }
    }

    @Test
    public void testIsEmptyWhenFileIsEmpty() {
        assertTrue(index.isEmpty());
    }

    @Test
    public void testIsEmptyWhenFileIsNotEmpty() throws IOException {
        index.append(new AbortedTxn(0L, 0, 10, 2));
        assertFalse(index.isEmpty());
    }
}
