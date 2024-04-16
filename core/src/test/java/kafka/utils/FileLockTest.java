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
package kafka.utils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileLockTest {
    @Test
    void testLock() {
        File tempFile = TestUtils.tempFile();
        FileLock lock1 = new FileLock(tempFile);
        try {
            lock1.lock();
            assertThrows(OverlappingFileLockException.class, lock1::lock);

            FileLock lock2 = new FileLock(tempFile);
            assertThrows(OverlappingFileLockException.class, lock2::lock);
            assertFalse(lock2.tryLock());
            lock1.unlock();
        } finally {
            lock1.destroy();
        }
    }

    @Test
    void testTryLock() {
        File tempFile = TestUtils.tempFile();
        FileLock lock1 = new FileLock(tempFile);
        try {
            assertTrue(lock1.tryLock());
            assertFalse(lock1.tryLock());

            FileLock lock2 = new FileLock(tempFile);
            assertFalse(lock2.tryLock());
            assertThrows(OverlappingFileLockException.class, lock2::lock);
            lock1.unlock();
        } finally {
            lock1.destroy();
        }
    }

    @Test
    void testUnlock() {
        File tempFile = TestUtils.tempFile();
        FileLock lock1 = new FileLock(tempFile);
        try {
            lock1.lock();
            lock1.unlock();
            lock1.lock();
            lock1.unlock();

            assertTrue(lock1.tryLock());
            lock1.unlock();
            assertTrue(lock1.tryLock());
            lock1.unlock();

            FileLock lock2 = new FileLock(tempFile);
            assertTrue(lock2.tryLock());
            lock2.unlock();
            assertDoesNotThrow(lock2::lock);
            lock2.unlock();
        } finally {
            lock1.destroy();
        }
    }

    @Test
    void testDestroy() {
        File tempFile = TestUtils.tempFile();
        FileLock lock1 = new FileLock(tempFile);
        lock1.destroy();
        assertFalse(tempFile.exists());
        assertDoesNotThrow(lock1::destroy);
    }
}
