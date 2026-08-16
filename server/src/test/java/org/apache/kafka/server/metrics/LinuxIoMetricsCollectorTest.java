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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class LinuxIoMetricsCollectorTest {

    @Test
    public void testReadProcFile() throws IOException {
        TestDirectory testDirectory = new TestDirectory();
        Time time = new MockTime(0L, 100L, 1000L);
        testDirectory.writeProcFile(123L, 456L, 1000L, 2000L, 10L, 20L, 5L);
        LinuxIoMetricsCollector collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath(), time);

        // Test that we can read the values we wrote.
        assertTrue(collector.usable());
        assertEquals(123L, collector.readBytes());
        assertEquals(456L, collector.writeBytes());
        assertEquals(1000L, collector.rchar());
        assertEquals(2000L, collector.wchar());
        assertEquals(10L, collector.syscr());
        assertEquals(20L, collector.syscw());
        assertEquals(5L, collector.cancelledWriteBytes());
        testDirectory.writeProcFile(124L, 457L, 1001L, 2001L, 11L, 21L, 6L);

        // The previous values should still be cached.
        assertEquals(123L, collector.readBytes());
        assertEquals(456L, collector.writeBytes());
        assertEquals(1000L, collector.rchar());
        assertEquals(2000L, collector.wchar());
        assertEquals(10L, collector.syscr());
        assertEquals(20L, collector.syscw());
        assertEquals(5L, collector.cancelledWriteBytes());

        // Update the time, and the values should be re-read.
        time.sleep(1);
        assertEquals(124L, collector.readBytes());
        assertEquals(457L, collector.writeBytes());
        assertEquals(1001L, collector.rchar());
        assertEquals(2001L, collector.wchar());
        assertEquals(11L, collector.syscr());
        assertEquals(21L, collector.syscw());
        assertEquals(6L, collector.cancelledWriteBytes());
    }

    @Test
    public void testAllMetricsWithRealWorldValues() throws IOException {
        TestDirectory testDirectory = new TestDirectory();
        Time time = new MockTime(0L, 100L, 1000L);
        // Simulate real-world values where rchar/wchar are much larger than read_bytes/write_bytes
        // (due to page cache hits)
        testDirectory.writeProcFile(
            1048576L,     // read_bytes: 1 MB actually read from disk
            2097152L,     // write_bytes: 2 MB actually written to disk
            10485760L,    // rchar: 10 MB total reads (9 MB from cache)
            20971520L,    // wchar: 20 MB total writes (18 MB cached)
            150L,         // syscr: 150 read syscalls
            300L,         // syscw: 300 write syscalls
            524288L       // cancelled_write_bytes: 512 KB cancelled
        );
        LinuxIoMetricsCollector collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath(), time);

        assertTrue(collector.usable());
        assertEquals(1048576L, collector.readBytes());
        assertEquals(2097152L, collector.writeBytes());
        assertEquals(10485760L, collector.rchar());
        assertEquals(20971520L, collector.wchar());
        assertEquals(150L, collector.syscr());
        assertEquals(300L, collector.syscw());
        assertEquals(524288L, collector.cancelledWriteBytes());
    }

    @Test
    public void testUnableToReadNonexistentProcFile() throws IOException {
        TestDirectory testDirectory = new TestDirectory();
        Time time = new MockTime(0L, 100L, 1000L);
        LinuxIoMetricsCollector collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath(), time);

        // Test that we can't read the file, since it hasn't been written.
        assertFalse(collector.usable());
    }

    static class TestDirectory {

        public final File baseDir;
        private final Path selfDir;

        TestDirectory() throws IOException {
            baseDir = TestUtils.tempDirectory();
            selfDir = Files.createDirectories(baseDir.toPath().resolve("self"));
        }

        void writeProcFile(long readBytes, long writeBytes, long rchar, long wchar,
                           long syscr, long syscw, long cancelledWriteBytes) throws IOException {
            String bld = "rchar: " + rchar + "\n" +
                         "wchar: " + wchar + "\n" +
                         "syscr: " + syscr + "\n" +
                         "syscw: " + syscw + "\n" +
                         "read_bytes: " + readBytes + "\n" +
                         "write_bytes: " + writeBytes + "\n" +
                         "cancelled_write_bytes: " + cancelledWriteBytes + "\n";
            Files.writeString(selfDir.resolve("io"), bld);
        }
    }
}
