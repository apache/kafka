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
        testDirectory.writeProcFile(123L, 456L);
        LinuxIoMetricsCollector collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath(), time);

        // Test that we can read the values we wrote.
        assertTrue(collector.usable());
        assertEquals(123L, collector.readBytes());
        assertEquals(456L, collector.writeBytes());
        testDirectory.writeProcFile(124L, 457L);

        // The previous values should still be cached.
        assertEquals(123L, collector.readBytes());
        assertEquals(456L, collector.writeBytes());

        // Update the time, and the values should be re-read.
        time.sleep(1);
        assertEquals(124L, collector.readBytes());
        assertEquals(457L, collector.writeBytes());
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

        void writeProcFile(long readBytes, long writeBytes) throws IOException {
            String bld = "rchar: 0\n" +
                         "wchar: 0\n" +
                         "syschr: 0\n" +
                         "syscw: 0\n" +
                         "read_bytes: " + readBytes + "\n" +
                         "write_bytes: " + writeBytes + "\n" +
                         "cancelled_write_bytes: 0\n";
            Files.writeString(selfDir.resolve("io"), bld);
        }
    }
}
