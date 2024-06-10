/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.metrics

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kafka.utils.Logging
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}

@Timeout(120)
class LinuxIoMetricsCollectorTest extends Logging {

  class TestDirectory {
    val baseDir = TestUtils.tempDirectory()
    val selfDir = Files.createDirectories(baseDir.toPath.resolve("self"))

    def writeProcFile(readBytes: Long, writeBytes: Long) = {
      val bld = new StringBuilder()
      bld.append("rchar: 0%n".format())
      bld.append("wchar: 0%n".format())
      bld.append("syschr: 0%n".format())
      bld.append("syscw: 0%n".format())
      bld.append("read_bytes: %d%n".format(readBytes))
      bld.append("write_bytes: %d%n".format(writeBytes))
      bld.append("cancelled_write_bytes: 0%n".format())
      Files.write(selfDir.resolve("io"), bld.toString().getBytes(StandardCharsets.UTF_8))
    }
  }

  @Test
  def testReadProcFile(): Unit = {
    val testDirectory = new TestDirectory()
    val time = new MockTime(100, 1000)
    testDirectory.writeProcFile(123L, 456L)
    val collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath,
      time, logger.underlying)

    // Test that we can read the values we wrote.
    assertTrue(collector.usable())
    assertEquals(123L, collector.readBytes())
    assertEquals(456L, collector.writeBytes())
    testDirectory.writeProcFile(124L, 457L)

    // The previous values should still be cached.
    assertEquals(123L, collector.readBytes())
    assertEquals(456L, collector.writeBytes())

    // Update the time, and the values should be re-read.
    time.sleep(1)
    assertEquals(124L, collector.readBytes())
    assertEquals(457L, collector.writeBytes())
  }

  @Test
  def testUnableToReadNonexistentProcFile(): Unit = {
    val testDirectory = new TestDirectory()
    val time = new MockTime(100, 1000)
    val collector = new LinuxIoMetricsCollector(testDirectory.baseDir.getAbsolutePath,
      time, logger.underlying)

    // Test that we can't read the file, since it hasn't been written.
    assertFalse(collector.usable())
  }
}
