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

package unit.kafka.log

import kafka.log.CleanShutdownFileHandler
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}


@Timeout(value = 10)
class CleanShutdownFileHandlerTest {
  @Test def testCleanShutdownFileBasic(): Unit = {
    val logDir = TestUtils.tempDirectory
    val cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath)
    cleanShutdownFileHandler.write(10L)
    assertTrue(cleanShutdownFileHandler.exists)
    assertEquals(10L, cleanShutdownFileHandler.read)
    cleanShutdownFileHandler.delete()
    assertFalse(cleanShutdownFileHandler.exists)
  }

  @Test def testCleanShutdownFileWrongVersion(): Unit = {
    val logDir = TestUtils.tempDirectory
    val cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath)
    cleanShutdownFileHandler.write(10L, -1)
    assertTrue(cleanShutdownFileHandler.exists)
    assertThrows(classOf[Exception], () => cleanShutdownFileHandler.read)
    cleanShutdownFileHandler.delete()
    assertFalse(cleanShutdownFileHandler.exists)
  }
}