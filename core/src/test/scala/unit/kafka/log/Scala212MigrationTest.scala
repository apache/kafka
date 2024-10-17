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

package kafka.log

import kafka.utils.{Pool, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogDirFailureChannel}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.mutable


class Scala212MigrationTest {
  private val time = new MockTime()
  private val logCleaner: LogCleaner = new LogCleaner(new CleanerConfig(true),
    logDirs = Array(TestUtils.tempDir(), TestUtils.tempDir()),
    logs = new Pool[TopicPartition, UnifiedLog](),
    logDirFailureChannel = new LogDirFailureChannel(1),
    time = time)
  private val cleaners = mutable.ArrayBuffer[logCleaner.CleanerThread]()

  private def maxOverCleanerThreadsOldImpl(f: logCleaner.CleanerThread => Double): Int =
    cleaners.foldLeft(0.0d)((max: Double, thread: logCleaner.CleanerThread) => math.max(max, f(thread))).toInt

  private def maxOverCleanerThreadsNewImpl(f: logCleaner.CleanerThread => Double): Int =
    cleaners.map(f).maxOption.getOrElse(0.0d).toInt

  @Test
  def maxOverCleanerThreads(): Unit = {
    val cleaner1 = new logCleaner.CleanerThread(1)
    cleaner1.lastStats = new CleanerStats(time)
    cleaner1.lastStats.bufferUtilization = 0.75d
    cleaners += cleaner1

    val cleaner2 = new logCleaner.CleanerThread(2)
    cleaner2.lastStats = new CleanerStats(time)
    cleaner2.lastStats.bufferUtilization = 0.85d
    cleaners += cleaner2

    val cleaner3 = new logCleaner.CleanerThread(3)
    cleaner3.lastStats = new CleanerStats(time)
    cleaner3.lastStats.bufferUtilization = 0.65d
    cleaners += cleaner3

    assertEquals(0, maxOverCleanerThreadsOldImpl(_.lastStats.bufferUtilization))
    assertEquals(0, maxOverCleanerThreadsNewImpl(_.lastStats.bufferUtilization))
  }
}
