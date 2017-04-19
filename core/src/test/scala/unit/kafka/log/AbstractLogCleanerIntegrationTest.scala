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
package unit.kafka.log

import java.io.File
import java.util.Properties

import kafka.log.{CleanerConfig, Log, LogCleaner, LogConfig}
import kafka.server.Defaults
import kafka.utils.{MockTime, Pool, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.After

import scala.collection.mutable.ListBuffer

abstract class AbstractLogCleanerIntegrationTest {

  val logDir = TestUtils.tempDir()
  var cleaner: LogCleaner = _
  val logs = ListBuffer.empty[Log]

  def time: MockTime

  @After
  def teardown(): Unit = {
    if (cleaner != null)
      cleaner.shutdown()
    time.scheduler.shutdown()
    logs.foreach(_.close())
    Utils.delete(logDir)
  }

  def logConfigProperties(propertyOverrides: Properties = new Properties(),
                          maxMessageSize: Int,
                          minCleanableDirtyRatio: Float = 0.0F,
                          compactionLag: Long = Defaults.LogCleanerMinCompactionLagMs,
                          deleteDelay: Int = 1000,
                          segmentSize: Int = 256): Properties = {
    val props = new Properties()
    props.put(LogConfig.MaxMessageBytesProp, maxMessageSize: java.lang.Integer)
    props.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    props.put(LogConfig.SegmentIndexBytesProp, 100*1024: java.lang.Integer)
    props.put(LogConfig.FileDeleteDelayMsProp, deleteDelay: java.lang.Integer)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.MinCleanableDirtyRatioProp, minCleanableDirtyRatio: java.lang.Float)
    props.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
    props.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Long)
    props.putAll(propertyOverrides)
    props
  }

  def makeCleaner(partitions: Iterable[TopicPartition],
                  minCleanableDirtyRatio: Float = 0.0F,
                  numThreads: Int = 1,
                  backOffMs: Long = 15000L,
                  maxMessageSize: Int = 128,
                  compactionLag: Long = Defaults.LogCleanerMinCompactionLagMs,
                  deleteDelay: Int = 1000,
                  segmentSize: Int = 256,
                  propertyOverrides: Properties = new Properties()): LogCleaner = {

    val logMap = new Pool[TopicPartition, Log]()
    for (partition <- partitions) {
      val dir = new File(logDir, s"${partition.topic}-${partition.partition}")
      dir.mkdirs()

      val logConfig = LogConfig(logConfigProperties(propertyOverrides, maxMessageSize,
        minCleanableDirtyRatio, compactionLag, deleteDelay, segmentSize))
      val log = new Log(dir,
        logConfig,
        logStartOffset = 0L,
        recoveryPoint = 0L,
        scheduler = time.scheduler,
        time = time)
      logMap.put(partition, log)
      this.logs += log
    }

    val cleanerConfig = CleanerConfig(
      numThreads = numThreads,
      ioBufferSize = maxMessageSize / 2,
      maxMessageSize = maxMessageSize,
      backOffMs = backOffMs)
    new LogCleaner(cleanerConfig,
      logDirs = Array(logDir),
      logs = logMap,
      time = time)
  }
}
