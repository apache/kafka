/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.util.Properties

import kafka.log.{ClientRecordDeletion, Log, LogConfig, LogManager}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class ReplicaTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  var log: Log = _

  @BeforeEach
  def setup(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    val config = LogConfig(logProps)
    log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))
  }

  @AfterEach
  def tearDown(): Unit = {
    log.close()
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testSegmentDeletionWithHighWatermarkInitialization(): Unit = {
    val expiredTimestamp = time.milliseconds() - 1000
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes, timestamp = expiredTimestamp)
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    val initialHighWatermark = log.updateHighWatermark(25L)
    assertEquals(25L, initialHighWatermark)

    val initialNumSegments = log.numberOfSegments
    log.deleteOldSegments()
    assertTrue(log.numberOfSegments < initialNumSegments)
    assertTrue(log.logStartOffset <= initialHighWatermark)
  }

  @Test
  def testCannotDeleteSegmentsAtOrAboveHighWatermark(): Unit = {
    val expiredTimestamp = time.milliseconds() - 1000
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes, timestamp = expiredTimestamp)
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    // ensure we have at least a few segments so the test case is not trivial
    assertTrue(log.numberOfSegments > 5)
    assertEquals(0L, log.highWatermark)
    assertEquals(0L, log.logStartOffset)
    assertEquals(100L, log.logEndOffset)

    for (hw <- 0 to 100) {
      log.updateHighWatermark(hw)
      assertEquals(hw, log.highWatermark)
      log.deleteOldSegments()
      assertTrue(log.logStartOffset <= hw)

      // verify that all segments up to the high watermark have been deleted

      log.logSegments.headOption.foreach { segment =>
        assertTrue(segment.baseOffset <= hw)
        assertTrue(segment.baseOffset >= log.logStartOffset)
      }
      log.logSegments.tail.foreach { segment =>
        assertTrue(segment.baseOffset > hw)
        assertTrue(segment.baseOffset >= log.logStartOffset)
      }
    }

    assertEquals(100L, log.logStartOffset)
    assertEquals(1, log.numberOfSegments)
    assertEquals(0, log.activeSegment.size)
  }

  @Test
  def testCannotIncrementLogStartOffsetPastHighWatermark(): Unit = {
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    log.updateHighWatermark(25L)
    assertThrows(classOf[OffsetOutOfRangeException], () => log.maybeIncrementLogStartOffset(26L, ClientRecordDeletion))
  }
}
