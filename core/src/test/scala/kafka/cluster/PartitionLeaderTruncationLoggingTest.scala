/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import java.io.File
import java.nio.file.Files
import java.util.Properties
import kafka.api.ApiVersion
import kafka.log.{Log, LogConfig, LogManager}
import kafka.server._
import kafka.utils.{LogCaptureAppender, MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.apache.log4j.spi.LoggingEvent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.Map

/**
 * Verifies that Partition emits a structured warn log line when a leader truncates,
 * and stays silent for follower / future-log truncations.
 */
final class PartitionLeaderTruncationLoggingTest {

  private val time = new MockTime()
  private val localBrokerId = 1
  private var appender: LogCaptureAppender = _

  @BeforeEach
  def setUp(): Unit = {
    appender = LogCaptureAppender.createAndRegister()
  }

  @AfterEach
  def tearDown(): Unit = {
    LogCaptureAppender.unregister(appender)
  }

  // --------------- helpers ---------------

  private def newLogManagerFor(logs: Map[TopicPartition, Log]): LogManager = {
    val lm = Mockito.mock(classOf[LogManager])

    Mockito
      .doAnswer { inv =>
        inv.getArgument(0).asInstanceOf[Map[TopicPartition, Long]]
          .foreach { case (tp, off) => logs(tp).truncateTo(off) }
        null
      }
      .when(lm)
      .truncateTo(ArgumentMatchers.any(), ArgumentMatchers.anyBoolean())

    Mockito
      .doAnswer { inv =>
        val tp = inv.getArgument(0, classOf[TopicPartition])
        val off = inv.getArgument(1, classOf[java.lang.Long]).longValue()
        logs(tp).truncateFullyAndStartAt(off)
        null
      }
      .when(lm)
      .truncateFullyAndStartAt(
        ArgumentMatchers.any(classOf[TopicPartition]),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyBoolean()
      )

    lm
  }

  private def newTestLog(tp: TopicPartition, baseDir: File, time: Time): Log = {
    val partDir = new File(baseDir, Log.logDirName(tp))
    Files.createDirectories(partDir.toPath)
    Log.apply(
      dir = partDir,
      config = LogConfig(new Properties()),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = new MockScheduler(time),
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 24 * 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = 60 * 1000,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = false)
  }

  private def appendN(log: Log, n: Int): Unit = {
    (0 until n).foreach { i =>
      val batch = MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(time.milliseconds(), s"k-$i".getBytes, s"v-$i".getBytes))
      log.appendAsLeader(batch, leaderEpoch = 0,
        origin = kafka.log.AppendOrigin.Client, interBrokerProtocolVersion = ApiVersion.latestVersion)
    }
  }

  /** Create a partition wired to a real Log, marked as leader. */
  private def newLeaderPartition(topic: String, numRecords: Int): Partition = {
    val tp = new TopicPartition(topic, 0)
    val log = newTestLog(tp, TestUtils.tempDir(), time)
    appendN(log, numRecords)

    val delayedOps = new DelayedOperations(tp, null, null, null)
    val isrChangeListener = new IsrChangeListener {
      override def markExpand(): Unit = ()
      override def markShrink(): Unit = ()
      override def markFailed(): Unit = ()
    }

    val partition = new Partition(
      topicPartition = tp,
      replicaLagTimeMaxMs = 30000L,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = localBrokerId,
      time = time,
      isrChangeListener = isrChangeListener,
      delayedOperations = delayedOps,
      metadataCache = Mockito.mock(classOf[MetadataCache]),
      logManager = newLogManagerFor(Map(tp -> log)),
      alterIsrManager = Mockito.mock(classOf[AlterIsrManager]),
      transferLeaderManager = Mockito.mock(classOf[TransferLeaderManager])
    )

    partition.setLog(log, isFutureLog = false)
    partition.leaderReplicaIdOpt = Some(localBrokerId)
    partition
  }

  private def findLeaderTruncationLine(): Option[String] = {
    val it = appender.getMessages.iterator
    while (it.hasNext) {
      val msg = it.next().asInstanceOf[LoggingEvent].getRenderedMessage
      if (msg != null && msg.contains("Partition leader brokerId=")) return Some(msg)
    }
    None
  }

  private def findLeaderTruncationLineOrFail(): String = {
    findLeaderTruncationLine().getOrElse {
      val captured = new StringBuilder("Captured logs:\n")
      val it = appender.getMessages.iterator
      while (it.hasNext)
        captured.append(String.valueOf(it.next().asInstanceOf[LoggingEvent].getRenderedMessage)).append('\n')
      fail("Expected leader truncation line\n" + captured.toString)
    }
  }

  private def assertValidTruncationLine(line: String, expectedOperation: String): Unit = {
    assertTrue(line.contains(s"operation=$expectedOperation"))
    assertTrue(line.contains(s"brokerId=$localBrokerId"))

    val fromLogEndOffset = """previousLogEndOffset=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val toLogEndOffset = """newLogEndOffset=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val messagesRemoved = """messagesRemoved=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get

    assertEquals(fromLogEndOffset - toLogEndOffset, messagesRemoved, "messagesRemoved must equal previousLogEndOffset - newLogEndOffset")
  }

  // --------------- tests ---------------

  @Test
  def leader_truncateTo_logs_structured_line(): Unit = {
    val partition = newLeaderPartition("lt-topic", numRecords = 5)
    partition.truncateTo(3L, isFuture = false)

    val line = findLeaderTruncationLineOrFail()
    assertTrue(line.contains("topicPartition=lt-topic-0"))
    assertValidTruncationLine(line, expectedOperation = "truncateTo")
  }

  @Test
  def leader_truncateFullyAndStartAt_logs_structured_line(): Unit = {
    val partition = newLeaderPartition("lt-topic-full", numRecords = 4)
    partition.truncateFullyAndStartAt(2L, isFuture = false)

    assertValidTruncationLine(findLeaderTruncationLineOrFail(), expectedOperation = "truncateFullyAndStartAt")
  }

  @Test
  def future_log_truncation_is_not_logged(): Unit = {
    val partition = newLeaderPartition("lt-topic-future", numRecords = 3)
    partition.truncateTo(2L, isFuture = true)

    assertTrue(findLeaderTruncationLine().isEmpty,
      "No leader truncation line should be logged for future log truncation")
  }

  @Test
  def follower_truncation_is_not_logged(): Unit = {
    val partition = newLeaderPartition("lt-topic-follower", numRecords = 4)
    partition.leaderReplicaIdOpt = Some(localBrokerId + 1) // demote to follower
    assertFalse(partition.isLeader)

    partition.truncateTo(2L, isFuture = false)

    assertTrue(findLeaderTruncationLine().isEmpty,
      "No leader truncation line should be logged when broker is a follower")
  }
}
