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

import kafka.utils.{LogCaptureAppender, TestUtils}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

/**
 * Verifies that Partition emits a structured warn log line when a leader truncates,
 * and stays silent for follower / future-log truncations.
 */
final class PartitionLeaderTruncationLoggingTest extends AbstractPartitionTest {

  private var appender: LogCaptureAppender = _

  @BeforeEach
  override def setup(): Unit = {
    super.setup()
    appender = LogCaptureAppender.createAndRegister()
  }

  @AfterEach
  override def tearDown(): Unit = {
    LogCaptureAppender.unregister(appender)
    super.tearDown()
  }

  /** Append n records to the partition log as leader at leaderEpoch 0. */
  private def appendN(n: Int): Unit = {
    val log = partition.leaderLogIfLocal.getOrElse(
      throw new IllegalStateException("Partition must be leader to append"))
    (0 until n).foreach { i =>
      val batch = MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(time.milliseconds(), s"k-$i".getBytes, s"v-$i".getBytes))
      log.appendAsLeader(batch, leaderEpoch = 0,
        origin = AppendOrigin.CLIENT,
        interBrokerProtocolVersion = MetadataVersion.latest)
    }
  }

  private def findLeaderTruncationLine(): Option[String] = {
    appender.getMessages.toSeq.collectFirst {
      case e if e.getRenderedMessage != null && e.getRenderedMessage.contains("Partition leader brokerId=") =>
        e.getRenderedMessage
    }
  }

  private def findLeaderTruncationLineOrFail(): String = {
    findLeaderTruncationLine().getOrElse {
      val captured = appender.getMessages.toSeq.map(e => String.valueOf(e.getRenderedMessage)).mkString("\n")
      fail(s"Expected leader truncation line\nCaptured logs:\n$captured")
    }
  }

  private def assertValidTruncationLine(line: String, expectedOperation: String): Unit = {
    assertTrue(line.contains(s"operation=$expectedOperation"))
    assertTrue(line.contains(s"brokerId=$brokerId"))

    val fromLogEndOffset = """previousLogEndOffset=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val toLogEndOffset = """newLogEndOffset=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val messagesRemoved = """messagesRemoved=(\d+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get

    assertEquals(fromLogEndOffset - toLogEndOffset, messagesRemoved,
      "messagesRemoved must equal previousLogEndOffset - newLogEndOffset")
  }

  // --------------- tests ---------------

  @Test
  def leader_truncateTo_logs_structured_line(): Unit = {
    val tp = topicPartition
    setupPartitionWithMocks(leaderEpoch = 0, isLeader = true)
    appendN(5)

    partition.truncateTo(3L, isFuture = false)

    val line = findLeaderTruncationLineOrFail()
    assertTrue(line.contains(s"topicPartition=${tp.topic}-${tp.partition}"))
    assertValidTruncationLine(line, expectedOperation = "truncateTo")
  }

  @Test
  def leader_truncateFullyAndStartAt_logs_structured_line(): Unit = {
    setupPartitionWithMocks(leaderEpoch = 0, isLeader = true)
    appendN(4)

    partition.truncateFullyAndStartAt(2L, isFuture = false)

    assertValidTruncationLine(findLeaderTruncationLineOrFail(), expectedOperation = "truncateFullyAndStartAt")
  }

  @Test
  def future_log_truncation_is_not_logged(): Unit = {
    setupPartitionWithMocks(leaderEpoch = 0, isLeader = true)
    appendN(3)

    partition.truncateTo(2L, isFuture = true)

    assertTrue(findLeaderTruncationLine().isEmpty,
      "No leader truncation line should be logged for future log truncation")
  }

  @Test
  def follower_truncation_is_not_logged(): Unit = {
    setupPartitionWithMocks(leaderEpoch = 0, isLeader = false)

    assertFalse(partition.isLeader)
    partition.truncateTo(2L, isFuture = false)

    assertTrue(findLeaderTruncationLine().isEmpty,
      "No leader truncation line should be logged when broker is a follower")
  }
}
