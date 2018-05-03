/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Properties}

import kafka.admin.ConsumerGroupCommand.ConsumerGroupService
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class TimeConversionTests {

  @Test
  def testDateTimeFormats(): Unit = {
    //check valid formats
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    //check some invalid formats
    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }

    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }
  }

  private def invokeGetDateTimeMethod(format: SimpleDateFormat): Unit = {
    val checkpoint = new Date()
    val timestampString = format.format(checkpoint)
    ConsumerGroupCommand.convertTimestamp(timestampString)
  }

}

/**
  * Test cases by:
  * - Non-existing consumer group
  * - One for each scenario, with scope=all-topics
  * - scope=one topic, scenario=to-earliest
  * - scope=one topic+partitions, scenario=to-earliest
  * - scope=topics, scenario=to-earliest
  * - scope=topics+partitions, scenario=to-earliest
  * - export/import
  */
class ResetConsumerGroupOffsetTest extends ConsumerGroupCommandTest {
  
  val overridingProps = new Properties()
  val topic1 = "foo1"
  val topic2 = "foo2"

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))  
  } 

  @Test
  def testResetOffsetsNotExistingGroup(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "missing.group", "--all-topics",
      "--to-current", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)
    val resetOffsets = consumerGroupCommand.resetOffsets()
    assertEquals(Map.empty, resetOffsets)
    assertEquals(resetOffsets, committedOffsets(group = "missing.group"))
  }

  @Test
  def testResetOffsetsNewConsumerExistingTopic(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "new.group", "--topic", topic,
      "--to-offset", "50")
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--dry-run"), expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--execute"), expectedOffset = 50, group = "new.group")
  }

  @Test
  def testResetOffsetsToLocalDateTime(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)

    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic)
    awaitConsumerProgress(count = 100L)
    executor.shutdown()

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-datetime", format.format(calendar.getTime), "--execute")
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToZonedDateTime(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    TestUtils.produceMessages(servers, topic, 50, acks = 1, 100 * 1000)
    val checkpoint = new Date()
    TestUtils.produceMessages(servers, topic, 50, acks = 1, 100 * 1000)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic)
    awaitConsumerProgress(count = 100L)
    executor.shutdown()

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-datetime", format.format(checkpoint), "--execute")
    resetAndAssertOffsets(args, expectedOffset = 50)
  }

  @Test
  def testResetOffsetsByDuration(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--by-duration", "PT1M", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsByDurationToEarliest(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--by-duration", "PT0.1S", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 100)
  }

  @Test
  def testResetOffsetsToEarliest(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-earliest", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToLatest(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-latest", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 200)
  }

  @Test
  def testResetOffsetsToCurrentOffset(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-current", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 100)
  }

  @Test
  def testResetOffsetsToSpecificOffset(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-offset", "1", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 1)
  }

  @Test
  def testResetOffsetsShiftPlus(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "50", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 150)
  }

  @Test
  def testResetOffsetsShiftMinus(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "-50", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 50)
  }

  @Test
  def testResetOffsetsShiftByLowerThanEarliest(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "-150", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsShiftByHigherThanLatest(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "150", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    TestUtils.produceMessages(servers, topic, 100, acks = 1, 100 * 1000)
    resetAndAssertOffsets(args, expectedOffset = 200)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopic(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic,
      "--to-earliest", "--execute")
    produceConsumeAndShutdown(totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopicAndPartition(): Unit = {
    val topic = "bar"
    adminZkClient.createTopic(topic, 2, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic",
      s"$topic:1", "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(totalMessages = 100, numConsumers = 2, topic)
    val priorCommittedOffsets = committedOffsets(topic = topic)

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val expectedOffsets = Map(tp0 -> priorCommittedOffsets(tp0), tp1 -> 0L)
    resetAndAssertOffsetsCommitted(consumerGroupCommand, expectedOffsets, topic)

    adminZkClient.deleteTopic(topic)
  }

  @Test
  def testResetOffsetsToEarliestOnTopics(): Unit = {
    val topic1 = "topic1"
    val topic2 = "topic2"
    adminZkClient.createTopic(topic1, 1, 1)
    adminZkClient.createTopic(topic2, 1, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic1,
      "--topic", topic2, "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(100, 1, topic1)
    produceConsumeAndShutdown(100, 1, topic2)

    val tp1 = new TopicPartition(topic1, 0)
    val tp2 = new TopicPartition(topic2, 0)

    val allResetOffsets = resetOffsets(consumerGroupCommand)
    assertEquals(Map(tp1 -> 0L, tp2 -> 0L), allResetOffsets)
    assertEquals(Map(tp1 -> 0L), committedOffsets(topic1))
    assertEquals(Map(tp2 -> 0L), committedOffsets(topic2))

    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  def testResetOffsetsToEarliestOnTopicsAndPartitions(): Unit = {
    val topic1 = "topic1"
    val topic2 = "topic2"

    adminZkClient.createTopic(topic1, 2, 1)
    adminZkClient.createTopic(topic2, 2, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic",
      s"$topic1:1", "--topic", s"$topic2:1", "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(100, 2, topic1)
    produceConsumeAndShutdown(100, 2, topic2)

    val priorCommittedOffsets1 = committedOffsets(topic1)
    val priorCommittedOffsets2 = committedOffsets(topic2)

    val tp1 = new TopicPartition(topic1, 1)
    val tp2 = new TopicPartition(topic2, 1)
    val allResetOffsets = resetOffsets(consumerGroupCommand)
    assertEquals(Map(tp1 -> 0, tp2 -> 0), allResetOffsets)

    assertEquals(priorCommittedOffsets1 + (tp1 -> 0L), committedOffsets(topic1))
    assertEquals(priorCommittedOffsets2 + (tp2 -> 0L), committedOffsets(topic2))

    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  def testResetOffsetsExportImportPlan(): Unit = {
    val topic = "bar"
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    adminZkClient.createTopic(topic, 2, 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-offset", "2", "--export")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    produceConsumeAndShutdown(100, 2, topic)

    val file = File.createTempFile("reset", ".csv")
    file.deleteOnExit()

    val exportedOffsets = consumerGroupCommand.resetOffsets()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(consumerGroupCommand.exportOffsetsToReset(exportedOffsets))
    bw.close()
    assertEquals(Map(tp0 -> 2L, tp1 -> 2L), exportedOffsets.mapValues(_.offset))

    val cgcArgsExec = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--from-file", file.getCanonicalPath, "--dry-run")
    val consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec)
    val importedOffsets = consumerGroupCommandExec.resetOffsets()
    assertEquals(Map(tp0 -> 2L, tp1 -> 2L), importedOffsets.mapValues(_.offset))

    adminZkClient.deleteTopic(topic)
  }

  private def produceConsumeAndShutdown(totalMessages: Int, numConsumers: Int = 1, topic: String = topic): Unit = {
    TestUtils.produceMessages(servers, topic, totalMessages, acks = 1, 100 * 1000)
    val executor =  addConsumerGroupExecutor(numConsumers, topic)
    awaitConsumerProgress(topic, totalMessages)
    executor.shutdown()
  }

  private def awaitConsumerProgress(topic: String = topic, count: Long): Unit = {
    TestUtils.waitUntilTrue(() => {
      val offsets = committedOffsets(topic).values
      count == offsets.sum
    }, "Expected that consumer group has consumed all messages from topic/partition.")
  }

  private def resetAndAssertOffsets(args: Array[String],
                                           expectedOffset: Long,
                                           group: String = group,
                                           dryRun: Boolean = false): Unit = {
    val consumerGroupCommand = getConsumerGroupService(args)
    try {
      val priorOffsets = committedOffsets(group = group)
      val expectedOffsets = Map(new TopicPartition(topic, 0) -> expectedOffset)
      assertEquals(expectedOffsets, resetOffsets(consumerGroupCommand))
      assertEquals(if (dryRun) priorOffsets else expectedOffsets, committedOffsets(group = group))
    } finally {
      consumerGroupCommand.close()
    }
  }

  private def resetAndAssertOffsetsCommitted(consumerGroupService: ConsumerGroupService,
                                             expectedOffsets: Map[TopicPartition, Long],
                                             topic: String): Unit = {
    val allResetOffsets = resetOffsets(consumerGroupService)
    allResetOffsets.foreach { case (tp, offset) =>
      assertEquals(offset, expectedOffsets(tp))
    }
    assertEquals(expectedOffsets, committedOffsets(topic))
  }

  private def resetOffsets(consumerGroupService: ConsumerGroupService): Map[TopicPartition, Long] = {
    consumerGroupService.resetOffsets().mapValues(_.offset)
  }

}
