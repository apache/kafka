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

import joptsimple.OptionException
import kafka.admin.ConsumerGroupCommand.ConsumerGroupService
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class TimeConversionTests {

  @Test
  def testDateTimeFormats() {
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

  private def invokeGetDateTimeMethod(format: SimpleDateFormat) {
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
  def testResetOffsetsNotExistingGroup() {
    val group = "missing.group"
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-current", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)
    // Make sure we got a coordinator
    TestUtils.waitUntilTrue(() => {
      consumerGroupCommand.collectGroupState(group).coordinator.host() == "localhost"
    }, "Can't find a coordinator")
    val resetOffsets = consumerGroupCommand.resetOffsets()(group)
    assertEquals(Map.empty, resetOffsets)
    assertEquals(resetOffsets, committedOffsets(group = group))
  }

  @Test
  def testResetOffsetsExistingTopic(): Unit = {
    val group = "new.group"
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic,
      "--to-offset", "50")
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--dry-run"), expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--execute"), expectedOffset = 50)
  }

  @Test
  def testResetOffsetsExistingTopicSelectedGroups(): Unit = {
    produceMessages(topic, 100)
    val groups = (
      for (id <- 1 to 3) yield {
        val group = this.group + id
        val executor = addConsumerGroupExecutor(numConsumers = 1, topic = topic, group = group)
        awaitConsumerProgress(count = 100L, group = group)
        executor.shutdown()
        Array("--group", group)
      }).toArray.flatten
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--topic", topic,
      "--to-offset", "50") ++ groups
    resetAndAssertOffsets(args, expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--dry-run"), expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--execute"), expectedOffset = 50)
  }

  @Test
  def testResetOffsetsExistingTopicAllGroups(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--all-groups", "--topic", topic,
      "--to-offset", "50")
    produceMessages(topic, 100)
    for (group <- 1 to 3 map (group + _)) {
      val executor = addConsumerGroupExecutor(numConsumers = 1, topic = topic, group = group)
      awaitConsumerProgress(count = 100L, group = group)
      executor.shutdown()
    }
    resetAndAssertOffsets(args, expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--dry-run"), expectedOffset = 50, dryRun = true)
    resetAndAssertOffsets(args ++ Array("--execute"), expectedOffset = 50)
  }

  @Test
  def testResetOffsetsAllTopicsAllGroups(): Unit = {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--all-groups", "--all-topics",
      "--to-offset", "50")
    val topics = 1 to 3 map (topic + _)
    val groups = 1 to 3 map (group + _)
    topics foreach (topic => produceMessages(topic, 100))
    for {
      topic <- topics
      group <- groups
    } {
      val executor = addConsumerGroupExecutor(numConsumers = 3, topic = topic, group = group)
      awaitConsumerProgress(topic = topic, count = 100L, group = group)
      executor.shutdown()
    }
    resetAndAssertOffsets(args, expectedOffset = 50, dryRun = true, topics = topics)
    resetAndAssertOffsets(args ++ Array("--dry-run"), expectedOffset = 50, dryRun = true, topics = topics)
    resetAndAssertOffsets(args ++ Array("--execute"), expectedOffset = 50, topics = topics)
  }

  @Test
  def testResetOffsetsToLocalDateTime() {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)

    produceMessages(topic, 100)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic)
    awaitConsumerProgress(count = 100L)
    executor.shutdown()

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-datetime", format.format(calendar.getTime), "--execute")
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToZonedDateTime() {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    produceMessages(topic, 50)
    val checkpoint = new Date()
    produceMessages(topic, 50)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic)
    awaitConsumerProgress(count = 100L)
    executor.shutdown()

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-datetime", format.format(checkpoint), "--execute")
    resetAndAssertOffsets(args, expectedOffset = 50)
  }

  @Test
  def testResetOffsetsByDuration() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--by-duration", "PT1M", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsByDurationToEarliest() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--by-duration", "PT0.1S", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 100)
  }

  @Test
  def testResetOffsetsToEarliest() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-earliest", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToLatest() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-latest", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 200)
  }

  @Test
  def testResetOffsetsToCurrentOffset() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-current", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 100)
  }

  @Test
  def testResetOffsetsToSpecificOffset() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-offset", "1", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 1)
  }

  @Test
  def testResetOffsetsShiftPlus() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "50", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 150)
  }

  @Test
  def testResetOffsetsShiftMinus() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "-50", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 50)
  }

  @Test
  def testResetOffsetsShiftByLowerThanEarliest() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "-150", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsShiftByHigherThanLatest() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--shift-by", "150", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    produceMessages(topic, 100)
    resetAndAssertOffsets(args, expectedOffset = 200)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopic() {
    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic,
      "--to-earliest", "--execute")
    produceConsumeAndShutdown(topic, group, totalMessages = 100)
    resetAndAssertOffsets(args, expectedOffset = 0)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopicAndPartition() {
    val topic = "bar"
    createTopic(topic, 2, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic",
      s"$topic:1", "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(topic, group, totalMessages = 100, numConsumers = 2)
    val priorCommittedOffsets = committedOffsets(topic = topic)

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val expectedOffsets = Map(tp0 -> priorCommittedOffsets(tp0), tp1 -> 0L)
    resetAndAssertOffsetsCommitted(consumerGroupCommand, expectedOffsets, topic)

    adminZkClient.deleteTopic(topic)
  }

  @Test
  def testResetOffsetsToEarliestOnTopics() {
    val topic1 = "topic1"
    val topic2 = "topic2"
    createTopic(topic1, 1, 1)
    createTopic(topic2, 1, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic1,
      "--topic", topic2, "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(topic1, group, 100, 1)
    produceConsumeAndShutdown(topic2, group, 100, 1)

    val tp1 = new TopicPartition(topic1, 0)
    val tp2 = new TopicPartition(topic2, 0)

    val allResetOffsets = resetOffsets(consumerGroupCommand)(group).mapValues(_.offset())
    assertEquals(Map(tp1 -> 0L, tp2 -> 0L), allResetOffsets)
    assertEquals(Map(tp1 -> 0L), committedOffsets(topic1))
    assertEquals(Map(tp2 -> 0L), committedOffsets(topic2))

    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  def testResetOffsetsToEarliestOnTopicsAndPartitions() {
    val topic1 = "topic1"
    val topic2 = "topic2"

    createTopic(topic1, 2, 1)
    createTopic(topic2, 2, 1)

    val args = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic",
      s"$topic1:1", "--topic", s"$topic2:1", "--to-earliest", "--execute")
    val consumerGroupCommand = getConsumerGroupService(args)

    produceConsumeAndShutdown(topic1, group, 100, 2)
    produceConsumeAndShutdown(topic2, group, 100, 2)

    val priorCommittedOffsets1 = committedOffsets(topic1)
    val priorCommittedOffsets2 = committedOffsets(topic2)

    val tp1 = new TopicPartition(topic1, 1)
    val tp2 = new TopicPartition(topic2, 1)
    val allResetOffsets = resetOffsets(consumerGroupCommand)(group).mapValues(_.offset())
    assertEquals(Map(tp1 -> 0, tp2 -> 0), allResetOffsets)

    assertEquals(priorCommittedOffsets1 + (tp1 -> 0L), committedOffsets(topic1))
    assertEquals(priorCommittedOffsets2 + (tp2 -> 0L), committedOffsets(topic2))

    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  // This one deals with old CSV export/import format for a single --group arg: "topic,partition,offset" to support old behavior
  def testResetOffsetsExportImportPlanSingleGroupArg() {
    val topic = "bar"
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    createTopic(topic, 2, 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-offset", "2", "--export")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    produceConsumeAndShutdown(topic = topic, group = group, totalMessages = 100, numConsumers = 2)

    val file = File.createTempFile("reset", ".csv")
    file.deleteOnExit()

    val exportedOffsets = consumerGroupCommand.resetOffsets()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets))
    bw.close()
    assertEquals(Map(tp0 -> 2L, tp1 -> 2L), exportedOffsets(group).mapValues(_.offset))

    val cgcArgsExec = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--from-file", file.getCanonicalPath, "--dry-run")
    val consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec)
    val importedOffsets = consumerGroupCommandExec.resetOffsets()
    assertEquals(Map(tp0 -> 2L, tp1 -> 2L), importedOffsets(group).mapValues(_.offset))

    adminZkClient.deleteTopic(topic)
  }

  @Test
  // This one deals with universal CSV export/import file format "group,topic,partition,offset",
  // supporting multiple --group args or --all-groups arg
  def testResetOffsetsExportImportPlan() {
    val group1 = group + "1"
    val group2 = group + "2"
    val topic1 = "bar1"
    val topic2 = "bar2"
    val t1p0 = new TopicPartition(topic1, 0)
    val t1p1 = new TopicPartition(topic1, 1)
    val t2p0 = new TopicPartition(topic2, 0)
    val t2p1 = new TopicPartition(topic2, 1)
    createTopic(topic1, 2, 1)
    createTopic(topic2, 2, 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group1, "--group", group2, "--all-topics",
      "--to-offset", "2", "--export")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    produceConsumeAndShutdown(topic = topic1, group = group1, totalMessages = 100, numConsumers = 2)
    produceConsumeAndShutdown(topic = topic2, group = group2, totalMessages = 100, numConsumers = 5)

    val file = File.createTempFile("reset", ".csv")
    file.deleteOnExit()

    val exportedOffsets = consumerGroupCommand.resetOffsets()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets))
    bw.close()
    assertEquals(Map(t1p0 -> 2L, t1p1 -> 2L), exportedOffsets(group1).mapValues(_.offset))
    assertEquals(Map(t2p0 -> 2L, t2p1 -> 2L), exportedOffsets(group2).mapValues(_.offset))

    // Multiple --group's offset import
    val cgcArgsExec = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group1, "--group", group2, "--all-topics",
      "--from-file", file.getCanonicalPath, "--dry-run")
    val consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec)
    val importedOffsets = consumerGroupCommandExec.resetOffsets()
    assertEquals(Map(t1p0 -> 2L, t1p1 -> 2L), importedOffsets(group1).mapValues(_.offset))
    assertEquals(Map(t2p0 -> 2L, t2p1 -> 2L), importedOffsets(group2).mapValues(_.offset))

    // Single --group offset import using "group,topic,partition,offset" csv format
    val cgcArgsExec2 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group1, "--all-topics",
      "--from-file", file.getCanonicalPath, "--dry-run")
    val consumerGroupCommandExec2 = getConsumerGroupService(cgcArgsExec2)
    val importedOffsets2 = consumerGroupCommandExec2.resetOffsets()
    assertEquals(Map(t1p0 -> 2L, t1p1 -> 2L), importedOffsets2(group1).mapValues(_.offset))

    adminZkClient.deleteTopic(topic)
  }

  @Test(expected = classOf[OptionException])
  def testResetWithUnrecognizedNewConsumerOption() {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics",
      "--to-offset", "2", "--export")
    getConsumerGroupService(cgcArgs)
  }

  private def produceMessages(topic: String, numMessages: Int): Unit = {
    val records = (0 until numMessages).map(_ => new ProducerRecord[Array[Byte], Array[Byte]](topic,
      new Array[Byte](100 * 1000)))
    TestUtils.produceMessages(servers, records, acks = 1)
  }

  private def produceConsumeAndShutdown(topic: String, group: String, totalMessages: Int, numConsumers: Int = 1) {
    produceMessages(topic, totalMessages)
    val executor = addConsumerGroupExecutor(numConsumers = numConsumers, topic = topic, group = group)
    awaitConsumerProgress(topic, group, totalMessages)
    executor.shutdown()
  }

  private def awaitConsumerProgress(topic: String = topic, group: String = group, count: Long): Unit = {
    TestUtils.waitUntilTrue(() => {
      val offsets = committedOffsets(topic = topic, group = group).values
      count == offsets.sum
    }, "Expected that consumer group has consumed all messages from topic/partition. " +
      s"Expected offset: $count. Actual offset: ${committedOffsets(topic, group).values.sum}")
  }

  private def resetAndAssertOffsets(args: Array[String],
                                    expectedOffset: Long,
                                    dryRun: Boolean = false,
                                    topics: Seq[String] = Seq(topic)): Unit = {
    val consumerGroupCommand = getConsumerGroupService(args)
    val expectedOffsets = topics.map(topic => topic -> Map(new TopicPartition(topic, 0) -> expectedOffset)).toMap
    val resetOffsetsResultByGroup = resetOffsets(consumerGroupCommand)

    try {
      for {
        topic <- topics
        (group, partitionInfo) <- resetOffsetsResultByGroup
      } {
        val priorOffsets = committedOffsets(topic = topic, group = group)
        assertEquals(expectedOffsets(topic), partitionInfo.filter(partitionInfo => partitionInfo._1.topic() == topic).mapValues(_.offset))
        assertEquals(if (dryRun) priorOffsets else expectedOffsets(topic), committedOffsets(topic = topic, group = group))
      }
    } finally {
      consumerGroupCommand.close()
    }
  }

  private def resetAndAssertOffsetsCommitted(consumerGroupService: ConsumerGroupService,
                                             expectedOffsets: Map[TopicPartition, Long],
                                             topic: String): Unit = {
    val allResetOffsets = resetOffsets(consumerGroupService)
    for {
      (group, offsetsInfo) <- allResetOffsets
      (tp, offsetMetadata) <- offsetsInfo
    } {
      assertEquals(offsetMetadata.offset(), expectedOffsets(tp))
      assertEquals(expectedOffsets, committedOffsets(topic, group))
    }
  }

  private def resetOffsets(consumerGroupService: ConsumerGroupService) = {
    consumerGroupService.resetOffsets()
  }
}
