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
package unit.kafka.admin

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Calendar, Collections, Date, Properties}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}
import kafka.admin.{AdminUtils, ConsumerGroupCommand}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.{Before, Test}

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
class ResetConsumerGroupOffsetTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  val topic1 = "foo1"
  val topic2 = "foo2"
  val group = "test.group"
  val props = new Properties

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    props.setProperty("group.id", group)
  }

  @Test
  def testResetOffsetsNotExistingGroup() {
    new ConsumerGroupExecutor(brokerList, 1, group, topic1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "missing.group", "--all-topics")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset == Map.empty
    }, "Expected to have an empty assignations map.")

    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToLocalDateTime() {
    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val checkpoint = new Date()
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)


    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic1)

    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.describeGroup()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = assignments.filter(_.topic.exists(_ == topic1))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get)
            .foldLeft(0.toLong)(_ + _)
          sumOffset == 100
        case _ => false
      }
    }, "Expected that consumer group has consumed all messages from topic/partition.")

    executor.shutdown()

    val cgcArgs1 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-datetime", format.format(calendar.getTime), "--execute")
    val opts1 = new ConsumerGroupCommandOptions(cgcArgs1)
    val consumerGroupCommand1 = new KafkaConsumerGroupService(opts1)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand1.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to when offset was 50.")

    printConsumerGroup()

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToZonedDateTime() {
    AdminUtils.createTopic(zkUtils, topic1, 1, 1)
    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
    val checkpoint = new Date()

    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic1)

    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.describeGroup()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = (assignments.filter(_.topic.exists(_ == topic1))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get) foldLeft 0.toLong)(_ + _)
          sumOffset == 100
        case _ => false
      }
    }, "Expected that consumer group has consumed all messages from topic/partition.")

    executor.shutdown()

    val cgcArgs1 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-datetime", format.format(checkpoint), "--execute")
    val opts1 = new ConsumerGroupCommandOptions(cgcArgs1)
    val consumerGroupCommand1 = new KafkaConsumerGroupService(opts1)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand1.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 50 }
    }, "Expected the consumer group to reset to when offset was 50.")

    printConsumerGroup()

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsByDuration() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", "PT1M", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
        val assignmentsToReset = consumerGroupCommand.resetOffsets()
        assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest by duration).")

    printConsumerGroup()

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsByDurationToEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", "PT0.1S", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 100 }
    }, "Expected the consumer group to reset to offset 100 (latest by duration).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-earliest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToLatest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-latest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 200 })
    }, "Expected the consumer group to reset to offset 200 (latest).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToCurrentOffset() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 100 })
    }, "Expected the consumer group to reset to offset 100 (current).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  private def produceConsumeAndShutdown(consumerGroupCommand: KafkaConsumerGroupService, numConsumers: Int = 1, topic: String, totalMessages: Int) {
    TestUtils.produceMessages(servers, topic, totalMessages, acks = 1, 100 * 1000)
    val executor = new ConsumerGroupExecutor(brokerList, numConsumers, group, topic)


    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.describeGroup()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = assignments.filter(_.topic.exists(_ == topic))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get)
            .foldLeft(0.toLong)(_ + _)
          sumOffset == totalMessages
        case _ => false
      }
    }, "Expected the consumer group to consume all messages from topic.")

    executor.shutdown()
  }

  @Test
  def testResetOffsetsToSpecificOffset() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-offset", "1", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 1 })
    }, "Expected the consumer group to reset to offset 1 (specific offset).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsShiftPlus() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "50", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 150 })
    }, "Expected the consumer group to reset to offset 150 (current + 50).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsShiftMinus() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "-50", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 50 })
    }, "Expected the consumer group to reset to offset 50 (current - 50).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsShiftByLowerThanEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "-150", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 0 })
    }, "Expected the consumer group to reset to offset 0 (earliest by shift).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsShiftByHigherThanLatest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "150", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 200 })
    }, "Expected the consumer group to reset to offset 200 (latest by shift).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopic() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic1, "--to-earliest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopicAndPartition() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", String.format("%s:1", topic1), "--to-earliest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 }
    }, "Expected the consumer group to reset to offset 0 (earliest) in partition 1.")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToEarliestOnTopics() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets",
      "--group", group,
      "--topic", topic1,
      "--topic", topic2,
      "--to-earliest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)
    AdminUtils.createTopic(zkUtils, topic2, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)
    produceConsumeAndShutdown(consumerGroupCommand, 1, topic2, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.topic() == topic1 } &&
        assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.topic() == topic2 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    AdminUtils.deleteTopic(zkUtils, topic2)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToEarliestOnTopicsAndPartitions() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets",
      "--group", group,
      "--topic", String.format("%s:1", topic1),
      "--topic", String.format("%s:1", topic2),
      "--to-earliest", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 2, 1)
    AdminUtils.createTopic(zkUtils, topic2, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)
    produceConsumeAndShutdown(consumerGroupCommand, 2, topic2, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 && assignment._1.topic() == topic1 }
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 && assignment._1.topic() == topic2 }
    }, "Expected the consumer group to reset to offset 0 (earliest) in partition 1.")

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    AdminUtils.deleteTopic(zkUtils, topic2)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsExportImportPlan() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-earliest", "--export")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)

    val file = File.createTempFile("reset", ".csv")

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(consumerGroupCommand.exportOffsetsToReset(assignmentsToReset))
      bw.close()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 } && file.exists()
    }, "Expected the consume all messages and save reset offsets plan to file")


    val cgcArgsExec = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-earliest", "--from-file", file.getCanonicalPath)
    val optsExec = new ConsumerGroupCommandOptions(cgcArgsExec)
    val consumerGroupCommandExec = new KafkaConsumerGroupService(optsExec)


    TestUtils.waitUntilTrue(() => {
        val assignmentsToReset = consumerGroupCommandExec.resetOffsets()
        assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest) by file.")

    file.deleteOnExit()

    printConsumerGroup()
    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  private def printConsumerGroup() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--group", group, "--describe")
    ConsumerGroupCommand.main(cgcArgs)
  }

}


class ConsumerThread(broker: String, id: Int, groupId: String, topic: String) extends Runnable {
  val props = new Properties
  props.put("bootstrap.servers", broker)
  props.put("group.id", groupId)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  val consumer = new KafkaConsumer(props)

  def run() {
    try {
      consumer.subscribe(Collections.singleton(topic))
      while (true)
        consumer.poll(Long.MaxValue)
    } catch {
      case _: WakeupException => // OK
    } finally {
      consumer.close()
    }
  }

  def shutdown() {
    consumer.wakeup()
  }
}

class ConsumerGroupExecutor(broker: String, numConsumers: Int, groupId: String, topic: String) {
  val executor: ExecutorService = Executors.newFixedThreadPool(numConsumers)
  var consumers: List[ConsumerThread] = List[ConsumerThread]()

  for (i <- 1 to numConsumers) {
    val consumer = new ConsumerThread(broker, i, groupId, topic)
    consumers ++= List(consumer)
    executor.submit(consumer)
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      shutdown()
    }
  })

  def shutdown() {
    consumers.foreach(_.shutdown())
    executor.shutdown()
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }
}
