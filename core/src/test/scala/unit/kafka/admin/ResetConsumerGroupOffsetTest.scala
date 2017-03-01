package unit.kafka.admin

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDateTime}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Collections, Properties}

import kafka.admin.AdminUtils
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.{GroupCoordinatorNotAvailableException, WakeupException}
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
  override def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    props.setProperty("group.id", group)
  }

  @Test
  def testResetOffsetsNotExistingGroup() {
    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "missing.group", "--all-topics")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    TestUtils.waitUntilTrue(() => {
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted == Map.empty
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.")

    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToDateTime() {
    AdminUtils.createTopic(zkUtils, topic1, 1, 1)
    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)

    val checkpoint = LocalDateTime.now()

    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)


    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    val executor = new ConsumerGroupExecutor(brokerList, 1, group, topic1)

    TestUtils.waitUntilTrue(() => {
      try {
        val (_, assignmentsOption) = consumerGroupCommand.describeGroup()
        assignmentsOption match {
          case Some(assignments) => {
            val sumOffset = assignments.filter(_.topic.exists(_ == topic1))
              .filter(_.offset.isDefined)
              .map(assignment => assignment.offset.get)
              .foldLeft(0.toLong)(_ + _)
            sumOffset == 100
          }
        }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.", 30000) //TODO fix description

    executor.shutdown()

    val cgcArgs1 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-datetime", checkpoint.toString, "--execute")
    val opts1 = new ConsumerGroupCommandOptions(cgcArgs1)
    val consumerGroupCommand1 = new KafkaConsumerGroupService(opts1)

    TestUtils.waitUntilTrue(() => {
      try {
        val assignmentsResetted = consumerGroupCommand1.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 50 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsByDuration() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", Duration.ofMinutes(1).toString, "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsByDurationToEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", Duration.ofMillis(1).toString, "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 100 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 200 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 100 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToSpecificOffset() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-offset", "1", "--execute")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    AdminUtils.createTopic(zkUtils, topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)


    TestUtils.waitUntilTrue(() => {
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 1 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 150 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 50 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 0 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists({ assignment => assignment._2 == 200 })
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 && assignment._1.partition.get == 1 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

    AdminUtils.deleteTopic(zkUtils, topic1)
    consumerGroupCommand.close()
  }

  private def produceConsumeAndShutdown(consumerGroupCommand: KafkaConsumerGroupService, numConsumers: Int = 1, topic: String, totalMessages: Int) {
    TestUtils.produceMessages(servers, topic, totalMessages, acks = 1, 100 * 1000)
    val executor = new ConsumerGroupExecutor(brokerList, numConsumers, group, topic)


    TestUtils.waitUntilTrue(() => {
      try {
        val (_, assignmentsOption) = consumerGroupCommand.describeGroup()
        assignmentsOption match {
          case Some(assignments) => {
            val sumOffset = assignments.filter(_.topic.exists(_ == topic))
              .filter(_.offset.isDefined)
              .map(assignment => assignment.offset.get)
              .foldLeft(0.toLong)(_ + _)
            sumOffset == totalMessages
          }
        }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.", 30000) //TODO fix description

    executor.shutdown()
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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 && assignment._1.topic.get == topic1 } &&
          assignmentsResetted.exists { assignment => assignment._2 == 0 && assignment._1.topic.get == topic2 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

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
      try {
        val assignmentsResetted = consumerGroupCommand.resetOffsets()
        assignmentsResetted.exists { assignment => assignment._2 == 0 && assignment._1.partition.get == 1 && assignment._1.topic.get == topic1 }
        assignmentsResetted.exists { assignment => assignment._2 == 0 && assignment._1.partition.get == 1 && assignment._1.topic.get == topic2 }
      } catch {
        case _: GroupCoordinatorNotAvailableException | _: IllegalArgumentException =>
          // Do nothing while the group initializes
          false
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }
    }, "Expected the state to be 'Dead' with no members in the group.") //TODO fix description

    AdminUtils.deleteTopic(zkUtils, topic1)
    AdminUtils.deleteTopic(zkUtils, topic2)
    consumerGroupCommand.close()
  }

  @Test
  def testExportResetOffsetsPlan() {
    //TODO
  }

  @Test
  def testResetOffsetsFromFile() {
    //TODO
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
      case e: WakeupException => // OK
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
  var consumers = List[ConsumerThread]()

  for (i <- 1 to numConsumers) {
    val consumer = new ConsumerThread(broker, i, groupId, topic)
    consumers ++= List(consumer)
    executor.submit(consumer)
  }

  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run() {
      shutdown()
    }
  })

  def shutdown() {
    consumers.foreach(_.shutdown)
    executor.shutdown();
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }
}
