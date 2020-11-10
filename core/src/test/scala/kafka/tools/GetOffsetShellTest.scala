package kafka.tools

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Exit, Logging, TestUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

class GetOffsetShellTest extends KafkaServerTestHarness with Logging {
  private val topicCount = 4
  private val offsetTopicPartitionCount = 4
  private val topicPattern = Pattern.compile("test.*")

  override def generateConfigs: collection.Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect)
    .map(p => {
      p.put(KafkaConfig.OffsetsTopicPartitionsProp, offsetTopicPartitionCount)
      p
    }).map(KafkaConfig.fromProps)

  @Before
  def createTopicAndConsume(): Unit = {
    Range(1, topicCount + 1).foreach(i => createTopic(topicName(i), i))

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "GetOffsetShellTest")

    // Send X messages to each partition of topicX
    val producer = new KafkaProducer[String, String](props)
    Range(1, topicCount + 1).foreach(i => Range(0, i*i)
      .foreach(msgCount => producer.send(new ProducerRecord[String, String](topicName(i), msgCount % i, null, "val" + msgCount))))
    producer.close()

    // Consume so consumer offsets topic is created
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(topicPattern)
    consumer.poll(Duration.ofMillis(1000))
    consumer.commitSync()
    consumer.close()
  }

  @Test
  def testNoFilterOptions(): Unit = {
    val offsets = executeAndParse(Array())
    assertTrue(expectedOffsets() sameElements offsets.filter(r => !isConsumerOffsetTopicPartition(r)))
    assertEquals(offsetTopicPartitionCount, offsets.count(isConsumerOffsetTopicPartition))
  }

  @Test
  def testInternalExcluded(): Unit = {
    val offsets = executeAndParse(Array("--exclude-internal-topics"))
    assertTrue(expectedOffsets() sameElements offsets.filter(r => !isConsumerOffsetTopicPartition(r)))
    assertEquals(0, offsets.count(isConsumerOffsetTopicPartition))
  }

  @Test
  def testTopicNameArg(): Unit = {
    Range(1, topicCount + 1).foreach(i => {
      val offsets = executeAndParse(Array("--topic", topicName(i)))
      assertTrue("Offset output did not match for " + topicName(i), expectedOffsetsForTopic(i) sameElements offsets)
    })
  }

  @Test
  def testTopicPatternArg(): Unit = {
    val offsets = executeAndParse(Array("--topic", "topic.*"))
    assertTrue(expectedOffsets() sameElements offsets)
  }

  @Test
  def testPartitionsArg(): Unit = {
    val offsets = executeAndParse(Array("--partitions", "0,1"))
    assertTrue(expectedOffsets().filter(r => r._2 <= 1) sameElements offsets.filter(r => !isConsumerOffsetTopicPartition(r)))
    assertEquals(2, offsets.count(isConsumerOffsetTopicPartition))
  }

  @Test
  def testTopicPatternArgWithPartitionsArg(): Unit = {
    val offsets = executeAndParse(Array("--topic", "topic.*", "--partitions", "0,1"))
    assertTrue(expectedOffsets().filter(r => r._2 <= 1) sameElements offsets)
  }

  @Test
  def testTopicPartitionsArg(): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "topic1:0,topic2:1,topic(3|4):2,__.*:3"))
    assertTrue(
      Array(
        ("topic1", 0, Some(1)),
        ("topic2", 1, Some(2)),
        ("topic3", 2, Some(3)),
        ("topic4", 2, Some(4))
      )
      sameElements offsets.filter(r => !isConsumerOffsetTopicPartition(r))
    )
    assertEquals(1, offsets.count(isConsumerOffsetTopicPartition))
  }

  @Test
  def testTopicPartitionsArgWithInternalExcluded(): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions",
      "topic1:0,topic2:1,topic(3|4):2,__.*:3", "--exclude-internal-topics"))
    assertTrue(
      Array(
        ("topic1", 0, Some(1)),
        ("topic2", 1, Some(2)),
        ("topic3", 2, Some(3)),
        ("topic4", 2, Some(4))
      )
        sameElements offsets.filter(r => !isConsumerOffsetTopicPartition(r))
    )
    assertEquals(0, offsets.count(isConsumerOffsetTopicPartition))
  }

  @Test
  def testTopicPartitionsNotFoundForNonExistentTopic(): Unit = {
    assertExitCodeIsOne(Array("--topic", "some_nonexistent_topic"))
  }

  @Test
  def testTopicPartitionsNotFoundForExcludedInternalTopic(): Unit = {
    assertExitCodeIsOne(Array("--topic", "some_nonexistent_topic:*"))
  }

  @Test
  def testTopicPartitionsNotFoundForNontMatchingTopicPartitionPattern(): Unit = {
    assertExitCodeIsOne(Array("--topic-partitions", "__consumer_offsets", "--exclude-internal-topics"))
  }

  private def isConsumerOffsetTopicPartition(record: (String, Int, Option[Long])): Boolean = {
    record._1 == "__consumer_offsets"
  }

  private def expectedOffsets(): Array[(String, Int, Option[Long])] = {
    Range(1, topicCount + 1).flatMap(i => expectedOffsetsForTopic(i)).toArray
  }

  private def expectedOffsetsForTopic(i: Int): Array[(String, Int, Option[Long])] = {
    val name = topicName(i)
    Range(0, i).map(p => (name, p, Some(i.toLong))).toArray
  }

  private def topicName(i: Int): String = "topic" + i

  private def assertExitCodeIsOne(args: Array[String]): Unit = {
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }

    try {
      GetOffsetShell.main(addBootstrapServer(args))
    } catch {
      case e: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  private def executeAndParse(args: Array[String]): Array[(String, Int, Option[Long])] = {
    val output = executeAndGrabOutput(args)
    output.split(System.lineSeparator())
      .map(_.split(":"))
      .filter(_.length >= 2)
      .map(line => {
        val topic = line(0)
        val partition = line(1).toInt
        val timestamp = if (line.length == 2 || line(2).isEmpty) None else Some(line(2).toLong)
        (topic, partition, timestamp)
      })
  }

  private def executeAndGrabOutput(args: Array[String]): String = {
    TestUtils.grabConsoleOutput(GetOffsetShell.main(addBootstrapServer(args)))
  }

  private def addBootstrapServer(args: Array[String]): Array[String] = {
    args ++ Array("--bootstrap-server", brokerList)
  }
}
