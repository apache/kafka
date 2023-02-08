package integration.kafka.api

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable.Buffer
import scala.jdk.CollectionConverters._

trait ProducerSendTestHelpers extends KafkaServerTestHarness {

  def baseProps =
    TestUtils.createBrokerConfigs(2, zkConnectOrNull, false, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
      .map { p =>
        p.put(KafkaConfig.NumPartitionsProp, 4.toString)
        p
      }

  protected var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _
  protected val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  protected var admin: Admin = _

  protected val topic = "topic"
  protected val numRecords = 100

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    admin = TestUtils.createAdminClient(brokers, listenerName,
      TestUtils.securityConfigs(Mode.CLIENT,
        securityProtocol,
        trustStoreFile,
        "adminClient",
        TestUtils.SslCertificateCn,
        clientSaslProperties))

    consumer = TestUtils.createConsumer(
      bootstrapServers(listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)),
      securityProtocol = SecurityProtocol.PLAINTEXT
    )
  }

  @AfterEach
  override def tearDown(): Unit = {
    consumer.close()
    // Ensure that all producers are closed since unclosed producers impact other tests when Kafka server ports are reused
    producers.foreach(_.close())
    admin.close()

    super.tearDown()
  }

  protected def createProducer(lingerMs: Int = 0,
                               deliveryTimeoutMs: Int = 2 * 60 * 1000,
                               batchSize: Int = 16384,
                               compressionType: String = "none",
                               maxBlockMs: Long = 60 * 1000L,
                               bufferSize: Long = 1024L * 1024L): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producer = TestUtils.createProducer(
      bootstrapServers(),
      compressionType = compressionType,
      securityProtocol = securityProtocol,
      trustStoreFile = trustStoreFile,
      saslProperties = clientSaslProperties,
      lingerMs = lingerMs,
      deliveryTimeoutMs = deliveryTimeoutMs,
      maxBlockMs = maxBlockMs,
      batchSize = batchSize,
      bufferSize = bufferSize)
    registerProducer(producer)
  }

  protected def registerProducer(producer: KafkaProducer[Array[Byte], Array[Byte]]): KafkaProducer[Array[Byte], Array[Byte]] = {
    producers += producer
    producer
  }

  protected def sendAndVerify(producer: KafkaProducer[Array[Byte], Array[Byte]],
                              numRecords: Int = numRecords,
                              timeoutMs: Long = 20000L): Unit = {
    val partition = 0
    try {
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2)

      val futures = for (i <- 1 to numRecords) yield {
        val record = new ProducerRecord(topic, partition, s"key$i".getBytes(StandardCharsets.UTF_8),
          s"value$i".getBytes(StandardCharsets.UTF_8))
        producer.send(record)
      }
      producer.close(Duration.ofMillis(timeoutMs))
      val lastOffset = futures.foldLeft(0) { (offset, future) =>
        val recordMetadata = future.get
        assertEquals(topic, recordMetadata.topic)
        assertEquals(partition, recordMetadata.partition)
        assertEquals(offset, recordMetadata.offset)
        offset + 1
      }
      assertEquals(numRecords, lastOffset)
    } finally {
      producer.close()
    }
  }

  protected def sendAndVerifyTimestamp(producer: KafkaProducer[Array[Byte], Array[Byte]], timestampType: TimestampType): Unit = {
    val partition = 0

    val baseTimestamp = 123456L
    val startTime = System.currentTimeMillis()

    object callback extends Callback {
      var offset = 0L
      var timestampDiff = 1L

      def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          assertEquals(offset, metadata.offset)
          assertEquals(topic, metadata.topic)
          if (timestampType == TimestampType.CREATE_TIME)
            assertEquals(baseTimestamp + timestampDiff, metadata.timestamp)
          else
            assertTrue(metadata.timestamp >= startTime && metadata.timestamp <= System.currentTimeMillis())
          assertEquals(partition, metadata.partition)
          offset += 1
          timestampDiff += 1
        } else {
          fail(s"Send callback returns the following exception: $exception")
        }
      }
    }

    try {
      // create topic
      val topicProps = new Properties()
      if (timestampType == TimestampType.LOG_APPEND_TIME)
        topicProps.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
      else
        topicProps.setProperty(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime")
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 1, 2, topicConfig = topicProps)

      val recordAndFutures = for (i <- 1 to numRecords) yield {
        val record = new ProducerRecord(topic, partition, baseTimestamp + i, s"key$i".getBytes(StandardCharsets.UTF_8),
          s"value$i".getBytes(StandardCharsets.UTF_8))
        (record, producer.send(record, callback))
      }
      producer.close(Duration.ofSeconds(20L))
      recordAndFutures.foreach { case (record, future) =>
        val recordMetadata = future.get
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          assertTrue(recordMetadata.timestamp >= startTime && recordMetadata.timestamp <= System.currentTimeMillis())
        else
          assertEquals(record.timestamp, recordMetadata.timestamp)
      }
      assertEquals(numRecords, callback.offset, s"Should have offset $numRecords but only successfully sent ${callback.offset}")
    } finally {
      producer.close()
    }
  }

  def defaultRecordAssertions(record: ConsumerRecord[Array[Byte], Array[Byte]], i: Int, now: Long, topic: String, partition: Int) = {
    assertEquals(topic, record.topic)
    assertEquals(partition, record.partition)
    assertEquals(i.toLong, record.offset)
    assertNull(record.key)
    assertEquals(s"value${i + 1}", new String(record.value))
    assertEquals(now, record.timestamp)
  }


  def sendToPartition(quorum: String, expectedRecords: Int = numRecords, recordAssertions: (ConsumerRecord[Array[Byte], Array[Byte]], Int, Long, String, Int) => Unit = defaultRecordAssertions _): Unit = {
    val producer = createProducer()

    try {
      TestUtils.createTopicWithAdmin(admin, topic, brokers, 2, 2)
      val partition = 1

      val now = System.currentTimeMillis()
      val futures = (1 to numRecords).map { i =>
        producer.send(new ProducerRecord(topic, partition, now, null, ("value" + i).getBytes(StandardCharsets.UTF_8)))
      }.map(_.get(30, TimeUnit.SECONDS))

      // make sure all of them end up in the same partition with increasing offset values
      for ((recordMetadata, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset.toLong, recordMetadata.offset)
        assertEquals(topic, recordMetadata.topic)
        assertEquals(partition, recordMetadata.partition)
      }

      consumer.assign(List(new TopicPartition(topic, partition)).asJava)

      // make sure the fetched messages also respect the partitioning and ordering
      val records = TestUtils.consumeRecords(consumer, expectedRecords)

      records.zipWithIndex.foreach { case (record, i) =>
        recordAssertions(record, i, now, topic, partition)
      }

    } finally {
      producer.close()
    }
  }

}
