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
package kafka.api

import java.util.{Locale, Properties}
import kafka.server.KafkaServer
import kafka.utils.{JaasTestUtils, TestUtils}
import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.config.{SaslConfigs, TopicConfig}
import org.apache.kafka.common.errors.{InvalidTopicException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.TestJaasConfig
import org.apache.kafka.server.config.ZkConfigs
import org.apache.kafka.server.config.{ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig, RemoteStorageMetrics}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class MetricsTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 1

  override protected def listenerName = new ListenerName("CLIENT")
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KafkaServerContextName}"
  this.serverConfig.setProperty(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, "false")
  this.serverConfig.setProperty(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false")
  this.serverConfig.setProperty(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "2.8")
  this.producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10")
  // intentionally slow message down conversion via gzip compression to ensure we can measure the time it takes
  this.producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties =
    Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties =
    Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    if (testInfo.getDisplayName.contains("testMetrics") && testInfo.getDisplayName.endsWith("true")) {
      // systemRemoteStorageEnabled is enabled
      this.serverConfig.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
      this.serverConfig.setProperty(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
      this.serverConfig.setProperty(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    }
    verifyNoRequestMetrics("Request metrics not removed in a previous test")
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, kafkaServerJaasEntryName))
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
    verifyNoRequestMetrics("Request metrics not removed in this test")
  }

  /**
   * Verifies some of the metrics of producer, consumer as well as server.
   */
  @nowarn("cat=deprecation")
  @ParameterizedTest(name = "testMetrics with systemRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testMetrics(systemRemoteStorageEnabled: Boolean): Unit = {
    val topic = "topicWithOldMessageFormat"
    val props = new Properties
    props.setProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.9.0")
    createTopic(topic, numPartitions = 1, replicationFactor = 1, props)
    val tp = new TopicPartition(topic, 0)

    // Produce and consume some records
    val numRecords = 10
    val recordSize = 100000
    val prop = new Properties()
    // idempotence producer doesn't support old version of messages
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    val producer = createProducer(configOverrides = prop)
    sendRecords(producer, numRecords, recordSize, tp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    TestUtils.consumeRecords(consumer, numRecords)

    verifyKafkaRateMetricsHaveCumulativeCount(producer, consumer)
    verifyClientVersionMetrics(consumer.metrics, "Consumer")
    verifyClientVersionMetrics(producer.metrics, "Producer")

    val server = servers.head
    verifyBrokerMessageConversionMetrics(server, recordSize, tp)
    verifyBrokerErrorMetrics(servers.head)
    verifyBrokerZkMetrics(server, topic)

    generateAuthenticationFailure(tp)
    verifyBrokerAuthenticationMetrics(server)
    verifyRemoteStorageMetrics(systemRemoteStorageEnabled)
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
      recordSize: Int, tp: TopicPartition): Unit = {
    val bytes = new Array[Byte](recordSize)
    (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, bytes))
    }
    producer.flush()
  }

  // Create a producer that fails authentication to verify authentication failure metrics
  private def generateAuthenticationFailure(tp: TopicPartition): Unit = {
    val saslProps = new Properties()
    saslProps.put(SaslConfigs.SASL_MECHANISM, kafkaClientSaslMechanism)
    saslProps.put(SaslConfigs.SASL_JAAS_CONFIG, TestJaasConfig.jaasConfigProperty(kafkaClientSaslMechanism, "badUser", "badPass"))
    // Use acks=0 to verify error metric when connection is closed without a response
    val producer = TestUtils.createProducer(bootstrapServers(),
      acks = 0,
      requestTimeoutMs = 1000,
      maxBlockMs = 1000,
      securityProtocol = securityProtocol,
      trustStoreFile = trustStoreFile,
      saslProperties = Some(saslProps))

    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "key".getBytes, "value".getBytes)).get
    } catch {
      case _: Exception => // expected exception
    } finally {
      producer.close()
    }
  }

  private def verifyKafkaRateMetricsHaveCumulativeCount(producer: KafkaProducer[Array[Byte], Array[Byte]],
                                                        consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {

    def exists(name: String, rateMetricName: MetricName, allMetricNames: Set[MetricName]): Boolean = {
      allMetricNames.contains(new MetricName(name, rateMetricName.group, "", rateMetricName.tags))
    }

    def verify(rateMetricName: MetricName, allMetricNames: Set[MetricName]): Unit = {
      val name = rateMetricName.name
      val totalExists = exists(name.replace("-rate", "-total"), rateMetricName, allMetricNames)
      val totalTimeExists = exists(name.replace("-rate", "-time"), rateMetricName, allMetricNames)
      assertTrue(totalExists || totalTimeExists, s"No cumulative count/time metric for rate metric $rateMetricName")
    }

    val consumerMetricNames = consumer.metrics.keySet.asScala.toSet
    consumerMetricNames.filter(_.name.endsWith("-rate"))
        .foreach(verify(_, consumerMetricNames))

    val producerMetricNames = producer.metrics.keySet.asScala.toSet
    val producerExclusions = Set("compression-rate") // compression-rate is an Average metric, not Rate
    producerMetricNames.filter(_.name.endsWith("-rate"))
        .filterNot(metricName => producerExclusions.contains(metricName.name))
        .foreach(verify(_, producerMetricNames))

    // Check a couple of metrics of consumer and producer to ensure that values are set
    verifyKafkaMetricRecorded("records-consumed-rate", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("records-consumed-total", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("record-send-rate", producer.metrics, "Producer")
    verifyKafkaMetricRecorded("record-send-total", producer.metrics, "Producer")
  }

  private def verifyClientVersionMetrics(metrics: java.util.Map[MetricName, _ <: Metric], entity: String): Unit = {
    Seq("commit-id", "version").foreach { name =>
      verifyKafkaMetric(name, metrics, entity) { matchingMetrics =>
        assertEquals(1, matchingMetrics.size)
        val metric = matchingMetrics.head
        val value = metric.metricValue
        assertNotNull(value, s"$entity metric not recorded $name")
        assertNotNull(value.isInstanceOf[String] && value.asInstanceOf[String].nonEmpty,
          s"$entity metric $name should be a non-empty String")
        assertTrue(metric.metricName.tags.containsKey("client-id"), "Client-id not specified")
      }
    }
  }

  private def verifyBrokerAuthenticationMetrics(server: KafkaServer): Unit = {
    val metrics = server.metrics.metrics
    TestUtils.waitUntilTrue(() =>
      maxKafkaMetricValue("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics")) > 0,
      "failed-authentication-total not updated")
    verifyKafkaMetricRecorded("successful-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("successful-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
  }

  private def verifyBrokerMessageConversionMetrics(server: KafkaServer, recordSize: Int, tp: TopicPartition): Unit = {
    val requestMetricsPrefix = "kafka.network:type=RequestMetrics"
    val requestBytes = verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Produce")
    val tempBytes = verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=TemporaryMemoryBytes,request=Produce")
    assertTrue(tempBytes >= recordSize, s"Unexpected temporary memory size requestBytes $requestBytes tempBytes $tempBytes")

    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=ProduceMessageConversionsPerSec")
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=MessageConversionsTimeMs,request=Produce", value => value > 0.0)
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Fetch")
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=TemporaryMemoryBytes,request=Fetch", value => value == 0.0)

     // request size recorded for all request types, check one
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Metadata")
  }

  private def verifyBrokerZkMetrics(server: KafkaServer, topic: String): Unit = {
    val histogram = yammerHistogram("kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs")
    // Latency is rounded to milliseconds, so check the count instead
    val initialCount = histogram.count
    servers.head.zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    val newCount = histogram.count
    assertTrue(newCount > initialCount, "ZooKeeper latency not recorded")

    val min = histogram.min
    assertTrue(min >= 0, s"Min latency should not be negative: $min")

    assertEquals("CONNECTED", yammerMetricValue("SessionState"), s"Unexpected ZK state")
  }

  private def verifyBrokerErrorMetrics(server: KafkaServer): Unit = {

    def errorMetricCount = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.count(_.getName == "ErrorsPerSec")

    val startErrorMetricCount = errorMetricCount
    val errorMetricPrefix = "kafka.network:type=RequestMetrics,name=ErrorsPerSec"
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=NONE")

    val consumer = createConsumer()
    try {
      consumer.partitionsFor("12{}!")
    } catch {
      case _: InvalidTopicException => // expected
    }
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=INVALID_TOPIC_EXCEPTION")

    // Check that error metrics are registered dynamically
    val currentErrorMetricCount = errorMetricCount
    assertEquals(startErrorMetricCount + 1, currentErrorMetricCount)
    assertTrue(currentErrorMetricCount < 10, s"Too many error metrics $currentErrorMetricCount")

    try {
      consumer.partitionsFor("non-existing-topic")
    } catch {
      case _: UnknownTopicOrPartitionException => // expected
    }
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=UNKNOWN_TOPIC_OR_PARTITION")
  }

  private def verifyKafkaMetric[T](name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None)(verify: Iterable[Metric] => T) : T = {
    val matchingMetrics = metrics.asScala.filter {
      case (metricName, _) => metricName.name == name && group.forall(_ == metricName.group)
    }
    assertTrue(matchingMetrics.nonEmpty, s"Metric not found $name")
    verify(matchingMetrics.values)
  }

  private def maxKafkaMetricValue(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String]): Double = {
    // Use max value of all matching metrics since Selector metrics are recorded for each Processor
    verifyKafkaMetric(name, metrics, entity, group) { matchingMetrics =>
      matchingMetrics.foldLeft(0.0)((max, metric) => Math.max(max, metric.metricValue.asInstanceOf[Double]))
    }
  }

  private def verifyKafkaMetricRecorded(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None): Unit = {
    val value = maxKafkaMetricValue(name, metrics, entity, group)
    assertTrue(value > 0.0, s"$entity metric not recorded correctly for $name value $value")
  }

  private def yammerMetricValue(name: String): Any = {
    val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
    metric match {
      case m: Meter => m.count.toDouble
      case m: Histogram => m.max
      case m: Gauge[_] => m.value
      case m => fail(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  private def yammerHistogram(name: String): Histogram = {
    val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
    metric match {
      case m: Histogram => m
      case m => throw new AssertionError(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  private def verifyYammerMetricRecorded(name: String, verify: Double => Boolean = d => d > 0): Double = {
    val metricValue = yammerMetricValue(name).asInstanceOf[Double]
    assertTrue(verify(metricValue), s"Broker metric not recorded correctly for $name value $metricValue")
    metricValue
  }

  private def verifyNoRequestMetrics(errorMessage: String): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (n, _) =>
      n.getMBeanName.startsWith("kafka.network:type=RequestMetrics")
    }
    assertTrue(metrics.isEmpty, s"$errorMessage: ${metrics.keys}")
  }

  private def fromNameToBrokerTopicStatsMBean(name: String): String = {
    s"kafka.server:type=BrokerTopicMetrics,name=$name"
  }

  private def verifyRemoteStorageMetrics(shouldContainMetrics: Boolean): Unit = {
    val metrics = RemoteStorageMetrics.allMetrics().asScala.filter(name =>
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.exists(metric => {
        metric._1.getMBeanName.equals(name.getMBeanName)
      })
    ).toList
    val aggregatedBrokerTopicStats = Set(
      RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName,
      RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName,
      RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName,
      RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName,
      RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName)
    val aggregatedBrokerTopicMetrics = aggregatedBrokerTopicStats.filter(name =>
      KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.exists(metric => {
        metric._1.getMBeanName.equals(fromNameToBrokerTopicStatsMBean(name))
      })
    ).toList
    if (shouldContainMetrics) {
      assertEquals(RemoteStorageMetrics.allMetrics().size(), metrics.size, s"Only $metrics appear in the metrics")
      assertEquals(aggregatedBrokerTopicStats.size, aggregatedBrokerTopicMetrics.size, s"Only $aggregatedBrokerTopicMetrics appear in the metrics")
    } else {
      assertEquals(0, metrics.size, s"$metrics should not appear in the metrics")
      assertEquals(0, aggregatedBrokerTopicMetrics.size, s"$aggregatedBrokerTopicMetrics should not appear in the metrics")
    }
  }
}
