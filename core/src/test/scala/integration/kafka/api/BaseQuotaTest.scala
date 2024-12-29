/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}
import com.yammer.metrics.core.{Histogram, Meter}
import kafka.api.QuotaTestClients._
import kafka.server.{ClientQuotaManager, KafkaBroker}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.metrics.{KafkaMetric, Quota}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.{QuotaConfig, ServerConfigs}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.quota.QuotaType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.Map
import scala.jdk.CollectionConverters._

abstract class BaseQuotaTest extends IntegrationTestHarness {

  override val brokerCount = 2

  protected def producerClientId = "QuotasTestProducer-1"
  protected def consumerClientId = "QuotasTestConsumer-1"
  protected def createQuotaTestClients(topic: String, leaderNode: KafkaBroker): QuotaTestClients

  this.serverConfig.setProperty(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
  this.serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "2")
  this.serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
  this.serverConfig.setProperty(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "100")
  this.serverConfig.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "60000")
  this.serverConfig.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "-1")
  this.producerConfig.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "300000")
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "QuotasTest")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "0")
  this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")

  // Low enough quota that a producer sending a small payload in a tight loop should get throttled
  val defaultProducerQuota: Long = 8000
  val defaultConsumerQuota: Long = 2500
  val defaultRequestQuota: Double = Long.MaxValue.toDouble

  val topic1 = "topic-1"
  var leaderNode: KafkaBroker = _
  var followerNode: KafkaBroker = _
  var quotaTestClients: QuotaTestClients = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    val numPartitions = 1
    val leaders = createTopic(topic1, numPartitions, brokerCount, adminClientConfig = adminClientConfig)
    leaderNode = if (leaders(0) == brokers.head.config.brokerId) brokers.head else brokers(1)
    followerNode = if (leaders(0) != brokers.head.config.brokerId) brokers.head else brokers(1)
    quotaTestClients = createQuotaTestClients(topic1, leaderNode)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testThrottledProducerConsumer(quorum: String, groupProtocol: String): Unit = {
    val numRecords = 1000
    val produced = quotaTestClients.produceUntilThrottled(numRecords)
    quotaTestClients.verifyProduceThrottle(expectThrottle = true)

    // Consumer should read in a bursty manner and get throttled immediately
    assertTrue(quotaTestClients.consumeUntilThrottled(produced) > 0, "Should have consumed at least one record")
    quotaTestClients.verifyConsumeThrottle(expectThrottle = true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProducerConsumerOverrideUnthrottled(quorum: String, groupProtocol: String): Unit = {
    // Give effectively unlimited quota for producer and consumer
    val props = new Properties()
    props.put(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, Long.MaxValue.toString)
    props.put(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, Long.MaxValue.toString)

    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, Long.MaxValue.toDouble)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Long.MaxValue.toDouble)

    val numRecords = 1000
    assertEquals(numRecords, quotaTestClients.produceUntilThrottled(numRecords))
    quotaTestClients.verifyProduceThrottle(expectThrottle = false)

    // The "client" consumer does not get throttled.
    assertEquals(numRecords, quotaTestClients.consumeUntilThrottled(numRecords))
    quotaTestClients.verifyConsumeThrottle(expectThrottle = false)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProducerConsumerOverrideLowerQuota(quorum: String, groupProtocol: String): Unit = {
    // consumer quota is set such that consumer quota * default quota window (10 seconds) is less than
    // MAX_PARTITION_FETCH_BYTES_CONFIG, so that we can test consumer ability to fetch in this case
    // In this case, 250 * 10 < 4096
    quotaTestClients.overrideQuotas(2000, 250, Long.MaxValue.toDouble)
    quotaTestClients.waitForQuotaUpdate(2000, 250, Long.MaxValue.toDouble)

    val numRecords = 1000
    val produced = quotaTestClients.produceUntilThrottled(numRecords)
    quotaTestClients.verifyProduceThrottle(expectThrottle = true)

    // Consumer should be able to consume at least one record, even when throttled
    assertTrue(quotaTestClients.consumeUntilThrottled(produced) > 0, "Should have consumed at least one record")
    quotaTestClients.verifyConsumeThrottle(expectThrottle = true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testQuotaOverrideDelete(quorum: String, groupProtocol: String): Unit = {
    // Override producer and consumer quotas to unlimited
    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, Long.MaxValue.toDouble)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Long.MaxValue.toDouble)

    val numRecords = 1000
    assertEquals(numRecords, quotaTestClients.produceUntilThrottled(numRecords))
    quotaTestClients.verifyProduceThrottle(expectThrottle = false)
    assertEquals(numRecords, quotaTestClients.consumeUntilThrottled(numRecords))
    quotaTestClients.verifyConsumeThrottle(expectThrottle = false)

    // Delete producer and consumer quota overrides. Consumer and producer should now be
    // throttled since broker defaults are very small
    quotaTestClients.removeQuotaOverrides()
    quotaTestClients.waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
    val produced = quotaTestClients.produceUntilThrottled(numRecords)
    quotaTestClients.verifyProduceThrottle(expectThrottle = true)

    // Since producer may have been throttled after producing a couple of records,
    // consume from beginning till throttled
    quotaTestClients.consumer.seekToBeginning(Collections.singleton(new TopicPartition(topic1, 0)))
    quotaTestClients.consumeUntilThrottled(numRecords + produced)
    quotaTestClients.verifyConsumeThrottle(expectThrottle = true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testThrottledRequest(quorum: String, groupProtocol: String): Unit = {
    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, 0.1)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, 0.1)

    val consumer = quotaTestClients.consumer
    consumer.subscribe(Collections.singleton(topic1))
    val endTimeMs = System.currentTimeMillis + 10000
    var throttled = false
    while ((!throttled || quotaTestClients.exemptRequestMetric == null || metricValue(quotaTestClients.exemptRequestMetric) <= 0)
      && System.currentTimeMillis < endTimeMs) {
      consumer.poll(Duration.ofMillis(100L))
      val throttleMetric = quotaTestClients.throttleMetric(QuotaType.REQUEST, consumerClientId)
      throttled = throttleMetric != null && metricValue(throttleMetric) > 0
    }

    assertTrue(throttled, "Should have been throttled")
    quotaTestClients.verifyConsumerClientThrottleTimeMetric(expectThrottle = true,
      Some(QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT * 1000.0))

    val exemptMetric = quotaTestClients.exemptRequestMetric
    assertNotNull(exemptMetric, "Exempt requests not recorded")
    assertTrue(metricValue(exemptMetric) > 0, "Exempt requests not recorded")
  }
}

object QuotaTestClients {
  val DefaultEntity: String = null

  def metricValue(metric: Metric): Double = metric.metricValue().asInstanceOf[Double]
}

abstract class QuotaTestClients(topic: String,
                                leaderNode: KafkaBroker,
                                producerClientId: String,
                                consumerClientId: String,
                                val producer: KafkaProducer[Array[Byte], Array[Byte]],
                                val consumer: Consumer[Array[Byte], Array[Byte]],
                                val adminClient: Admin) {

  def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit
  def removeQuotaOverrides(): Unit

  protected def userPrincipal: KafkaPrincipal
  protected def quotaMetricTags(clientId: String): Map[String, String]

  def produceUntilThrottled(maxRecords: Int, waitForRequestCompletion: Boolean = true): Int = {
    var numProduced = 0
    var throttled = false
    do {
      val payload = numProduced.toString.getBytes
      val future = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, null, null, payload),
        new ErrorLoggingCallback(topic, null, null, true))
      numProduced += 1
      do {
        val metric = throttleMetric(QuotaType.PRODUCE, producerClientId)
        throttled = metric != null && metricValue(metric) > 0
      } while (!future.isDone && (!throttled || waitForRequestCompletion))
    } while (numProduced < maxRecords && !throttled)
    numProduced
  }

  def consumeUntilThrottled(maxRecords: Int, waitForRequestCompletion: Boolean = true): Int = {
    val timeoutMs = TimeUnit.MINUTES.toMillis(1)

    consumer.subscribe(Collections.singleton(topic))
    var numConsumed = 0
    var throttled = false
    val startMs = System.currentTimeMillis
    do {
      numConsumed += consumer.poll(Duration.ofMillis(100L)).count
      val metric = throttleMetric(QuotaType.FETCH, consumerClientId)
      throttled = metric != null && metricValue(metric) > 0
    } while (numConsumed < maxRecords && !throttled && System.currentTimeMillis < startMs + timeoutMs)

    // If throttled, wait for the records from the last fetch to be received
    if (throttled && numConsumed < maxRecords && waitForRequestCompletion) {
      val minRecords = numConsumed + 1
      val startMs = System.currentTimeMillis
      while (numConsumed < minRecords && System.currentTimeMillis < startMs + timeoutMs)
        numConsumed += consumer.poll(Duration.ofMillis(100L)).count
    }
    numConsumed
  }

  private def quota(quotaManager: ClientQuotaManager, userPrincipal: KafkaPrincipal, clientId: String): Quota = {
    quotaManager.quota(userPrincipal, clientId)
  }

  private def verifyThrottleTimeRequestChannelMetric(apiKey: ApiKeys, metricNameSuffix: String,
                                                     clientId: String, expectThrottle: Boolean): Unit = {
    val throttleTimeMs = brokerRequestMetricsThrottleTimeMs(apiKey, metricNameSuffix)
    if (expectThrottle)
      assertTrue(throttleTimeMs > 0, s"Client with id=$clientId should have been throttled, $throttleTimeMs")
    else
      assertEquals(0.0, throttleTimeMs, 0.0, s"Client with id=$clientId should not have been throttled")
  }

  def verifyProduceThrottle(expectThrottle: Boolean, verifyClientMetric: Boolean = true,
                            verifyRequestChannelMetric: Boolean = true): Unit = {
    verifyThrottleTimeMetric(QuotaType.PRODUCE, producerClientId, expectThrottle)
    if (verifyRequestChannelMetric)
      verifyThrottleTimeRequestChannelMetric(ApiKeys.PRODUCE, "", producerClientId, expectThrottle)
    if (verifyClientMetric)
      verifyProducerClientThrottleTimeMetric(expectThrottle)
  }

  def verifyConsumeThrottle(expectThrottle: Boolean, verifyClientMetric: Boolean = true,
                            verifyRequestChannelMetric: Boolean = true): Unit = {
    verifyThrottleTimeMetric(QuotaType.FETCH, consumerClientId, expectThrottle)
    if (verifyRequestChannelMetric)
      verifyThrottleTimeRequestChannelMetric(ApiKeys.FETCH, "Consumer", consumerClientId, expectThrottle)
    if (verifyClientMetric)
      verifyConsumerClientThrottleTimeMetric(expectThrottle)
  }

  private def verifyThrottleTimeMetric(quotaType: QuotaType, clientId: String, expectThrottle: Boolean): Unit = {
    val throttleMetricValue = metricValue(throttleMetric(quotaType, clientId))
    if (expectThrottle) {
      assertTrue(throttleMetricValue > 0, s"Client with id=$clientId should have been throttled")
    } else {
      assertTrue(throttleMetricValue.isNaN, s"Client with id=$clientId should not have been throttled")
    }
  }

  private def throttleMetricName(quotaType: QuotaType, clientId: String): MetricName = {
    leaderNode.metrics.metricName("throttle-time",
      quotaType.toString,
      quotaMetricTags(clientId).asJava)
  }

  def throttleMetric(quotaType: QuotaType, clientId: String): KafkaMetric = {
    leaderNode.metrics.metrics.get(throttleMetricName(quotaType, clientId))
  }

  private def brokerRequestMetricsThrottleTimeMs(apiKey: ApiKeys, metricNameSuffix: String): Double = {
    def yammerMetricValue(name: String): Double = {
      val allMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      val (_, metric) = allMetrics.find { case (metricName, _) =>
        metricName.getMBeanName.startsWith(name)
      }.getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
      metric match {
        case m: Meter => m.count.toDouble
        case m: Histogram => m.max
        case m => throw new AssertionError(s"Unexpected broker metric of class ${m.getClass}")
      }
    }

    yammerMetricValue(s"kafka.network:type=RequestMetrics,name=ThrottleTimeMs,request=${apiKey.name}$metricNameSuffix")
  }

  def exemptRequestMetric: KafkaMetric = {
    val metricName = leaderNode.metrics.metricName("exempt-request-time", QuotaType.REQUEST.toString, "")
    leaderNode.metrics.metrics.get(metricName)
  }

  private def verifyProducerClientThrottleTimeMetric(expectThrottle: Boolean): Unit = {
    val tags = new util.HashMap[String, String]
    tags.put("client-id", producerClientId)
    val avgMetric = producer.metrics.get(new MetricName("produce-throttle-time-avg", "producer-metrics", "", tags))
    val maxMetric = producer.metrics.get(new MetricName("produce-throttle-time-max", "producer-metrics", "", tags))

    if (expectThrottle) {
      TestUtils.waitUntilTrue(() => metricValue(avgMetric) > 0.0 && metricValue(maxMetric) > 0.0,
        s"Producer throttle metric not updated: avg=${metricValue(avgMetric)} max=${metricValue(maxMetric)}")
    } else
      assertEquals(0.0, metricValue(maxMetric), 0.0, "Should not have been throttled")
  }

  def verifyConsumerClientThrottleTimeMetric(expectThrottle: Boolean, maxThrottleTime: Option[Double] = None): Unit = {
    val tags = new util.HashMap[String, String]
    tags.put("client-id", consumerClientId)
    val avgMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", "", tags))
    val maxMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-max", "consumer-fetch-manager-metrics", "", tags))

    if (expectThrottle) {
      TestUtils.waitUntilTrue(() => metricValue(avgMetric) > 0.0 && metricValue(maxMetric) > 0.0,
        s"Consumer throttle metric not updated: avg=${metricValue(avgMetric)} max=${metricValue(maxMetric)}")
      maxThrottleTime.foreach(max => assertTrue(metricValue(maxMetric) <= max,
        s"Maximum consumer throttle too high: ${metricValue(maxMetric)}"))
    } else
      assertEquals(0.0, metricValue(maxMetric), 0.0, "Should not have been throttled")
  }

  def clientQuotaEntity(user: Option[String], clientId: Option[String]): ClientQuotaEntity = {
    var entries = Map.empty[String, String]
    user.foreach(user => entries = entries ++ Map(ClientQuotaEntity.USER -> user))
    clientId.foreach(clientId => entries = entries ++ Map(ClientQuotaEntity.CLIENT_ID -> clientId))
    new ClientQuotaEntity(entries.asJava)
  }

  // None is translated to `null` which remove the quota
  def clientQuotaAlteration(quotaEntity: ClientQuotaEntity,
                            producerQuota: Option[Long],
                            consumerQuota: Option[Long],
                            requestQuota: Option[Double]): ClientQuotaAlteration = {
    var ops = Seq.empty[ClientQuotaAlteration.Op]
    def addOp(key: String, value: Option[Double]): Unit = {
      ops = ops ++ Seq(new ClientQuotaAlteration.Op(key, value.map(Double.box).orNull))
    }
    addOp(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerQuota.map(_.toDouble))
    addOp(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, consumerQuota.map(_.toDouble))
    addOp(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, requestQuota)
    new ClientQuotaAlteration(quotaEntity, ops.asJava)
  }

  def alterClientQuotas(quotaAlterations: ClientQuotaAlteration *): Unit = {
    adminClient.alterClientQuotas(quotaAlterations.asJava).all().get()
  }

  def waitForQuotaUpdate(producerQuota: Long, consumerQuota: Long, requestQuota: Double, server: KafkaBroker = leaderNode): Unit = {
    TestUtils.retry(10000) {
      val quotaManagers = server.dataPlaneRequestProcessor.quotas
      val overrideProducerQuota = quota(quotaManagers.produce, userPrincipal, producerClientId)
      val overrideConsumerQuota = quota(quotaManagers.fetch, userPrincipal, consumerClientId)
      val overrideProducerRequestQuota = quota(quotaManagers.request, userPrincipal, producerClientId)
      val overrideConsumerRequestQuota = quota(quotaManagers.request, userPrincipal, consumerClientId)

      assertEquals(Quota.upperBound(producerQuota.toDouble), overrideProducerQuota,
        s"ClientId $producerClientId of user $userPrincipal must have producer quota")
      assertEquals(Quota.upperBound(consumerQuota.toDouble), overrideConsumerQuota,
        s"ClientId $consumerClientId of user $userPrincipal must have consumer quota")
      assertEquals(Quota.upperBound(requestQuota), overrideProducerRequestQuota,
        s"ClientId $producerClientId of user $userPrincipal must have request quota")
      assertEquals(Quota.upperBound(requestQuota), overrideConsumerRequestQuota,
        s"ClientId $consumerClientId of user $userPrincipal must have request quota")
    }
  }
}
