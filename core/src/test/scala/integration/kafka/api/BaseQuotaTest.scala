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

import java.util.{Collections, HashMap, Properties}

import kafka.api.QuotaTestClients._
import kafka.server.{ClientQuotaManager, ClientQuotaManagerConfig, DynamicConfig, KafkaConfig, KafkaServer, QuotaType}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.metrics.{KafkaMetric, Quota}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

abstract class BaseQuotaTest extends IntegrationTestHarness {

  override val serverCount = 2
  val producerCount = 1
  val consumerCount = 1

  protected def producerClientId = "QuotasTestProducer-1"
  protected def consumerClientId = "QuotasTestConsumer-1"
  protected def createQuotaTestClients(topic: String, leaderNode: KafkaServer): QuotaTestClients

  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100")
  this.serverConfig.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, "30000")
  this.serverConfig.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "300000")
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "QuotasTest")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "0")
  this.consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")

  // Low enough quota that a producer sending a small payload in a tight loop should get throttled
  val defaultProducerQuota = 8000
  val defaultConsumerQuota = 2500
  val defaultRequestQuota = Int.MaxValue

  val topic1 = "topic-1"
  var leaderNode: KafkaServer = _
  var followerNode: KafkaServer = _
  var quotaTestClients: QuotaTestClients = _

  @Before
  override def setUp() {
    super.setUp()

    val numPartitions = 1
    val leaders = createTopic(topic1, numPartitions, serverCount)
    leaderNode = if (leaders(0) == servers.head.config.brokerId) servers.head else servers(1)
    followerNode = if (leaders(0) != servers.head.config.brokerId) servers.head else servers(1)
    quotaTestClients = createQuotaTestClients(topic1, leaderNode)
  }

  @Test
  def testThrottledProducerConsumer() {

    val numRecords = 1000
    val produced = quotaTestClients.produceUntilThrottled(numRecords)
    quotaTestClients.verifyProduceThrottle(expectThrottle = true)

    // Consumer should read in a bursty manner and get throttled immediately
    quotaTestClients.consumeUntilThrottled(produced)
    quotaTestClients.verifyConsumeThrottle(expectThrottle = true)
  }

  @Test
  def testProducerConsumerOverrideUnthrottled() {
    // Give effectively unlimited quota for producer and consumer
    val props = new Properties()
    props.put(DynamicConfig.Client.ProducerByteRateOverrideProp, Long.MaxValue.toString)
    props.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, Long.MaxValue.toString)

    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, Int.MaxValue)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Int.MaxValue)

    val numRecords = 1000
    assertEquals(numRecords, quotaTestClients.produceUntilThrottled(numRecords))
    quotaTestClients.verifyProduceThrottle(expectThrottle = false)

    // The "client" consumer does not get throttled.
    assertEquals(numRecords, quotaTestClients.consumeUntilThrottled(numRecords))
    quotaTestClients.verifyConsumeThrottle(expectThrottle = false)
  }

  @Test
  def testQuotaOverrideDelete() {
    // Override producer and consumer quotas to unlimited
    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, Int.MaxValue)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Int.MaxValue)

    val numRecords = 1000
    assertEquals(numRecords, quotaTestClients.produceUntilThrottled(numRecords))
    quotaTestClients.verifyProduceThrottle(expectThrottle = false)
    assertEquals(numRecords, quotaTestClients.consumeUntilThrottled(numRecords))
    quotaTestClients.verifyConsumeThrottle(expectThrottle = false)

    // Delete producer and consumer quota overrides. Consumer and producer should now be
    // throttled since broker defaults are very small
    quotaTestClients.removeQuotaOverrides()
    val produced = quotaTestClients.produceUntilThrottled(numRecords)
    quotaTestClients.verifyProduceThrottle(expectThrottle = true)

    // Since producer may have been throttled after producing a couple of records,
    // consume from beginning till throttled
    consumers.head.seekToBeginning(Collections.singleton(new TopicPartition(topic1, 0)))
    quotaTestClients.consumeUntilThrottled(numRecords + produced)
    quotaTestClients.verifyConsumeThrottle(expectThrottle = true)
  }

  @Test
  def testThrottledRequest() {

    quotaTestClients.overrideQuotas(Long.MaxValue, Long.MaxValue, 0.1)
    quotaTestClients.waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, 0.1)

    val consumer = consumers.head
    consumer.subscribe(Collections.singleton(topic1))
    val endTimeMs = System.currentTimeMillis + 10000
    var throttled = false
    while ((!throttled || quotaTestClients.exemptRequestMetric == null) && System.currentTimeMillis < endTimeMs) {
      consumer.poll(100)
      val throttleMetric = quotaTestClients.throttleMetric(QuotaType.Request, consumerClientId)
      throttled = throttleMetric != null && metricValue(throttleMetric) > 0
    }

    assertTrue("Should have been throttled", throttled)
    quotaTestClients.verifyConsumerClientThrottleTimeMetric(expectThrottle = true,
      Some(ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds * 1000.0))

    val exemptMetric = quotaTestClients.exemptRequestMetric
    assertNotNull("Exempt requests not recorded", exemptMetric)
    assertTrue("Exempt requests not recorded", metricValue(exemptMetric) > 0)
  }
}

object QuotaTestClients {
  def metricValue(metric: Metric): Double = metric.metricValue().asInstanceOf[Double]
}

abstract class QuotaTestClients(topic: String,
                                leaderNode: KafkaServer,
                                producerClientId: String,
                                consumerClientId: String,
                                producer: KafkaProducer[Array[Byte], Array[Byte]],
                                consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {

  def userPrincipal : KafkaPrincipal
  def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double)
  def removeQuotaOverrides()

  def quotaMetricTags(clientId: String): Map[String, String]

  def quota(quotaManager: ClientQuotaManager, userPrincipal: KafkaPrincipal, clientId: String): Quota = {
    quotaManager.quota(userPrincipal, clientId)
  }

  def produceUntilThrottled(maxRecords: Int, waitForRequestCompletion: Boolean = true): Int = {
    var numProduced = 0
    var throttled = false
    do {
      val payload = numProduced.toString.getBytes
      val future = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, null, null, payload),
        new ErrorLoggingCallback(topic, null, null, true))
      numProduced += 1
      do {
        val metric = throttleMetric(QuotaType.Produce, producerClientId)
        throttled = metric != null && metricValue(metric) > 0
      } while (!future.isDone && (!throttled || waitForRequestCompletion))
    } while (numProduced < maxRecords && !throttled)
    numProduced
  }

  def consumeUntilThrottled(maxRecords: Int, waitForRequestCompletion: Boolean = true): Int = {
    consumer.subscribe(Collections.singleton(topic))
    var numConsumed = 0
    var throttled = false
    do {
      numConsumed += consumer.poll(100).count
      val metric = throttleMetric(QuotaType.Fetch, consumerClientId)
      throttled = metric != null && metricValue(metric) > 0
    }  while (numConsumed < maxRecords && !throttled)

    // If throttled, wait for the records from the last fetch to be received
    if (throttled && numConsumed < maxRecords && waitForRequestCompletion) {
      val minRecords = numConsumed + 1
      while (numConsumed < minRecords)
        numConsumed += consumer.poll(100).count
    }
    numConsumed
  }

  def verifyProduceThrottle(expectThrottle: Boolean, verifyClientMetric: Boolean = true): Unit = {
    verifyThrottleTimeMetric(QuotaType.Produce, producerClientId, expectThrottle)
    if (verifyClientMetric)
      verifyProducerClientThrottleTimeMetric(expectThrottle)
  }

  def verifyConsumeThrottle(expectThrottle: Boolean, verifyClientMetric: Boolean = true): Unit = {
    verifyThrottleTimeMetric(QuotaType.Fetch, consumerClientId, expectThrottle)
    if (verifyClientMetric)
      verifyConsumerClientThrottleTimeMetric(expectThrottle)
  }

  def verifyThrottleTimeMetric(quotaType: QuotaType, clientId: String, expectThrottle: Boolean): Unit = {
    val throttleMetricValue = metricValue(throttleMetric(quotaType, clientId))
    if (expectThrottle) {
      assertTrue("Should have been throttled", throttleMetricValue > 0)
    } else {
      assertEquals("Should not have been throttled", 0.0, throttleMetricValue, 0.0)
    }
  }

  def throttleMetricName(quotaType: QuotaType, clientId: String): MetricName = {
    leaderNode.metrics.metricName("throttle-time",
      quotaType.toString,
      quotaMetricTags(clientId).asJava)
  }

  def throttleMetric(quotaType: QuotaType, clientId: String): KafkaMetric = {
    leaderNode.metrics.metrics.get(throttleMetricName(quotaType, clientId))
  }

  def exemptRequestMetric: KafkaMetric = {
    val metricName = leaderNode.metrics.metricName("exempt-request-time", QuotaType.Request.toString, "")
    leaderNode.metrics.metrics.get(metricName)
  }

  def verifyProducerClientThrottleTimeMetric(expectThrottle: Boolean) {
    val tags = new HashMap[String, String]
    tags.put("client-id", producerClientId)
    val avgMetric = producer.metrics.get(new MetricName("produce-throttle-time-avg", "producer-metrics", "", tags))
    val maxMetric = producer.metrics.get(new MetricName("produce-throttle-time-max", "producer-metrics", "", tags))

    if (expectThrottle) {
      TestUtils.waitUntilTrue(() => metricValue(avgMetric) > 0.0 && metricValue(maxMetric) > 0.0,
        s"Producer throttle metric not updated: avg=${metricValue(avgMetric)} max=${metricValue(maxMetric)}")
    } else
      assertEquals("Should not have been throttled", 0.0, metricValue(maxMetric), 0.0)
  }

  def verifyConsumerClientThrottleTimeMetric(expectThrottle: Boolean, maxThrottleTime: Option[Double] = None) {
    val tags = new HashMap[String, String]
    tags.put("client-id", consumerClientId)
    val avgMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", "", tags))
    val maxMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-max", "consumer-fetch-manager-metrics", "", tags))

    if (expectThrottle) {
      TestUtils.waitUntilTrue(() => metricValue(avgMetric) > 0.0 && metricValue(maxMetric) > 0.0,
        s"Consumer throttle metric not updated: avg=${metricValue(avgMetric)} max=${metricValue(maxMetric)}")
      maxThrottleTime.foreach(max => assertTrue(s"Maximum consumer throttle too high: ${metricValue(maxMetric)}",
        metricValue(maxMetric) <= max))
    } else
      assertEquals("Should not have been throttled", 0.0, metricValue(maxMetric), 0.0)
  }

  def quotaProperties(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Properties = {
    val props = new Properties()
    props.put(DynamicConfig.Client.ProducerByteRateOverrideProp, producerQuota.toString)
    props.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, consumerQuota.toString)
    props.put(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    props
  }

  def waitForQuotaUpdate(producerQuota: Long, consumerQuota: Long, requestQuota: Double, server: KafkaServer = leaderNode) {
    TestUtils.retry(10000) {
      val quotaManagers = server.apis.quotas
      val overrideProducerQuota = quota(quotaManagers.produce, userPrincipal, producerClientId)
      val overrideConsumerQuota = quota(quotaManagers.fetch, userPrincipal, consumerClientId)
      val overrideProducerRequestQuota = quota(quotaManagers.request, userPrincipal, producerClientId)
      val overrideConsumerRequestQuota = quota(quotaManagers.request, userPrincipal, consumerClientId)

      assertEquals(s"ClientId $producerClientId of user $userPrincipal must have producer quota", Quota.upperBound(producerQuota), overrideProducerQuota)
      assertEquals(s"ClientId $consumerClientId of user $userPrincipal must have consumer quota", Quota.upperBound(consumerQuota), overrideConsumerQuota)
      assertEquals(s"ClientId $producerClientId of user $userPrincipal must have request quota", Quota.upperBound(requestQuota), overrideProducerRequestQuota)
      assertEquals(s"ClientId $consumerClientId of user $userPrincipal must have request quota", Quota.upperBound(requestQuota), overrideConsumerRequestQuota)
    }
  }
}
