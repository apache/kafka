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

import kafka.server.{ClientQuotaManagerConfig, DynamicConfig, KafkaConfig, KafkaServer, QuotaId, QuotaType}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.metrics.{KafkaMetric, Quota, Sanitizer}
import org.junit.Assert._
import org.junit.{Before, Test}

abstract class BaseQuotaTest extends IntegrationTestHarness {

  def userPrincipal : String
  def producerQuotaId : QuotaId
  def consumerQuotaId : QuotaId
  def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double)
  def removeQuotaOverrides()

  override val serverCount = 2
  val producerCount = 1
  val consumerCount = 1

  private val producerBufferSize = 300000
  protected def producerClientId = "QuotasTestProducer-1"
  protected def consumerClientId = "QuotasTestConsumer-1"

  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100")
  this.serverConfig.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, "30000")
  this.serverConfig.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferSize.toString)
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

  var leaderNode: KafkaServer = null
  var followerNode: KafkaServer = null
  private val topic1 = "topic-1"

  @Before
  override def setUp() {
    super.setUp()

    val numPartitions = 1
    val leaders = TestUtils.createTopic(zkUtils, topic1, numPartitions, serverCount, servers)
    leaderNode = if (leaders(0) == servers.head.config.brokerId) servers.head else servers(1)
    followerNode = if (leaders(0) != servers.head.config.brokerId) servers.head else servers(1)
  }

  @Test
  def testThrottledProducerConsumer() {

    val numRecords = 1000
    val producer = producers.head
    val produced = produceUntilThrottled(producer, numRecords)
    assertTrue("Should have been throttled", producerThrottleMetric.value > 0)
    verifyProducerThrottleTimeMetric(producer)

    // Consumer should read in a bursty manner and get throttled immediately
    val consumer = consumers.head
    consumeUntilThrottled(consumer, produced)
    assertTrue("Should have been throttled", consumerThrottleMetric.value > 0)
    verifyConsumerThrottleTimeMetric(consumer)
  }

  @Test
  def testProducerConsumerOverrideUnthrottled() {
    // Give effectively unlimited quota for producer and consumer
    val props = new Properties()
    props.put(DynamicConfig.Client.ProducerByteRateOverrideProp, Long.MaxValue.toString)
    props.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, Long.MaxValue.toString)

    overrideQuotas(Long.MaxValue, Long.MaxValue, Int.MaxValue)
    waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Int.MaxValue)

    val numRecords = 1000
    assertEquals(numRecords, produceUntilThrottled(producers.head, numRecords))
    assertEquals("Should not have been throttled", 0.0, producerThrottleMetric.value, 0.0)

    // The "client" consumer does not get throttled.
    assertEquals(numRecords, consumeUntilThrottled(consumers.head, numRecords))
    assertEquals("Should not have been throttled", 0.0, consumerThrottleMetric.value, 0.0)
  }

  @Test
  def testQuotaOverrideDelete() {
    // Override producer and consumer quotas to unlimited
    overrideQuotas(Long.MaxValue, Long.MaxValue, Int.MaxValue)
    waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, Int.MaxValue)

    val numRecords = 1000
    assertEquals(numRecords, produceUntilThrottled(producers.head, numRecords))
    assertEquals("Should not have been throttled", 0.0, producerThrottleMetric.value, 0.0)
    assertEquals(numRecords, consumeUntilThrottled(consumers.head, numRecords))
    assertEquals("Should not have been throttled", 0.0, consumerThrottleMetric.value, 0.0)

    // Delete producer and consumer quota overrides. Consumer and producer should now be
    // throttled since broker defaults are very small
    removeQuotaOverrides()
    val produced = produceUntilThrottled(producers.head, numRecords)
    assertTrue("Should have been throttled", producerThrottleMetric.value > 0)

    // Since producer may have been throttled after producing a couple of records,
    // consume from beginning till throttled
    consumers.head.seekToBeginning(Collections.singleton(new TopicPartition(topic1, 0)))
    consumeUntilThrottled(consumers.head, numRecords + produced)
    assertTrue("Should have been throttled", consumerThrottleMetric.value > 0)
  }

  @Test
  def testThrottledRequest() {

    overrideQuotas(Long.MaxValue, Long.MaxValue, 0.1)
    waitForQuotaUpdate(Long.MaxValue, Long.MaxValue, 0.1)

    val consumer = consumers.head
    consumer.subscribe(Collections.singleton(topic1))
    val endTimeMs = System.currentTimeMillis + 10000
    var throttled = false
    while ((!throttled || exemptRequestMetric == null) && System.currentTimeMillis < endTimeMs) {
      consumer.poll(100)
      val throttleMetric = consumerRequestThrottleMetric
      throttled = throttleMetric != null && throttleMetric.value > 0
    }

    assertTrue("Should have been throttled", throttled)
    verifyConsumerThrottleTimeMetric(consumer, Some(ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds * 1000.0))

    assertNotNull("Exempt requests not recorded", exemptRequestMetric)
    assertTrue("Exempt requests not recorded", exemptRequestMetric.value > 0)
  }

  def produceUntilThrottled(p: KafkaProducer[Array[Byte], Array[Byte]], maxRecords: Int): Int = {
    var numProduced = 0
    var throttled = false
    do {
      val payload = numProduced.toString.getBytes
      p.send(new ProducerRecord[Array[Byte], Array[Byte]](topic1, null, null, payload),
             new ErrorLoggingCallback(topic1, null, null, true)).get()
      numProduced += 1
      val throttleMetric = producerThrottleMetric
      throttled = throttleMetric != null && throttleMetric.value > 0
    } while (numProduced < maxRecords && !throttled)
    numProduced
  }

  def consumeUntilThrottled(consumer: KafkaConsumer[Array[Byte], Array[Byte]], maxRecords: Int): Int = {
    consumer.subscribe(Collections.singleton(topic1))
    var numConsumed = 0
    var throttled = false
    do {
      numConsumed += consumer.poll(100).count
      val throttleMetric = consumerThrottleMetric
      throttled = throttleMetric != null && throttleMetric.value > 0
    }  while (numConsumed < maxRecords && !throttled)

    // If throttled, wait for the records from the last fetch to be received
    if (throttled && numConsumed < maxRecords) {
      val minRecords = numConsumed + 1
      while (numConsumed < minRecords)
          numConsumed += consumer.poll(100).count
    }
    numConsumed
  }

  def waitForQuotaUpdate(producerQuota: Long, consumerQuota: Long, requestQuota: Double) {
    TestUtils.retry(10000) {
      val quotaManagers = leaderNode.apis.quotas
      val overrideProducerQuota = quotaManagers.produce.quota(userPrincipal, producerClientId)
      val overrideConsumerQuota = quotaManagers.fetch.quota(userPrincipal, consumerClientId)
      val overrideProducerRequestQuota = quotaManagers.request.quota(userPrincipal, producerClientId)
      val overrideConsumerRequestQuota = quotaManagers.request.quota(userPrincipal, consumerClientId)

      assertEquals(s"ClientId $producerClientId of user $userPrincipal must have producer quota", Quota.upperBound(producerQuota), overrideProducerQuota)
      assertEquals(s"ClientId $consumerClientId of user $userPrincipal must have consumer quota", Quota.upperBound(consumerQuota), overrideConsumerQuota)
      assertEquals(s"ClientId $producerClientId of user $userPrincipal must have request quota", Quota.upperBound(requestQuota), overrideProducerRequestQuota)
      assertEquals(s"ClientId $consumerClientId of user $userPrincipal must have request quota", Quota.upperBound(requestQuota), overrideConsumerRequestQuota)
    }
  }

  private def verifyProducerThrottleTimeMetric(producer: KafkaProducer[_, _]) {
    val tags = new HashMap[String, String]
    tags.put("client-id", Sanitizer.sanitize(producerClientId))
    val avgMetric = producer.metrics.get(new MetricName("produce-throttle-time-avg", "producer-metrics", "", tags))
    val maxMetric = producer.metrics.get(new MetricName("produce-throttle-time-max", "producer-metrics", "", tags))

    TestUtils.waitUntilTrue(() => avgMetric.value > 0.0 && maxMetric.value > 0.0,
        s"Producer throttle metric not updated: avg=${avgMetric.value} max=${maxMetric.value}")
  }

  private def verifyConsumerThrottleTimeMetric(consumer: KafkaConsumer[_, _], maxThrottleTime: Option[Double] = None) {
    val tags = new HashMap[String, String]
    tags.put("client-id", Sanitizer.sanitize(consumerClientId))
    val avgMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", "", tags))
    val maxMetric = consumer.metrics.get(new MetricName("fetch-throttle-time-max", "consumer-fetch-manager-metrics", "", tags))

    TestUtils.waitUntilTrue(() => avgMetric.value > 0.0 && maxMetric.value > 0.0,
        s"Consumer throttle metric not updated: avg=${avgMetric.value} max=${maxMetric.value}")
    maxThrottleTime.foreach(max => assertTrue(s"Maximum consumer throttle too high: ${maxMetric.value}", maxMetric.value <= max))
  }

  private def throttleMetricName(quotaType: QuotaType, quotaId: QuotaId): MetricName = {
    leaderNode.metrics.metricName("throttle-time",
                                  quotaType.toString,
                                  "Tracking throttle-time per user/client-id",
                                  "user", quotaId.sanitizedUser.getOrElse(""),
                                  "client-id", quotaId.sanitizedClientId.getOrElse(""))
  }

  def throttleMetric(quotaType: QuotaType, quotaId: QuotaId): KafkaMetric = {
    leaderNode.metrics.metrics.get(throttleMetricName(quotaType, quotaId))
  }

  private def producerThrottleMetric = throttleMetric(QuotaType.Produce, producerQuotaId)
  private def consumerThrottleMetric = throttleMetric(QuotaType.Fetch, consumerQuotaId)
  private def consumerRequestThrottleMetric = throttleMetric(QuotaType.Request, consumerQuotaId)

  private def exemptRequestMetric: KafkaMetric = {
    val metricName = leaderNode.metrics.metricName("exempt-request-time", QuotaType.Request.toString, "")
    leaderNode.metrics.metrics.get(metricName)
  }

  def quotaProperties(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Properties = {
    val props = new Properties()
    props.put(DynamicConfig.Client.ProducerByteRateOverrideProp, producerQuota.toString)
    props.put(DynamicConfig.Client.ConsumerByteRateOverrideProp, consumerQuota.toString)
    props.put(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    props
  }
}
