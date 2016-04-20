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

import java.util.Properties

import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.server.{ClientQuotaManager, ClientConfigOverride, KafkaConfig, KafkaServer, QuotaId}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{Quota, KafkaMetric}
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.Assert._
import org.junit.{After, Before, Test}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.collection.Map
import scala.collection.mutable
import kafka.server.ClientQuotaManagerConfig

abstract class BaseQuotasTest extends IntegrationTestHarness {

  def quotaType: String
  def clientPrincipal : String
  def producerQuotaId : String
  def consumerQuotaId : String
  def changeQuota(quotaId: String, props: Properties)

  override val serverCount = 2
  val producerCount = 0
  val consumerCount = 0

  private val producerBufferSize = 300000
  protected val producerClientId = "QuotasTestProducer-1"
  protected val consumerClientId = "QuotasTestConsumer-1"

  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100")
  this.serverConfig.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, "30000")

  // Low enough quota that a producer sending a small payload in a tight loop should get throttled
  this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, "8000")
  this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, "2500")
  this.serverConfig.setProperty(KafkaConfig.QuotaTypeProp, quotaType)

  var replicaConsumers = mutable.Buffer[SimpleConsumer]()

  var leaderNode: KafkaServer = null
  var followerNode: KafkaServer = null
  private val topic1 = "topic-1"

  @Before
  override def setUp() {
    super.setUp()
    val producerSecurityProps = TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
    val consumerSecurityProps = TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
    val producerProps = new Properties()
    producerProps.putAll(producerSecurityProps)
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "0")
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferSize.toString)
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producers += new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    val numPartitions = 1
    val leaders = TestUtils.createTopic(zkUtils, topic1, numPartitions, serverCount, servers)
    leaderNode = if (leaders(0).get == servers.head.config.brokerId) servers.head else servers(1)
    followerNode = if (leaders(0).get != servers.head.config.brokerId) servers.head else servers(1)
    assertTrue("Leader of all partitions of the topic should exist", leaders.values.forall(leader => leader.isDefined))

    // Create consumers
    val consumerProps = new Properties
    consumerProps.putAll(consumerSecurityProps)
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "QuotasTest")
    consumerProps.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])

    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
    consumers += new KafkaConsumer(consumerProps)
    // Create replica consumers with the same clientId as the high level consumer. These requests should never be throttled
    replicaConsumers += new SimpleConsumer("localhost", leaderNode.boundPort(), 1000000, 64*1024, consumerClientId)
  }

  @After
  override def tearDown() {
    replicaConsumers.foreach( _.close )
    super.tearDown()
  }

  @Test
  def testThrottledProducerConsumer() {
    val allMetrics: mutable.Map[MetricName, KafkaMetric] = leaderNode.metrics.metrics().asScala

    val numRecords = 1000
    produce(producers.head, numRecords)

    val producerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.PRODUCE.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, producerMetricTagValue)
    assertTrue("Should have been throttled", allMetrics(producerMetricName).value() > 0)

    // Consumer should read in a bursty manner and get throttled immediately
    consume(consumers.head, numRecords)
    // The replica consumer should not be throttled also. Create a fetch request which will exceed the quota immediately
    val request = new FetchRequestBuilder().addFetch(topic1, 0, 0, 1024*1024).replicaId(followerNode.config.brokerId).build()
    replicaConsumers.head.fetch(request)
    val consumerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.FETCH.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, consumerMetricTagValue)
    assertTrue("Should have been throttled", allMetrics(consumerMetricName).value() > 0)
  }

  @Test
  def testProducerConsumerOverrideUnthrottled() {
    // Give effectively unlimited quota for producer and consumer
    val props = new Properties()
    props.put(ClientConfigOverride.ProducerOverride, Long.MaxValue.toString)
    props.put(ClientConfigOverride.ConsumerOverride, Long.MaxValue.toString)

    changeQuota(producerQuotaId, props)
    changeQuota(consumerQuotaId, props)

    TestUtils.retry(10000) {
      val quotaManagers: Map[Short, ClientQuotaManager] = leaderNode.apis.quotaManagers
      val overrideProducerQuota = quotaManagers.get(ApiKeys.PRODUCE.id).get.quota(producerQuotaId)
      val overrideConsumerQuota = quotaManagers.get(ApiKeys.FETCH.id).get.quota(consumerQuotaId)

      assertEquals(s"$quotaType $producerQuotaId must have unlimited producer quota", Quota.upperBound(Long.MaxValue), overrideProducerQuota)
      assertEquals(s"$quotaType $consumerQuotaId must have unlimited consumer quota", Quota.upperBound(Long.MaxValue), overrideConsumerQuota)
    }

    val allMetrics: mutable.Map[MetricName, KafkaMetric] = leaderNode.metrics.metrics().asScala
    val numRecords = 1000
    produce(producers.head, numRecords)
    val producerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.PRODUCE.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, producerMetricTagValue)
    assertEquals("Should not have been throttled", 0.0, allMetrics(producerMetricName).value(), 0.0)

    // The "client" consumer does not get throttled.
    consume(consumers.head, numRecords)
    // The replica consumer should not be throttled also. Create a fetch request which will exceed the quota immediately
    val request = new FetchRequestBuilder().addFetch(topic1, 0, 0, 1024*1024).replicaId(followerNode.config.brokerId).build()
    replicaConsumers.head.fetch(request)
    val consumerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.FETCH.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, consumerMetricTagValue)
    assertEquals("Should not have been throttled", 0.0, allMetrics(consumerMetricName).value(), 0.0)
  }

  @Test
  def testQuotaOverrideDelete() {
    // Override producer and consumer quotas to unlimited
    val props = new Properties()
    props.put(ClientConfigOverride.ProducerOverride, Long.MaxValue.toString)
    props.put(ClientConfigOverride.ConsumerOverride, Long.MaxValue.toString)

    changeQuota(producerQuotaId, props)
    changeQuota(consumerQuotaId, props)

    val allMetrics: mutable.Map[MetricName, KafkaMetric] = leaderNode.metrics.metrics().asScala
    val producerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.PRODUCE.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, producerMetricTagValue)
    val consumerMetricName = leaderNode.metrics.metricName("throttle-time",
                                                           ApiKeys.FETCH.name,
                                                           "Tracking throttle-time per " + quotaType,
                                                           quotaMetricTag, consumerMetricTagValue)
    val numRecords = 1000
    produce(producers.head, numRecords)
    assertTrue("Should not have been throttled", allMetrics(producerMetricName).value() == 0)
    consume(consumers.head, numRecords)
    assertTrue("Should not have been throttled", allMetrics(consumerMetricName).value() == 0)

    // Delete producer and consumer quota overrides. Consumer and producer should now be
    // throttled since broker defaults are very small
    val emptyProps = new Properties()
    changeQuota(producerQuotaId, emptyProps)
    changeQuota(consumerQuotaId, emptyProps)
    produce(producers.head, numRecords)

    assertTrue("Should have been throttled", allMetrics(producerMetricName).value() > 0)
    consume(consumers.head, numRecords)
    assertTrue("Should have been throttled", allMetrics(consumerMetricName).value() > 0)
  }

  def produce(p: KafkaProducer[Array[Byte], Array[Byte]], count: Int): Int = {
    var numBytesProduced = 0
    for (i <- 0 to count) {
      val payload = i.toString.getBytes
      numBytesProduced += payload.length
      p.send(new ProducerRecord[Array[Byte], Array[Byte]](topic1, null, null, payload),
             new ErrorLoggingCallback(topic1, null, null, true)).get()
      Thread.sleep(1)
    }
    numBytesProduced
  }

  def consume(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int) {
    consumer.subscribe(List(topic1))
    var numConsumed = 0
    while (numConsumed < numRecords) {
      for (cr <- consumer.poll(100)) {
        numConsumed += 1
      }
    }
  }

  private def quotaMetricTag = quotaType
  private def producerMetricTagValue = QuotaId.sanitize(quotaType, producerQuotaId)
  private def consumerMetricTagValue = QuotaId.sanitize(quotaType, consumerQuotaId)
}
