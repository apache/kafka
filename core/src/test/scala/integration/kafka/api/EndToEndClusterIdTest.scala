/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.api

import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicReference
import java.util.Properties
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, TopicPartition}
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.test.{TestUtils => _, _}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import scala.jdk.CollectionConverters._
import org.apache.kafka.test.TestUtils.isValidClusterId
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/** The test cases here verify the following conditions.
  * 1. The ProducerInterceptor receives the cluster id after the onSend() method is called and before onAcknowledgement() method is called.
  * 2. The Serializer receives the cluster id before the serialize() method is called.
  * 3. The producer MetricReporter receives the cluster id after send() method is called on KafkaProducer.
  * 4. The ConsumerInterceptor receives the cluster id before the onConsume() method.
  * 5. The Deserializer receives the cluster id before the deserialize() method is called.
  * 6. The consumer MetricReporter receives the cluster id after poll() is called on KafkaConsumer.
  * 7. The broker MetricReporter receives the cluster id after the broker startup is over.
  * 8. The broker KafkaMetricReporter receives the cluster id after the broker startup is over.
  * 9. All the components receive the same cluster id.
  */

object EndToEndClusterIdTest {

  object MockConsumerMetricsReporter {
    val CLUSTER_META = new AtomicReference[ClusterResource]
  }

  class MockConsumerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockConsumerMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }
  }

  object MockProducerMetricsReporter {
    val CLUSTER_META = new AtomicReference[ClusterResource]
  }

  class MockProducerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockProducerMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }
  }

  object MockBrokerMetricsReporter {
    val CLUSTER_META = new AtomicReference[ClusterResource]
  }

  class MockBrokerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockBrokerMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }
  }
}

class EndToEndClusterIdTest extends KafkaServerTestHarness {

  import EndToEndClusterIdTest._

  val producerCount = 1
  val consumerCount = 1
  val serverCount = 1
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties
  val numRecords = 1
  val topic = "e2etopic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  this.serverConfig.setProperty(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, classOf[MockBrokerMetricsReporter].getName)

  override def generateConfigs = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnectOrNull, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    MockDeserializer.resetStaticVariables()
    // create the consumer offset topic
    createTopic(topic, 2, serverCount)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testEndToEnd(quorum: String): Unit = {
    val appendStr = "mock"
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()

    assertNotNull(MockBrokerMetricsReporter.CLUSTER_META)
    isValidClusterId(MockBrokerMetricsReporter.CLUSTER_META.get.clusterId)

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, classOf[MockProducerInterceptor].getName)
    producerProps.put("mock.interceptor.append", appendStr)
    producerProps.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[MockProducerMetricsReporter].getName)
    val testProducer = new KafkaProducer(producerProps, new MockSerializer, new MockSerializer)

    // Send one record and make sure clusterId is set after send and before onAcknowledgement
    sendRecords(testProducer, 1, tp)
    assertNotEquals(MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT, MockProducerInterceptor.NO_CLUSTER_ID)
    assertNotNull(MockProducerInterceptor.CLUSTER_META)
    assertEquals(MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.get.clusterId, MockProducerInterceptor.CLUSTER_META.get.clusterId)
    isValidClusterId(MockProducerInterceptor.CLUSTER_META.get.clusterId)

    // Make sure that serializer gets the cluster id before serialize method.
    assertNotEquals(MockSerializer.CLUSTER_ID_BEFORE_SERIALIZE, MockSerializer.NO_CLUSTER_ID)
    assertNotNull(MockSerializer.CLUSTER_META)
    isValidClusterId(MockSerializer.CLUSTER_META.get.clusterId)

    assertNotNull(MockProducerMetricsReporter.CLUSTER_META)
    isValidClusterId(MockProducerMetricsReporter.CLUSTER_META.get.clusterId)

    this.consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, classOf[MockConsumerInterceptor].getName)
    this.consumerConfig.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[MockConsumerMetricsReporter].getName)
    val testConsumer = new KafkaConsumer(this.consumerConfig, new MockDeserializer, new MockDeserializer)
    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are modified by interceptors
    consumeRecords(testConsumer, numRecords)

    // Check that cluster id is present after the first poll call.
    assertNotEquals(MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME, MockConsumerInterceptor.NO_CLUSTER_ID)
    assertNotNull(MockConsumerInterceptor.CLUSTER_META)
    isValidClusterId(MockConsumerInterceptor.CLUSTER_META.get.clusterId)
    assertEquals(MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME.get.clusterId, MockConsumerInterceptor.CLUSTER_META.get.clusterId)

    assertNotEquals(MockDeserializer.clusterIdBeforeDeserialize, MockDeserializer.noClusterId)
    assertNotNull(MockDeserializer.clusterMeta)
    isValidClusterId(MockDeserializer.clusterMeta.get.clusterId)
    assertEquals(MockDeserializer.clusterIdBeforeDeserialize.get.clusterId, MockDeserializer.clusterMeta.get.clusterId)

    assertNotNull(MockConsumerMetricsReporter.CLUSTER_META)
    isValidClusterId(MockConsumerMetricsReporter.CLUSTER_META.get.clusterId)

    // Make sure everyone receives the same cluster id.
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockSerializer.CLUSTER_META.get.clusterId)
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockProducerMetricsReporter.CLUSTER_META.get.clusterId)
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockConsumerInterceptor.CLUSTER_META.get.clusterId)
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockDeserializer.clusterMeta.get.clusterId)
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockConsumerMetricsReporter.CLUSTER_META.get.clusterId)
    assertEquals(MockProducerInterceptor.CLUSTER_META.get.clusterId, MockBrokerMetricsReporter.CLUSTER_META.get.clusterId)

    testConsumer.close()
    testProducer.close()
    MockConsumerInterceptor.resetCounters()
    MockProducerInterceptor.resetCounters()
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int, tp: TopicPartition): Unit = {
    val futures = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part): Unit = {
    val records = TestUtils.consumeRecords(consumer, numRecords)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic)
      assertEquals(part, record.partition)
      assertEquals(offset.toLong, record.offset)
    }
  }
}
