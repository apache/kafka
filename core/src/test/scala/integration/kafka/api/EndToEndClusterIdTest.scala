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

import java.util.concurrent.atomic.AtomicReference
import java.util.{ArrayList, Properties}

import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, TopicPartition}
import org.apache.kafka.test.{TestUtils => _, _}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

/** The test cases here verify the following:
  * 1. The ProducerInterceptor receives the Cluster metadata after the onSend() method is called.
  * 2. The Serializer receives the Cluster metadata before the serialize() method is called.
  * 3. The producer MetricReporter receives the Cluster metadata metadata after send() method is called on KafkaProducer.
  * 4. The ConsumerInterceptor receives the Cluster metadata before the onConsume() method.
  * 5. The Deserializer receives the Cluster metadata before the deserialize() method is called.
  * 6. The consumer MetricReporter receives the Cluster metadata metadata after poll() is called on KafkaConsumer.
  * 7. The broker MetricReporter receives the Cluster metadata metadata after the broker startup is over.
  * 8. The broker KafkaMetricReporter receives the Cluster metadata metadata after the broker startup is over.
  * 9. All the components receive the same Cluster metadata.
  */


object MockConsumerMetricsReporter {
  val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
}

class MockConsumerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

  override def onUpdate(clusterMetadata: ClusterResource) {
    MockConsumerMetricsReporter.CLUSTER_META.set(clusterMetadata)
  }
}

object MockProducerMetricsReporter {
  val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
}

class MockProducerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

  override def onUpdate(clusterMetadata: ClusterResource) {
    MockProducerMetricsReporter.CLUSTER_META.set(clusterMetadata)
  }
}

object MockBrokerMetricsReporter {
  val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
}

class MockBrokerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

  override def onUpdate(clusterMetadata: ClusterResource) {
    MockBrokerMetricsReporter.CLUSTER_META.set(clusterMetadata)
  }
}


class EndToEndClusterIdTest extends KafkaServerTestHarness {

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
  val topicAndPartition = new TopicAndPartition(topic, part)
  this.serverConfig.setProperty(KafkaConfig.MetricReporterClassesProp, "kafka.api.MockBrokerMetricsReporter")


  override def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = saslProperties)
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp() {
    super.setUp
    // create the consumer offset topic
    TestUtils.createTopic(this.zkUtils, topic, 2, serverCount, this.servers)

  }

  @Test
  def testEndToEnd() {
    val appendStr = "mock"

    assertNotNull(MockBrokerMetricsReporter.CLUSTER_META)
    assertEquals(48, MockBrokerMetricsReporter.CLUSTER_META.get().clusterId().length())



    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockProducerInterceptor")
    producerProps.put("mock.interceptor.append", appendStr)
    producerProps.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "kafka.api.MockProducerMetricsReporter")
    val testProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps, new MockSerializer, new MockSerializer)

    // Send one record and make sure clusterId is set after send
    testProducer.send(new ProducerRecord(tp.topic(), tp.partition(), s"42".getBytes, s"42".getBytes))

    assertNotNull(MockProducerInterceptor.CLUSTER_META)
    assertEquals(48, MockProducerInterceptor.CLUSTER_META.get().clusterId().length())

    assertNotNull(MockSerializer.CLUSTER_META)
    assertEquals(48, MockProducerInterceptor.CLUSTER_META.get().clusterId().length())
    assertTrue(MockSerializer.IS_CLUSTER_ID_PRESENT_BEFORE_SERIALIZE.get())

    assertNotNull(MockProducerMetricsReporter.CLUSTER_META)
    assertEquals(48, MockProducerMetricsReporter.CLUSTER_META.get().clusterId().length())

    this.consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    this.consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor")
    this.consumerConfig.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "kafka.api.MockConsumerMetricsReporter")
    val testConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](this.consumerConfig, new MockDeserializer(), new MockDeserializer())
    testConsumer.assign(List(tp).asJava)
    testConsumer.seek(tp, 0)

    // consume and verify that values are modified by interceptors
    consumeRecords(testConsumer, numRecords)

    // Check that cluster id is present after the first poll call.
    assertTrue(MockConsumerInterceptor.IS_CLUSTER_ID_PRESENT_BEFORE_ON_CONSUME.get())
    assertNotNull(MockConsumerInterceptor.CLUSTER_META)
    assertEquals(48, MockConsumerInterceptor.CLUSTER_META.get().clusterId().length())

    assertTrue(MockDeserializer.IS_CLUSTER_ID_PRESENT_BEFORE_DESERIALIZE.get())
    assertNotNull(MockDeserializer.CLUSTER_META)
    assertEquals(48, MockDeserializer.CLUSTER_META.get().clusterId().length())

    assertNotNull(MockConsumerMetricsReporter.CLUSTER_META)
    assertEquals(48, MockConsumerMetricsReporter.CLUSTER_META.get().clusterId().length())

    // Make sure everyone receives the same cluster id.
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockSerializer.CLUSTER_META.get().clusterId())
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockProducerMetricsReporter.CLUSTER_META.get().clusterId())
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockConsumerInterceptor.CLUSTER_META.get().clusterId())
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockDeserializer.CLUSTER_META.get().clusterId())
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockConsumerMetricsReporter.CLUSTER_META.get().clusterId())
    assertEquals(MockProducerInterceptor.CLUSTER_META.get().clusterId(), MockBrokerMetricsReporter.CLUSTER_META.get().clusterId())

    testConsumer.close()
    testProducer.close()
  }


  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                             numRecords: Int = 1,
                             startingOffset: Int = 0,
                             topic: String = topic,
                             part: Int = part) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 50
    var iters = 0
    while (records.size < numRecords) {
      for (record <- consumer.poll(50).asScala) {
        records.add(record)
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    }
  }
}


