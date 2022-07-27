/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.server.{ConfigEntityName, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util
import java.util.Collections.singleton
import java.util.Properties
import java.util.concurrent.{Future, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class QuotaMetricsTest extends IntegrationTestHarness {
  val producerClientId = "producer-100"
  val consumerClientId = "consumer-100"
  val inactiveSensorExpirationTimeSeconds = 2
  override protected def brokerCount: Int = 1
  this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, "1000000")
  this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, "1000000")
  this.serverConfig.setProperty(KafkaConfig.InactiveSensorExpirationTimeSecondsProp, inactiveSensorExpirationTimeSeconds.toString)


  @BeforeEach
  override def setUp(): Unit = {
    // change the frequency of the Metrics Scheduler so that expired metrics can be removed faster
    Metrics.setMetricsSchedulerInitialDelaySeconds(0);
    Metrics.setMetricsSchedulerPeriodSeconds(1);
    super.setUp()

    // [KIP-124] apply the dynamic request_percentage quota override on the client id level for all client ids
    val adminClient = createAdminClient()
    val alterEntityMap = new util.HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.CLIENT_ID, ConfigEntityName.Default)
    val entity = new ClientQuotaEntity(alterEntityMap)
    val entries: util.List[ClientQuotaAlteration] = new util.ArrayList[ClientQuotaAlteration](1)
    entries.add(new ClientQuotaAlteration(entity, singleton(new ClientQuotaAlteration.Op(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 100.0))))
    adminClient.alterClientQuotas(entries).all().get(60, TimeUnit.SECONDS)
  }

  @Test
  def testThrottleTime(): Unit = {
    val topic = "test"
    val props = new Properties
    createTopic(topic, numPartitions = 1, replicationFactor = 1, props)
    val tp = new TopicPartition(topic, 0)

    // Produce some records
    val numRecords = 10
    val recordSize = 100000

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
    val producer = createProducer(configOverrides = producerProps)
    sendRecords(producer, numRecords, recordSize, tp)

    // Verify that the client id level throttle-time metrics show up even without producing quota violations
    verifyQuotaMetrics("throttle-time", "Produce", producerClientId, true, value => value == 0)
    verifyQuotaMetrics("byte-rate", "Produce", producerClientId, true, value => value > 0)
    verifyQuotaMetrics("throttle-time", "Request", producerClientId, true, value => value == 0)
    verifyQuotaMetrics("request-time", "Request", producerClientId, true, value => value > 0)

    // Consume some records
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
    val consumer = createConsumer(configOverrides = consumerProps)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    TestUtils.consumeRecords(consumer, numRecords)

    // Verify that the client id level throttle-time metrics show up even without fetching quota violations
    verifyQuotaMetrics("throttle-time", "Fetch", consumerClientId, true, value => value == 0)
    verifyQuotaMetrics("byte-rate", "Fetch", consumerClientId, true, value => value > 0)
    verifyQuotaMetrics("throttle-time", "Request", consumerClientId, true, value => value == 0)
    verifyQuotaMetrics("request-time", "Request", consumerClientId, true, value => value > 0)

    // Wait until the Produce and Fetch metrics are removed
    TestUtils.waitUntilTrue(() => {
      val produceMetrics = filterMetric("throttle-time", "Produce", producerClientId)
      val consumeMetrics = filterMetric("throttle-time", "Fetch", consumerClientId)
      produceMetrics.isEmpty && consumeMetrics.isEmpty
    }, "The Produce and Fetch throttle-time metrics should expire")
    // Verify that the Produce metrics are gone
    verifyQuotaMetrics("throttle-time", "Produce", producerClientId, false)
    verifyQuotaMetrics("byte-rate", "Produce", producerClientId, false)
    verifyQuotaMetrics("throttle-time", "Request", producerClientId, false)
    verifyQuotaMetrics("request-time", "Request", producerClientId, false)

    // Verify that the Fetch metrics are gone
    verifyQuotaMetrics("throttle-time", "Fetch", consumerClientId, false)
    verifyQuotaMetrics("byte-rate", "Fetch", consumerClientId, false)
    verifyQuotaMetrics("throttle-time", "Request", consumerClientId, false)
    verifyQuotaMetrics("request-time", "Request", consumerClientId, false)
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
    recordSize: Int, tp: TopicPartition) = {
    val bytes = new Array[Byte](recordSize)
    val sendFutures = mutable.Buffer[Future[RecordMetadata]]()
    (0 until numRecords).map { i =>
      sendFutures += producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, bytes))
    }
    sendFutures.foreach{_.get()}
  }

  private def filterMetric(metricNameFilter: String, metricGroupFilter: String,
    clientIdFilter: String) = {
    val allMetrics = servers(0).metrics.metrics().asScala

    allMetrics.filterKeys{name =>
      name.name().equals(metricNameFilter) && name.group().equals(metricGroupFilter) &&
        name.tags().containsKey("client-id") &&
        name.tags.get("client-id").equals(clientIdFilter)}
  }

  private def verifyQuotaMetrics(metricNameFilter: String, metricGroupFilter: String,
    clientIdFilter: String,
    shouldExist: Boolean,
    valuePredicate: Double => Boolean = v => true): Unit = {

    val metricsToCheck = filterMetric(metricNameFilter, metricGroupFilter, clientIdFilter)
    if (shouldExist) {
      assertEquals(1, metricsToCheck.size)
      assertTrue(metricsToCheck.forall{metric => valuePredicate(metric._2.metricValue.asInstanceOf[Double]) })
    } else {
      assertEquals(0, metricsToCheck.size)
    }
  }
}
