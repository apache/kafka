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

import kafka.metrics.clientmetrics.ClientMetricsConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientTelemetryListener, ClientTelemetrySubscription, CommonClientConfigs}
import org.apache.kafka.clients.admin.{AdminClientConfig, AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.ClientTelemetryMetricsReporter
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.telemetry.metrics.Metric
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.io.File
import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer

class ClientTelemetryTest extends IntegrationTestHarness {

  val topicName = "telemetry-integration-test"

  override def brokerCount: Int = 1

  val telemetryOutputDir: File = TestUtils.tempDir()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.serverConfig.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, classOf[ClientTelemetryMetricsReporter].getName)
    this.serverConfig.setProperty(ClientTelemetryMetricsReporter.OUTPUT_DIRECTORY_CONFIG, telemetryOutputDir.toString)
    super.setUp(testInfo)
  }

  @Test
  def testBasic(): Unit = {
    enableClientMetricsConfigs()

    val subscriptions = ListBuffer[ClientTelemetrySubscription]()
    val emitted = new ListBuffer[util.List[Metric]]()
    val producer = createProducer()

    try {
      val clientTelemetry = producer.asInstanceOf[KafkaProducer[ByteArraySerializer, ByteArraySerializer]].clientTelemetry().get()
      assertNotNull(clientTelemetry)

      val latch = new CountDownLatch(1)

      clientTelemetry.addListener(new ClientTelemetryListener {
        override def getSubscriptionRequestCreated(clientInstanceId: Uuid): Unit = {}

        override def getSubscriptionCompleted(subscription: ClientTelemetrySubscription): Unit = {
          subscriptions.append(subscription)
        }

        override def pushRequestCreated(clientInstanceId: Uuid, subscriptionId: Int, terminating: Boolean, compressionType: CompressionType, metrics: util.List[Metric]): Unit = {
          emitted.append(metrics)
        }

        override def pushRequestCompleted(clientInstanceId: Uuid): Unit = {
          latch.countDown()
        }
      })

      val clientInstanceId = producer.clientInstanceId(Duration.ofSeconds(1))
      assertNotNull(clientInstanceId)

      val record = new ProducerRecord(topicName, "key".getBytes, "value".getBytes)
      producer.send(record).get()

      latch.await()
    } finally {
      // Give the terminal push some time to write to the broker.
      producer.close(Duration.ofSeconds(1))
    }

    assertTrue(subscriptions.nonEmpty)

    for (sub <- subscriptions) {
      println("value of sub: " + sub)
    }

    assertEquals(1, subscriptions.size)
    assertEquals(2, emitted.size)

    for (metricsList <- emitted) {
      println("value of metricsList: " + metricsList)
    }

    for (telemetryFile <- telemetryOutputDir.list()) {
      println("telemetryFile: " + telemetryFile)
    }
  }

//  @Test
//  def testDisableMetricsPush(): Unit = {
//    val producer = createProducer(configOverrides = disablePushProperties())
//    assertEquals(Optional.empty, producer.asInstanceOf[KafkaProducer[ByteArraySerializer, ByteArraySerializer]].clientTelemetry())
//    val record = new ProducerRecord(topicName, "key".getBytes, "value".getBytes)
//    producer.send(record)
//
//    val clientInstanceId = producer.clientInstanceId(Duration.ofSeconds(1))
//    assertFalse(clientInstanceId.isPresent)
//  }

  def disablePushProperties(): Properties = {
    val p = new Properties()
    p.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "false")
    p
  }

  def enableClientMetricsConfigs(pushInterval: Option[Int] = Some(1000)): Unit = {
    val adminClient = createAdminClient(configOverrides = disablePushProperties())

    try {
      val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]

      val cr = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "test-subscription")
      val list = new util.ArrayList[AlterConfigOp]()

      if (pushInterval.isDefined)
        list.add(new AlterConfigOp(new ConfigEntry(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.get.toString), AlterConfigOp.OpType.SET))

      list.add(new AlterConfigOp(new ConfigEntry(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true"), AlterConfigOp.OpType.SET))
      configs.put(cr, list)

      // Block until the update has fully completed.
      adminClient.incrementalAlterConfigs(configs).all().get()
    } finally {
      adminClient.close()
    }
  }

}