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

package integration.kafka.controller

import com.yammer.metrics.core.Gauge
import kafka.api.IntegrationTestHarness
import kafka.server.IntegrationTestUtils
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._


class OfflinePartitionsFromDeletedTopicTest extends IntegrationTestHarness {
  val log: Logger = LoggerFactory.getLogger(classOf[OfflinePartitionsFromDeletedTopicTest])

  override protected def brokerCount: Int = 3

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    doSetup(testInfo, createOffsetsTopic = false)
  }

  @Test
  def testPartitionsOfTopicsToBeDeletedNotMarkedOfflineOnControllerElection(): Unit = {
    var producer: KafkaProducer[String, String] = null
    var admin: Admin = null

    try {
      // Create a topic with one partition per broker
      admin = TestUtils.createAdminClient(brokers, listenerName, adminClientConfig)
      val topic = "test-topic"
      val replicaAssignment = Map(0 -> List(0), 1 -> List(1), 2 -> List(2))
      IntegrationTestUtils.createTopic(admin, topic, replicaAssignment)

      // Produce some data
      val numRecords = 10
      val numPartitions = replicaAssignment.size
      producer = createProducer(new StringSerializer(), new StringSerializer())
      (0 until numRecords)
        .map { i =>
          val partition = i % numPartitions
          producer.send(new ProducerRecord(topic, partition, i.toString, i.toString))
        }.map(_.get)

      // There should be no offline partitions at this point
      var controllerId = zkClient.getControllerId.get
      assertEquals(0, offlinePartitionsCount(controllerId), "Non zero offline partitions")

      // Shut down one of the brokers that is not a controller
      val brokerToKill = if (controllerId == 2) 1 else 2
      killBroker(brokerToKill)
      TestUtils.waitUntilTrue(() => offlinePartitionsCount(controllerId) == 1, "Expected one partition to be offline after broker shutdown")

      // Now elect a new controller
      controllerId = electNewController(zkClient)

      // Delete the topic now
      admin.deleteTopics(List(topic).asJava).all().get(30, TimeUnit.SECONDS)
      TestUtils.waitUntilTrue(() => offlinePartitionsCount(controllerId) == 0, "Non zero offline partitions after topic deletion")

      // Elect a controller again, this must be the original controller that the test started with
      controllerId = electNewController(zkClient)
      TestUtils.waitUntilTrue(() => offlinePartitionsCount(controllerId) == 0, "Non zero offline partitions after topic deletion and old controller re-elected")

      // Bring the killed broker back up again. The topic should get deleted eventually.
      startBroker(brokerToKill)
      TestUtils.waitUntilTrue(() => !zkClient.isTopicMarkedForDeletion(topic), "Topic is still pending deletion after broker comes back up")
      assertEquals(0, offlinePartitionsCount(controllerId))
    } finally {
      if (producer != null) {
        producer.close(Duration.ofSeconds(30))
      }
      if (admin != null) {
        admin.close(Duration.ofSeconds(30))
      }
    }
  }

  private def electNewController(zkClient: KafkaZkClient): Int = {
    val oldController = zkClient.getControllerId
    var newController = oldController
    var tries = 10
    while (tries > 0 && oldController == newController) {
      zkClient.deletePath("/controller")
      newController = Some(TestUtils.waitUntilControllerElected(zkClient))
      tries -= 1
    }
    assertNotEquals(oldController, newController, "Failed to elect a different controller")
    log.info(s"Controller changed from $oldController to $newController")
    newController.get
  }

  private def offlinePartitionsCount(controllerId: Int): Int = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName.endsWith("OfflinePartitionsCount") && k.getScope == s"brokerId.$controllerId" }.get._2
      .asInstanceOf[Gauge[Int]].value()
  }
}
