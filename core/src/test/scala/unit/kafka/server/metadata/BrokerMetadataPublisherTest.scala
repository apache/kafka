/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.metadata

import java.util.Collections.{singleton, singletonList, singletonMap}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import kafka.log.UnifiedLog
import kafka.server.{BrokerServer, KafkaConfig}
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.BROKER
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataImageTest, TopicImage, TopicsImage}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.fault.FaultHandler
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.doThrow
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.jdk.CollectionConverters._

class BrokerMetadataPublisherTest {
  val exitException = new AtomicReference[Throwable](null)

  @BeforeEach
  def setUp(): Unit = {
    Exit.setExitProcedure((code, _) => exitException.set(new RuntimeException(s"Exit ${code}")))
    Exit.setHaltProcedure((code, _) => exitException.set(new RuntimeException(s"Halt ${code}")))
  }

  @AfterEach
  def tearDown(): Unit = {
    Exit.resetExitProcedure();
    Exit.resetHaltProcedure();
    val exception = exitException.get()
    if (exception != null) {
      throw exception
    }
  }

  @Test
  def testGetTopicDelta(): Unit = {
    assert(BrokerMetadataPublisher.getTopicDelta(
      "not-a-topic",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isEmpty, "Expected no delta for unknown topic")

    assert(BrokerMetadataPublisher.getTopicDelta(
      "foo",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isEmpty, "Expected no delta for deleted topic")

    assert(BrokerMetadataPublisher.getTopicDelta(
      "bar",
      MetadataImageTest.IMAGE1,
      MetadataImageTest.DELTA1).isDefined, "Expected to see delta for changed topic")
  }

  @Test
  def testFindStrayReplicas(): Unit = {
    val brokerId = 0

    // Topic has been deleted
    val deletedTopic = "a"
    val deletedTopicId = Uuid.randomUuid()
    val deletedTopicPartition1 = new TopicPartition(deletedTopic, 0)
    val deletedTopicLog1 = mockLog(deletedTopicId, deletedTopicPartition1)
    val deletedTopicPartition2 = new TopicPartition(deletedTopic, 1)
    val deletedTopicLog2 = mockLog(deletedTopicId, deletedTopicPartition2)

    // Topic was deleted and recreated
    val recreatedTopic = "b"
    val recreatedTopicPartition = new TopicPartition(recreatedTopic, 0)
    val recreatedTopicLog = mockLog(Uuid.randomUuid(), recreatedTopicPartition)
    val recreatedTopicImage = topicImage(Uuid.randomUuid(), recreatedTopic, Map(
      recreatedTopicPartition.partition -> Seq(0, 1, 2)
    ))

    // Topic exists, but some partitions were reassigned
    val reassignedTopic = "c"
    val reassignedTopicId = Uuid.randomUuid()
    val reassignedTopicPartition = new TopicPartition(reassignedTopic, 0)
    val reassignedTopicLog = mockLog(reassignedTopicId, reassignedTopicPartition)
    val retainedTopicPartition = new TopicPartition(reassignedTopic, 1)
    val retainedTopicLog = mockLog(reassignedTopicId, retainedTopicPartition)

    val reassignedTopicImage = topicImage(reassignedTopicId, reassignedTopic, Map(
      reassignedTopicPartition.partition -> Seq(1, 2, 3),
      retainedTopicPartition.partition -> Seq(0, 2, 3)
    ))

    val logs = Seq(
      deletedTopicLog1,
      deletedTopicLog2,
      recreatedTopicLog,
      reassignedTopicLog,
      retainedTopicLog
    )

    val image = topicsImage(Seq(
      recreatedTopicImage,
      reassignedTopicImage
    ))

    val expectedStrayPartitions = Set(
      deletedTopicPartition1,
      deletedTopicPartition2,
      recreatedTopicPartition,
      reassignedTopicPartition
    )

    val strayPartitions = BrokerMetadataPublisher.findStrayPartitions(brokerId, image, logs).toSet
    assertEquals(expectedStrayPartitions, strayPartitions)
  }

  private def mockLog(
    topicId: Uuid,
    topicPartition: TopicPartition
  ): UnifiedLog = {
    val log = Mockito.mock(classOf[UnifiedLog])
    Mockito.when(log.topicId).thenReturn(Some(topicId))
    Mockito.when(log.topicPartition).thenReturn(topicPartition)
    log
  }

  private def topicImage(
    topicId: Uuid,
    topic: String,
    partitions: Map[Int, Seq[Int]]
  ): TopicImage = {
    val partitionRegistrations = partitions.map { case (partitionId, replicas) =>
      Int.box(partitionId) -> new PartitionRegistration(
        replicas.toArray,
        replicas.toArray,
        Array.empty[Int],
        Array.empty[Int],
        replicas.head,
        LeaderRecoveryState.RECOVERED,
        0,
        0
      )
    }
    new TopicImage(topic, topicId, partitionRegistrations.asJava)
  }

  private def topicsImage(
    topics: Seq[TopicImage]
  ): TopicsImage = {
    val idsMap = topics.map(t => t.id -> t).toMap
    val namesMap = topics.map(t => t.name -> t).toMap
    new TopicsImage(idsMap.asJava, namesMap.asJava)
  }

  private def newMockDynamicConfigPublisher(
    broker: BrokerServer,
    errorHandler: FaultHandler
  ): DynamicConfigPublisher = {
    Mockito.spy(new DynamicConfigPublisher(
      conf = broker.config,
      faultHandler = errorHandler,
      dynamicConfigHandlers = broker.dynamicConfigHandlers.toMap,
      nodeType = "broker"))
  }

  @Test
  def testReloadUpdatedFilesWithoutConfigChange(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      val publisher = newMockDynamicConfigPublisher(broker, cluster.nonFatalFaultHandler())

      val numTimesReloadCalled = new AtomicInteger(0)
      Mockito.when(publisher.reloadUpdatedFilesWithoutConfigChange(any[Properties]())).
        thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = numTimesReloadCalled.addAndGet(1)
        })
      broker.metadataPublisher.dynamicConfigPublisher = publisher
      val admin = Admin.create(cluster.clientProperties())
      try {
        assertEquals(0, numTimesReloadCalled.get())
        admin.incrementalAlterConfigs(singletonMap(
          new ConfigResource(BROKER, ""),
          singleton(new AlterConfigOp(new ConfigEntry(KafkaConfig.MaxConnectionsProp, "123"), SET)))).all().get()
        TestUtils.waitUntilTrue(() => numTimesReloadCalled.get() == 0,
          "numTimesConfigured never reached desired value")

        // Setting the foo.bar.test.configuration to 1 will still trigger reconfiguration because
        // reloadUpdatedFilesWithoutConfigChange will be called.
        admin.incrementalAlterConfigs(singletonMap(
          new ConfigResource(BROKER, broker.config.nodeId.toString),
          singleton(new AlterConfigOp(new ConfigEntry(KafkaConfig.MaxConnectionsProp, "123"), SET)))).all().get()
        TestUtils.waitUntilTrue(() => numTimesReloadCalled.get() == 1,
          "numTimesConfigured never reached desired value")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testExceptionInUpdateCoordinator(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      TestUtils.retry(60000) {
        assertNotNull(broker.metadataPublisher)
      }
      val publisher = Mockito.spy(broker.metadataPublisher)
      doThrow(new RuntimeException("injected failure")).when(publisher).updateCoordinator(any(), any(), any(), any(), any())
      broker.metadataListener.alterPublisher(publisher).get()
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.createTopics(singletonList(new NewTopic("foo", 1, 1.toShort))).all().get()
      } finally {
        admin.close()
      }
      TestUtils.retry(60000) {
        assertTrue(Option(cluster.nonFatalFaultHandler().firstException()).
          flatMap(e => Option(e.getMessage())).getOrElse("(none)").contains("injected failure"))
      }
    } finally {
      cluster.nonFatalFaultHandler().setIgnore(true)
      cluster.close()
    }
  }
}
