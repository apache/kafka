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
package kafka.zk

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.{Collections, Properties}
import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.security.authorizer.AclEntry
import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.utils.CoreUtils
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper._
import org.apache.kafka.common.acl.AclOperation.READ
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.feature.Features._
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.kafka.common.utils.{SecurityUtils, Time}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.zookeeper.KeeperException.{Code, NoAuthException, NoNodeException, NodeExistsException}
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.common.ZKConfig
import org.apache.zookeeper.data.Stat
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.util.Random

class KafkaZkClientTest extends QuorumTestHarness {

  private val group = "my-group"
  private val topic1 = "topic1"
  private val topic2 = "topic2"
  private val topicIds = Map(topic1 -> Uuid.randomUuid(), topic2 -> Uuid.randomUuid())

  val topicPartition10 = new TopicPartition(topic1, 0)
  val topicPartition11 = new TopicPartition(topic1, 1)
  val topicPartition20 = new TopicPartition(topic2, 0)
  val topicPartitions10_11 = Seq(topicPartition10, topicPartition11)
  val controllerEpochZkVersion = 0

  var otherZkClient: KafkaZkClient = _
  var expiredSessionZkClient: ExpiredKafkaZkClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    zkClient.createControllerEpochRaw(1)
    otherZkClient = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClient",
      zkClientConfig = new ZKClientConfig)
    expiredSessionZkClient = ExpiredKafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled),
      zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (otherZkClient != null)
      otherZkClient.close()
    zkClient.deletePath(ControllerEpochZNode.path)
    if (expiredSessionZkClient != null)
      expiredSessionZkClient.close()
    super.tearDown()
  }

  private val topicPartition = new TopicPartition("topic", 0)

  @Test
  def testConnectionViaNettyClient(): Unit = {
    // Confirm that we can explicitly set client connection configuration, which is necessary for TLS.
    // TLS connectivity itself is tested in system tests rather than here to avoid having to add TLS support
    // to kafka.zk.EmbeddedZookeeper
    val clientConfig = new ZKClientConfig()
    val propKey = KafkaConfig.ZkClientCnxnSocketProp
    val propVal = "org.apache.zookeeper.ClientCnxnSocketNetty"
    KafkaConfig.setZooKeeperClientProperty(clientConfig, propKey, propVal)
    val client = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClient", zkClientConfig = clientConfig)
    try {
      assertEquals(Some(propVal), KafkaConfig.zooKeeperClientProperty(client.currentZooKeeper.getClientConfig, propKey))
      // For a sanity check, make sure a bad client connection socket class name generates an exception
      val badClientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(badClientConfig, propKey, propVal + "BadClassName")
      assertThrows(classOf[Exception],
        () => KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
          zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClientTest", zkClientConfig = badClientConfig))
    } finally {
      client.close()
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testChroot(createChrootIfNecessary: Boolean): Unit = {
    val chroot = "/chroot"
    val client = KafkaZkClient(zkConnect + chroot, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClientTest",
      zkClientConfig = new ZKClientConfig, createChrootIfNecessary = createChrootIfNecessary)
    try {
      client.createTopLevelPaths()
      if (!createChrootIfNecessary) {
        fail("We should not have been able to create top-level paths with a chroot when not explicitly creating the chroot path, but we were able to do so")
      }
    } catch {
      case e: Exception =>
        if (createChrootIfNecessary) {
          fail("We should have been able to create top-level paths with a chroot when explicitly creating the chroot path, but we failed to do so",
            e)
        }
    } finally {
      client.close()
    }
  }

  @Test
  def testChrootExistsAndRootIsLocked(): Unit = {
    // chroot is accessible
    val root = "/testChrootExistsAndRootIsLocked"
    val chroot = s"$root/chroot"

    zkClient.makeSurePersistentPathExists(chroot)
    zkClient.setAcl(chroot, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)

    // root is read-only
    zkClient.setAcl(root, ZooDefs.Ids.READ_ACL_UNSAFE.asScala)

    // we should not be able to create node under chroot folder
    assertThrows(classOf[NoAuthException], () => zkClient.makeSurePersistentPathExists(chroot))

    // this client doesn't have create permission to the root and chroot, but the chroot already exists
    // Expect that no exception thrown
    val chrootClient = KafkaZkClient(zkConnect + chroot, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClientTest",
      zkClientConfig = new ZKClientConfig, createChrootIfNecessary = true)
    chrootClient.close()
  }

  @Test
  def testSetAndGetConsumerOffset(): Unit = {
    val offset = 123L
    // None if no committed offsets
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
    // Set and retrieve an offset
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset)
    assertEquals(offset, zkClient.getConsumerOffset(group, topicPartition).get)
    // Update an existing offset and retrieve it
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset + 2L)
    assertEquals(offset + 2L, zkClient.getConsumerOffset(group, topicPartition).get)
  }

  @Test
  def testGetConsumerOffsetNoData(): Unit = {
    zkClient.createRecursive(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition))
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
  }

  @Test
  def testDeleteRecursive(): Unit = {
    zkClient.deleteRecursive("/delete/does-not-exist")

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    assertTrue(zkClient.deleteRecursive("/delete"))
    assertFalse(zkClient.pathExists("/delete"))

    assertThrows(classOf[IllegalArgumentException], () => zkClient.deleteRecursive("delete-invalid-path"))
  }

  @Test
  def testDeleteRecursiveWithControllerEpochVersionCheck(): Unit = {
    assertFalse(zkClient.deleteRecursive("/delete/does-not-exist", controllerEpochZkVersion))

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    assertThrows(classOf[ControllerMovedException], () => zkClient.deleteRecursive("/delete", controllerEpochZkVersion + 1))

    assertTrue(zkClient.deleteRecursive("/delete", controllerEpochZkVersion))
    assertFalse(zkClient.pathExists("/delete"))

    assertThrows(classOf[IllegalArgumentException], () => zkClient.deleteRecursive(
      "delete-invalid-path", controllerEpochZkVersion))
  }

  @Test
  def testCreateRecursive(): Unit = {
    zkClient.createRecursive("/create-newrootpath")
    assertTrue(zkClient.pathExists("/create-newrootpath"))

    zkClient.createRecursive("/create/some/random/long/path")
    assertTrue(zkClient.pathExists("/create/some/random/long/path"))
    zkClient.createRecursive("/create/some/random/long/path", throwIfPathExists = false) // no errors if path already exists

    assertThrows(classOf[IllegalArgumentException], () => zkClient.createRecursive("create-invalid-path"))
  }

  @Test
  def testTopicAssignmentMethods(): Unit = {
    assertTrue(zkClient.getAllTopicsInCluster().isEmpty)

    // test with non-existing topic
    assertFalse(zkClient.topicExists(topic1))
    assertTrue(zkClient.getTopicPartitionCount(topic1).isEmpty)
    assertTrue(zkClient.getPartitionAssignmentForTopics(Set(topic1)).isEmpty)
    assertTrue(zkClient.getPartitionsForTopics(Set(topic1)).isEmpty)
    assertTrue(zkClient.getReplicasForPartition(new TopicPartition(topic1, 2)).isEmpty)

    val assignment = Map(
      new TopicPartition(topic1, 0) -> Seq(0, 1),
      new TopicPartition(topic1, 1) -> Seq(0, 1),
      new TopicPartition(topic1, 2) -> Seq(1, 2, 3)
    )

    // create a topic assignment
    zkClient.createTopicAssignment(topic1, topicIds.get(topic1), assignment)

    assertTrue(zkClient.topicExists(topic1))

    val expectedAssignment = assignment map { topicAssignment =>
      val partition = topicAssignment._1.partition
      val assignment = topicAssignment._2
      partition -> ReplicaAssignment(assignment, List(), List())
    }

    assertEquals(assignment.size, zkClient.getTopicPartitionCount(topic1).get)
    assertEquals(expectedAssignment, zkClient.getPartitionAssignmentForTopics(Set(topic1))(topic1))
    assertEquals(Set(0, 1, 2), zkClient.getPartitionsForTopics(Set(topic1))(topic1).toSet)
    assertEquals(Set(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition(topic1, 2)).toSet)

    val updatedAssignment = assignment - new TopicPartition(topic1, 2)

    zkClient.setTopicAssignment(topic1, topicIds.get(topic1), updatedAssignment.map {
      case (k, v) => k -> ReplicaAssignment(v, List(), List()) })
    assertEquals(updatedAssignment.size, zkClient.getTopicPartitionCount(topic1).get)

    // add second topic
    val secondAssignment = Map(
      new TopicPartition(topic2, 0) -> Seq(0, 1),
      new TopicPartition(topic2, 1) -> Seq(0, 1)
    )

    zkClient.createTopicAssignment(topic2, topicIds.get(topic2), secondAssignment)

    assertEquals(Set(topic1, topic2), zkClient.getAllTopicsInCluster())
  }

  @Test
  def testGetAllTopicsInClusterTriggersWatch(): Unit = {
    zkClient.createTopLevelPaths()
    val latch = registerChildChangeHandler(1)

    // Listing all the topics and register the watch
    assertTrue(zkClient.getAllTopicsInCluster(true).isEmpty)

    // Verifies that listing all topics without registering the watch does
    // not interfere with the previous registered watcher
    assertTrue(zkClient.getAllTopicsInCluster(false).isEmpty)

    zkClient.createTopicAssignment(topic1, topicIds.get(topic1), Map.empty)

    assertTrue(latch.await(5, TimeUnit.SECONDS),
      "Failed to receive watch notification")

    assertTrue(zkClient.topicExists(topic1))
  }

  @Test
  def testGetAllTopicsInClusterDoesNotTriggerWatch(): Unit = {
    zkClient.createTopLevelPaths()
    val latch = registerChildChangeHandler(1)

    // Listing all the topics and don't register the watch
    assertTrue(zkClient.getAllTopicsInCluster(false).isEmpty)

    zkClient.createTopicAssignment(topic1, topicIds.get(topic1), Map.empty)

    assertFalse(latch.await(100, TimeUnit.MILLISECONDS),
      "Received watch notification")

    assertTrue(zkClient.topicExists(topic1))
  }

  private def registerChildChangeHandler(count: Int): CountDownLatch = {
    val znodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val znodeChildChangeHandler = new ZNodeChildChangeHandler {
      override val path: String = TopicsZNode.path

      override def handleChildChange(): Unit = {
        znodeChildChangeHandlerCountDownLatch.countDown()
      }
    }
    zkClient.registerZNodeChildChangeHandler(znodeChildChangeHandler)
    znodeChildChangeHandlerCountDownLatch
  }

  @Test
  def testGetDataAndVersion(): Unit = {
    val path = "/testpath"

    // test with non-existing path
    val (data0, version0) = zkClient.getDataAndVersion(path)
    assertTrue(data0.isEmpty)
    assertEquals(ZkVersion.UnknownVersion, version0)

    // create a test path
    zkClient.createRecursive(path)
    zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)

    // test with existing path
    val (data1, version1) = zkClient.getDataAndVersion(path)
    assertEquals("version1", new String(data1.get, UTF_8))
    assertEquals(1, version1)

    zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 1)
    val (data2, version2) = zkClient.getDataAndVersion(path)
    assertEquals("version2", new String(data2.get, UTF_8))
    assertEquals(2, version2)
  }

  @Test
  def testConditionalUpdatePath(): Unit = {
    val path = "/testconditionalpath"

    // test with non-existing path
    var statusAndVersion = zkClient.conditionalUpdatePath(path, "version0".getBytes(UTF_8), 0)
    assertFalse(statusAndVersion._1)
    assertEquals(ZkVersion.UnknownVersion, statusAndVersion._2)

    // create path
    zkClient.createRecursive(path)

    // test with valid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)
    assertTrue(statusAndVersion._1)
    assertEquals(1, statusAndVersion._2)

    // test with invalid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 2)
    assertFalse(statusAndVersion._1)
    assertEquals(ZkVersion.UnknownVersion, statusAndVersion._2)
  }

  @Test
  def testCreateSequentialPersistentPath(): Unit = {
    val path = "/testpath"
    zkClient.createRecursive(path)

    var result = zkClient.createSequentialPersistentPath(path + "/sequence_", null)
    assertEquals(s"$path/sequence_0000000000", result)
    assertTrue(zkClient.pathExists(s"$path/sequence_0000000000"))
    assertEquals(None, dataAsString(s"$path/sequence_0000000000"))

    result = zkClient.createSequentialPersistentPath(path + "/sequence_", "some value".getBytes(UTF_8))
    assertEquals(s"$path/sequence_0000000001", result)
    assertTrue(zkClient.pathExists(s"$path/sequence_0000000001"))
    assertEquals(Some("some value"), dataAsString(s"$path/sequence_0000000001"))
  }

  @Test
  def testPropagateIsrChanges(): Unit = {
    zkClient.createRecursive("/isr_change_notification")

    zkClient.propagateIsrChanges(Set(new TopicPartition("topic-a", 0), new TopicPartition("topic-b", 0)))
    var expectedPath = "/isr_change_notification/isr_change_0000000000"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"partitions":[{"topic":"topic-a","partition":0},{"topic":"topic-b","partition":0}]}"""),
      dataAsString(expectedPath))

    zkClient.propagateIsrChanges(Set(new TopicPartition("topic-b", 0)))
    expectedPath = "/isr_change_notification/isr_change_0000000001"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"partitions":[{"topic":"topic-b","partition":0}]}"""), dataAsString(expectedPath))
  }

  @Test
  def testIsrChangeNotificationGetters(): Unit = {
    assertEquals(Seq.empty, zkClient.getAllIsrChangeNotifications, "Failed for non existing parent ZK node")
    assertEquals(Seq.empty, zkClient.getPartitionsFromIsrChangeNotifications(Seq("0000000000")), "Failed for non existing parent ZK node")

    zkClient.createRecursive("/isr_change_notification")

    zkClient.propagateIsrChanges(Set(topicPartition10, topicPartition11))
    zkClient.propagateIsrChanges(Set(topicPartition10))

    assertEquals(Set("0000000000", "0000000001"), zkClient.getAllIsrChangeNotifications.toSet)

    // A partition can have multiple notifications
    assertEquals(Seq(topicPartition10, topicPartition11, topicPartition10),
      zkClient.getPartitionsFromIsrChangeNotifications(Seq("0000000000", "0000000001")))
  }

  @Test
  def testIsrChangeNotificationsDeletion(): Unit = {
    // Should not fail even if parent node does not exist
    zkClient.deleteIsrChangeNotifications(Seq("0000000000"), controllerEpochZkVersion)

    zkClient.createRecursive("/isr_change_notification")

    zkClient.propagateIsrChanges(Set(topicPartition10, topicPartition11))
    zkClient.propagateIsrChanges(Set(topicPartition10))
    zkClient.propagateIsrChanges(Set(topicPartition11))

    // Should throw exception if the controllerEpochZkVersion does not match
    assertThrows(classOf[ControllerMovedException], () => zkClient.deleteIsrChangeNotifications(Seq("0000000001"), controllerEpochZkVersion + 1))
    // Delete should not succeed
    assertEquals(Set("0000000000", "0000000001", "0000000002"), zkClient.getAllIsrChangeNotifications.toSet)

    zkClient.deleteIsrChangeNotifications(Seq("0000000001"), controllerEpochZkVersion)
    // Should not fail if called on a non-existent notification
    zkClient.deleteIsrChangeNotifications(Seq("0000000001"), controllerEpochZkVersion)

    assertEquals(Set("0000000000", "0000000002"), zkClient.getAllIsrChangeNotifications.toSet)
    zkClient.deleteIsrChangeNotifications(controllerEpochZkVersion)
    assertEquals(Seq.empty, zkClient.getAllIsrChangeNotifications)
  }

  @Test
  def testPropagateLogDir(): Unit = {
    zkClient.createRecursive("/log_dir_event_notification")

    val brokerId = 3

    zkClient.propagateLogDirEvent(brokerId)
    var expectedPath = "/log_dir_event_notification/log_dir_event_0000000000"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":3,"event":1}"""), dataAsString(expectedPath))

    zkClient.propagateLogDirEvent(brokerId)
    expectedPath = "/log_dir_event_notification/log_dir_event_0000000001"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":3,"event":1}"""), dataAsString(expectedPath))

    val anotherBrokerId = 4
    zkClient.propagateLogDirEvent(anotherBrokerId)
    expectedPath = "/log_dir_event_notification/log_dir_event_0000000002"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":4,"event":1}"""), dataAsString(expectedPath))
  }

  @Test
  def testLogDirGetters(): Unit = {
    assertEquals(Seq.empty,
      zkClient.getAllLogDirEventNotifications, "getAllLogDirEventNotifications failed for non existing parent ZK node")
    assertEquals(Seq.empty,
      zkClient.getBrokerIdsFromLogDirEvents(Seq("0000000000")), "getBrokerIdsFromLogDirEvents failed for non existing parent ZK node")

    zkClient.createRecursive("/log_dir_event_notification")

    val brokerId = 3
    zkClient.propagateLogDirEvent(brokerId)

    assertEquals(Seq(3), zkClient.getBrokerIdsFromLogDirEvents(Seq("0000000000")))

    zkClient.propagateLogDirEvent(brokerId)

    val anotherBrokerId = 4
    zkClient.propagateLogDirEvent(anotherBrokerId)

    val notifications012 = Seq("0000000000", "0000000001", "0000000002")
    assertEquals(notifications012.toSet, zkClient.getAllLogDirEventNotifications.toSet)
    assertEquals(Seq(3, 3, 4), zkClient.getBrokerIdsFromLogDirEvents(notifications012))
  }

  @Test
  def testLogDirEventNotificationsDeletion(): Unit = {
    // Should not fail even if parent node does not exist
    zkClient.deleteLogDirEventNotifications(Seq("0000000000", "0000000002"), controllerEpochZkVersion)

    zkClient.createRecursive("/log_dir_event_notification")

    val brokerId = 3
    val anotherBrokerId = 4

    zkClient.propagateLogDirEvent(brokerId)
    zkClient.propagateLogDirEvent(brokerId)
    zkClient.propagateLogDirEvent(anotherBrokerId)

    assertThrows(classOf[ControllerMovedException], () => zkClient.deleteLogDirEventNotifications(Seq("0000000000", "0000000002"), controllerEpochZkVersion + 1))
    assertEquals(Seq("0000000000", "0000000001", "0000000002"), zkClient.getAllLogDirEventNotifications)

    zkClient.deleteLogDirEventNotifications(Seq("0000000000", "0000000002"), controllerEpochZkVersion)

    assertEquals(Seq("0000000001"), zkClient.getAllLogDirEventNotifications)

    zkClient.propagateLogDirEvent(anotherBrokerId)

    zkClient.deleteLogDirEventNotifications(controllerEpochZkVersion)
    assertEquals(Seq.empty, zkClient.getAllLogDirEventNotifications)
  }

  @Test
  def testSetGetAndDeletePartitionReassignment(): Unit = {
    zkClient.createRecursive(AdminZNode.path)

    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    val reassignment = Map(
      new TopicPartition("topic_a", 0) -> Seq(0, 1, 3),
      new TopicPartition("topic_a", 1) -> Seq(2, 1, 3),
      new TopicPartition("topic_b", 0) -> Seq(4, 5),
      new TopicPartition("topic_c", 0) -> Seq(5, 3)
    )

    // Should throw ControllerMovedException if the controller epoch zkVersion does not match
    assertThrows(classOf[ControllerMovedException], () => zkClient.setOrCreatePartitionReassignment(reassignment, controllerEpochZkVersion + 1))

    zkClient.setOrCreatePartitionReassignment(reassignment, controllerEpochZkVersion)
    assertEquals(reassignment, zkClient.getPartitionReassignment)

    val updatedReassignment = reassignment - new TopicPartition("topic_b", 0)
    zkClient.setOrCreatePartitionReassignment(updatedReassignment, controllerEpochZkVersion)
    assertEquals(updatedReassignment, zkClient.getPartitionReassignment)

    zkClient.deletePartitionReassignment(controllerEpochZkVersion)
    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    zkClient.createPartitionReassignment(reassignment)
    assertEquals(reassignment, zkClient.getPartitionReassignment)
  }

  @Test
  def testGetDataAndStat(): Unit = {
    val path = "/testpath"

    // test with non-existing path
    val (data0, version0) = zkClient.getDataAndStat(path)
    assertTrue(data0.isEmpty)
    assertEquals(0, version0.getVersion)

    // create a test path
    zkClient.createRecursive(path)
    zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)

    // test with existing path
    val (data1, version1) = zkClient.getDataAndStat(path)
    assertEquals("version1", new String(data1.get, UTF_8))
    assertEquals(1, version1.getVersion)

    zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 1)
    val (data2, version2) = zkClient.getDataAndStat(path)
    assertEquals("version2", new String(data2.get, UTF_8))
    assertEquals(2, version2.getVersion)
  }

  @Test
  def testGetChildren(): Unit = {
    val path = "/testpath"

    // test with non-existing path
    assertTrue(zkClient.getChildren(path).isEmpty)

    // create child nodes
    zkClient.createRecursive( "/testpath/child1")
    zkClient.createRecursive( "/testpath/child2")
    zkClient.createRecursive( "/testpath/child3")

    val children = zkClient.getChildren(path)

    assertEquals(3, children.size)
    assertEquals(Set("child1","child2","child3"), children.toSet)
  }

  @Test
  def testAclManagementMethods(): Unit = {
    ZkAclStore.stores.foreach(store => {
      assertFalse(zkClient.pathExists(store.aclPath))
      assertFalse(zkClient.pathExists(store.changeStore.aclChangePath))
      AclEntry.ResourceTypes.foreach(resource => assertFalse(zkClient.pathExists(store.path(resource))))
    })

    // create acl paths
    zkClient.createAclPaths()

    ZkAclStore.stores.foreach(store => {
      assertTrue(zkClient.pathExists(store.aclPath))
      assertTrue(zkClient.pathExists(store.changeStore.aclChangePath))
      AclEntry.ResourceTypes.foreach(resource => assertTrue(zkClient.pathExists(store.path(resource))))

      val resource1 = new ResourcePattern(TOPIC, Uuid.randomUuid().toString, store.patternType)
      val resource2 = new ResourcePattern(TOPIC, Uuid.randomUuid().toString, store.patternType)

      // try getting acls for non-existing resource
      var versionedAcls = zkClient.getVersionedAclsForResource(resource1)
      assertTrue(versionedAcls.acls.isEmpty)
      assertEquals(ZkVersion.UnknownVersion, versionedAcls.zkVersion)
      assertFalse(zkClient.resourceExists(resource1))


      val acl1 = AclEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), DENY, "host1" , READ)
      val acl2 = AclEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), ALLOW, "*", READ)
      val acl3 = AclEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), DENY, "host1", READ)

      // Conditional set should fail if path not created
      assertFalse(zkClient.conditionalSetAclsForResource(resource1, Set(acl1, acl3), 0)._1)

      //create acls for resources
      assertTrue(zkClient.createAclsForResourceIfNotExists(resource1, Set(acl1, acl2))._1)
      assertTrue(zkClient.createAclsForResourceIfNotExists(resource2, Set(acl1, acl3))._1)

      // Create should fail if path already exists
      assertFalse(zkClient.createAclsForResourceIfNotExists(resource2, Set(acl1, acl3))._1)

      versionedAcls = zkClient.getVersionedAclsForResource(resource1)
      assertEquals(Set(acl1, acl2), versionedAcls.acls)
      assertEquals(0, versionedAcls.zkVersion)
      assertTrue(zkClient.resourceExists(resource1))

      //update acls for resource
      assertTrue(zkClient.conditionalSetAclsForResource(resource1, Set(acl1, acl3), 0)._1)

      versionedAcls = zkClient.getVersionedAclsForResource(resource1)
      assertEquals(Set(acl1, acl3), versionedAcls.acls)
      assertEquals(1, versionedAcls.zkVersion)

      //get resource Types
      assertEquals(AclEntry.ResourceTypes.map(SecurityUtils.resourceTypeName), zkClient.getResourceTypes(store.patternType).toSet)

      //get resource name
      val resourceNames = zkClient.getResourceNames(store.patternType, TOPIC)
      assertEquals(2, resourceNames.size)
      assertTrue(Set(resource1.name,resource2.name) == resourceNames.toSet)

      //delete resource
      assertTrue(zkClient.deleteResource(resource1))
      assertFalse(zkClient.resourceExists(resource1))

      //delete with invalid expected zk version
      assertFalse(zkClient.conditionalDelete(resource2, 10))
      //delete with valid expected zk version
      assertTrue(zkClient.conditionalDelete(resource2, 0))

      zkClient.createAclChangeNotification(new ResourcePattern(GROUP, "resource1", store.patternType))
      zkClient.createAclChangeNotification(new ResourcePattern(TOPIC, "resource2", store.patternType))

      assertEquals(2, zkClient.getChildren(store.changeStore.aclChangePath).size)

      zkClient.deleteAclChangeNotifications()
      assertTrue(zkClient.getChildren(store.changeStore.aclChangePath).isEmpty)
    })
  }

  @Test
  def testDeletePath(): Unit = {
    val path = "/a/b/c"
    zkClient.createRecursive(path)
    zkClient.deletePath(path)
    assertFalse(zkClient.pathExists(path))

    zkClient.createRecursive(path)
    zkClient.deletePath("/a")
    assertFalse(zkClient.pathExists(path))

    zkClient.createRecursive(path)
    zkClient.deletePath(path, recursiveDelete =  false)
    assertFalse(zkClient.pathExists(path))
    assertTrue(zkClient.pathExists("/a/b"))
  }

  @Test
  def testDeleteTopicZNode(): Unit = {
    zkClient.deleteTopicZNode(topic1, controllerEpochZkVersion)
    zkClient.createRecursive(TopicZNode.path(topic1))
    zkClient.deleteTopicZNode(topic1, controllerEpochZkVersion)
    assertFalse(zkClient.pathExists(TopicZNode.path(topic1)))
  }

  @Test
  def testDeleteTopicPathMethods(): Unit = {
    assertFalse(zkClient.isTopicMarkedForDeletion(topic1))
    assertTrue(zkClient.getTopicDeletions.isEmpty)

    zkClient.createDeleteTopicPath(topic1)
    zkClient.createDeleteTopicPath(topic2)

    assertTrue(zkClient.isTopicMarkedForDeletion(topic1))
    assertEquals(Set(topic1, topic2), zkClient.getTopicDeletions.toSet)

    assertThrows(classOf[ControllerMovedException], () => zkClient.deleteTopicDeletions(Seq(topic1, topic2), controllerEpochZkVersion + 1))
    assertEquals(Set(topic1, topic2), zkClient.getTopicDeletions.toSet)

    zkClient.deleteTopicDeletions(Seq(topic1, topic2), controllerEpochZkVersion)
    assertTrue(zkClient.getTopicDeletions.isEmpty)
  }

  private def assertPathExistenceAndData(expectedPath: String, data: String): Unit = {
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some(data), dataAsString(expectedPath))
   }

  @Test
  def testCreateTokenChangeNotification(): Unit = {
    assertThrows(classOf[NoNodeException], () => zkClient.createTokenChangeNotification("delegationToken"))
    zkClient.createDelegationTokenPaths()

    zkClient.createTokenChangeNotification("delegationToken")
    assertPathExistenceAndData("/delegation_token/token_changes/token_change_0000000000", "delegationToken")
  }

  @Test
  def testEntityConfigManagementMethods(): Unit = {
    assertTrue(zkClient.getEntityConfigs(ConfigType.TOPIC, topic1).isEmpty)

    zkClient.setOrCreateEntityConfigs(ConfigType.TOPIC, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.TOPIC, topic1))

    logProps.remove(TopicConfig.CLEANUP_POLICY_CONFIG)
    zkClient.setOrCreateEntityConfigs(ConfigType.TOPIC, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.TOPIC, topic1))

    zkClient.setOrCreateEntityConfigs(ConfigType.TOPIC, topic2, logProps)
    assertEquals(Set(topic1, topic2), zkClient.getAllEntitiesWithConfig(ConfigType.TOPIC).toSet)

    zkClient.deleteTopicConfigs(Seq(topic1, topic2), controllerEpochZkVersion)
    assertTrue(zkClient.getEntityConfigs(ConfigType.TOPIC, topic1).isEmpty)
  }

  @Test
  def testCreateConfigChangeNotification(): Unit = {
    assertFalse(zkClient.pathExists(ConfigEntityChangeNotificationZNode.path))

    // The parent path is created if needed
    zkClient.createConfigChangeNotification(ConfigEntityZNode.path(ConfigType.TOPIC, topic1))
    assertPathExistenceAndData(
      "/config/changes/config_change_0000000000",
      """{"version":2,"entity_path":"/config/topics/topic1"}""")

    // Creation does not fail if the parent path exists
    zkClient.createConfigChangeNotification(ConfigEntityZNode.path(ConfigType.TOPIC, topic2))
    assertPathExistenceAndData(
      "/config/changes/config_change_0000000001",
      """{"version":2,"entity_path":"/config/topics/topic2"}""")
  }

  private def createLogProps(bytesProp: Int): Properties = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, bytesProp.toString)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, bytesProp.toString)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    logProps
  }

  private val logProps = createLogProps(1024)

  @Test
  def testGetLogConfigs(): Unit = {
    val emptyConfig = new LogConfig(Collections.emptyMap())
    assertEquals((Map(topic1 -> emptyConfig), Map.empty),
      zkClient.getLogConfigs(Set(topic1), Collections.emptyMap()),
      "Non existent config, no defaults")

    val logProps2 = createLogProps(2048)

    zkClient.setOrCreateEntityConfigs(ConfigType.TOPIC, topic1, logProps)
    assertEquals((Map(topic1 -> new LogConfig(logProps), topic2 -> emptyConfig), Map.empty),
      zkClient.getLogConfigs(Set(topic1, topic2), Collections.emptyMap()),
      "One existing and one non-existent topic")

    zkClient.setOrCreateEntityConfigs(ConfigType.TOPIC, topic2, logProps2)
    assertEquals((Map(topic1 -> new LogConfig(logProps), topic2 -> new LogConfig(logProps2)), Map.empty),
      zkClient.getLogConfigs(Set(topic1, topic2), Collections.emptyMap()),
      "Two existing topics")

    val logProps1WithMoreValues = createLogProps(1024)
    logProps1WithMoreValues.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, "100")
    logProps1WithMoreValues.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1024")

    assertEquals((Map(topic1 -> new LogConfig(logProps1WithMoreValues)), Map.empty),
      zkClient.getLogConfigs(Set(topic1),
        Map[String, AnyRef](TopicConfig.SEGMENT_JITTER_MS_CONFIG -> "100", TopicConfig.SEGMENT_BYTES_CONFIG -> "128").asJava),
      "Config with defaults")
  }

  private def createBrokerInfo(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol,
                               rack: Option[String] = None,
                               features: Features[SupportedVersionRange] = emptySupportedFeatures): BrokerInfo =
    BrokerInfo(
      Broker(
        id,
        Seq(new EndPoint(host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)),
        rack = rack,
        features = features),
      MetadataVersion.latestTesting, jmxPort = port + 10)

  @Test
  def testRegisterBrokerInfo(): Unit = {
    zkClient.createTopLevelPaths()

    val brokerInfo = createBrokerInfo(
      1, "test.host", 9999, SecurityProtocol.PLAINTEXT,
      rack = None,
      features = Features.supportedFeatures(
        Map[String, SupportedVersionRange](
          "feature1" -> new SupportedVersionRange(1, 2)).asJava))
    val differentBrokerInfoWithSameId = createBrokerInfo(
      1, "test.host2", 9995, SecurityProtocol.SSL,
      features = Features.supportedFeatures(
        Map[String, SupportedVersionRange](
          "feature2" -> new SupportedVersionRange(4, 7)).asJava))

    zkClient.registerBroker(brokerInfo)
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))
    assertEquals(Some(brokerInfo.broker), otherZkClient.getBroker(1), "Other ZK clients can read broker info")

    // Node exists, owned by current session - no error, no update
    zkClient.registerBroker(differentBrokerInfoWithSameId)
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))

    // Other client tries to register broker with same id causes failure, info is not changed in ZK
    assertThrows(classOf[NodeExistsException], () => otherZkClient.registerBroker(differentBrokerInfoWithSameId))
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))
  }

  @Test
  def testRetryRegisterBrokerInfo(): Unit = {
    val brokerId = 5
    val brokerPort = 9999
    val brokerHost = "test.host"
    val expiredBrokerInfo = createBrokerInfo(brokerId, brokerHost, brokerPort, SecurityProtocol.PLAINTEXT)
    expiredSessionZkClient.createTopLevelPaths()

    // Register the broker, for the first time
    expiredSessionZkClient.registerBroker(expiredBrokerInfo)
    assertEquals(Some(expiredBrokerInfo.broker), expiredSessionZkClient.getBroker(brokerId))
    val originalCzxid = expiredSessionZkClient.getPathCzxid(BrokerIdZNode.path(brokerId))

    // Here, the node exists already, when trying to register under a different session id,
    // the node will be deleted and created again using the new session id.
    expiredSessionZkClient.registerBroker(expiredBrokerInfo)

    // The broker info should be the same, no error should be raised
    assertEquals(Some(expiredBrokerInfo.broker), expiredSessionZkClient.getBroker(brokerId))
    val newCzxid = expiredSessionZkClient.getPathCzxid(BrokerIdZNode.path(brokerId))

    assertNotEquals(originalCzxid, newCzxid, "The Czxid of original ephemeral znode should be different " +
      "from the new ephemeral znode Czxid")
  }

  @Test
  def testGetBrokerMethods(): Unit = {
    zkClient.createTopLevelPaths()

    assertEquals(Seq.empty,zkClient.getAllBrokersInCluster)
    assertEquals(Seq.empty, zkClient.getSortedBrokerList)
    assertEquals(None, zkClient.getBroker(0))

    val brokerInfo0 = createBrokerInfo(
      0, "test.host0", 9998, SecurityProtocol.PLAINTEXT,
      features = Features.supportedFeatures(
        Map[String, SupportedVersionRange](
          "feature1" -> new SupportedVersionRange(1, 2)).asJava))
    val brokerInfo1 = createBrokerInfo(
      1, "test.host1", 9999, SecurityProtocol.SSL,
      features = Features.supportedFeatures(
        Map[String, SupportedVersionRange](
          "feature2" -> new SupportedVersionRange(3, 6)).asJava))

    zkClient.registerBroker(brokerInfo1)
    otherZkClient.registerBroker(brokerInfo0)

    assertEquals(Seq(0, 1), zkClient.getSortedBrokerList)
    assertEquals(
      Seq(brokerInfo0.broker, brokerInfo1.broker),
      zkClient.getAllBrokersInCluster
    )
    assertEquals(Some(brokerInfo0.broker), zkClient.getBroker(0))
  }

  @Test
  def testUpdateBrokerInfo(): Unit = {
    zkClient.createTopLevelPaths()

    // Updating info of a broker not existing in ZK fails
    val originalBrokerInfo = createBrokerInfo(1, "test.host", 9999, SecurityProtocol.PLAINTEXT)
    assertThrows(classOf[NoNodeException], () => zkClient.updateBrokerInfo(originalBrokerInfo))

    zkClient.registerBroker(originalBrokerInfo)

    val updatedBrokerInfo = createBrokerInfo(1, "test.host2", 9995, SecurityProtocol.SSL)
    zkClient.updateBrokerInfo(updatedBrokerInfo)
    assertEquals(Some(updatedBrokerInfo.broker), zkClient.getBroker(1))

    // Other ZK clients can update info
    otherZkClient.updateBrokerInfo(originalBrokerInfo)
    assertEquals(Some(originalBrokerInfo.broker), otherZkClient.getBroker(1))
  }

  private def statWithVersion(version: Int): Stat = {
    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    stat.setVersion(version)
    stat
  }

  private def leaderIsrAndControllerEpochs(state: Int, partitionEpoch: Int): Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    Map(
      topicPartition10 -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(leader = 1, leaderEpoch = state, isr = List(2 + state, 3 + state), LeaderRecoveryState.RECOVERED, partitionEpoch = partitionEpoch),
        controllerEpoch = 4),
      topicPartition11 -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(leader = 0, leaderEpoch = state + 1, isr = List(1 + state, 2 + state), LeaderRecoveryState.RECOVERED, partitionEpoch = partitionEpoch),
        controllerEpoch = 4))

  val initialLeaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    leaderIsrAndControllerEpochs(0, 0)

  val initialLeaderIsrs: Map[TopicPartition, LeaderAndIsr] =
    initialLeaderIsrAndControllerEpochs.map { case (k, v) => k -> v.leaderAndIsr }

  private def leaderIsrs(state: Int, partitionEpoch: Int): Map[TopicPartition, LeaderAndIsr] =
    leaderIsrAndControllerEpochs(state, partitionEpoch).map { case (k, v) => k -> v.leaderAndIsr }

  private def checkUpdateLeaderAndIsrResult(
                  expectedSuccessfulPartitions: Map[TopicPartition, LeaderAndIsr],
                  expectedPartitionsToRetry: Seq[TopicPartition],
                  expectedFailedPartitions: Map[TopicPartition, (Class[_], String)],
                  actualUpdateLeaderAndIsrResult: UpdateLeaderAndIsrResult): Unit = {
    val failedPartitionsExcerpt = mutable.Map.empty[TopicPartition, (Class[_], String)]
    val successfulPartitions = mutable.Map.empty[TopicPartition, LeaderAndIsr]

    actualUpdateLeaderAndIsrResult.finishedPartitions.foreach {
      case (partition, Left(e)) => failedPartitionsExcerpt += partition -> (e.getClass, e.getMessage)
      case (partition, Right(leaderAndIsr)) => successfulPartitions += partition -> leaderAndIsr
    }

    assertEquals(expectedFailedPartitions,
      failedPartitionsExcerpt, "Permanently failed updates do not match expected")
    assertEquals(expectedPartitionsToRetry,
      actualUpdateLeaderAndIsrResult.partitionsToRetry, "Retriable updates (due to BADVERSION) do not match expected")
    assertEquals(expectedSuccessfulPartitions,
      successfulPartitions, "Successful updates do not match expected")
  }

  @Test
  def testTopicAssignments(): Unit = {
    val topicId = Some(Uuid.randomUuid())
    assertEquals(0, zkClient.getPartitionAssignmentForTopics(Set(topicPartition.topic())).size)
    zkClient.createTopicAssignment(topicPartition.topic(), topicId,
      Map(topicPartition -> Seq()))

    val expectedAssignment = ReplicaAssignment(Seq(1,2,3), Seq(1), Seq(3))
    val response = zkClient.setTopicAssignmentRaw(topicPartition.topic(), topicId,
      Map(topicPartition -> expectedAssignment), controllerEpochZkVersion)
    assertEquals(Code.OK, response.resultCode)

    val topicPartitionAssignments = zkClient.getPartitionAssignmentForTopics(Set(topicPartition.topic()))
    assertEquals(1, topicPartitionAssignments.size)
    assertTrue(topicPartitionAssignments.contains(topicPartition.topic()))
    val partitionAssignments = topicPartitionAssignments(topicPartition.topic())
    assertEquals(1, partitionAssignments.size)
    assertTrue(partitionAssignments.contains(topicPartition.partition()))
    val assignment = partitionAssignments(topicPartition.partition())
    assertEquals(expectedAssignment, assignment)
  }

  @Test
  def testUpdateLeaderAndIsr(): Unit = {
    zkClient.createRecursive(TopicZNode.path(topic1))

    // Non-existing topicPartitions
    checkUpdateLeaderAndIsrResult(
        Map.empty,
        mutable.ArrayBuffer.empty,
      Map(
        topicPartition10 -> (classOf[NoNodeException], "KeeperErrorCode = NoNode for /brokers/topics/topic1/partitions/0/state"),
        topicPartition11 -> (classOf[NoNodeException], "KeeperErrorCode = NoNode for /brokers/topics/topic1/partitions/1/state")),
      zkClient.updateLeaderAndIsr(initialLeaderIsrs, controllerEpoch = 4, controllerEpochZkVersion))

    zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)

    // Mismatch controller epoch zkVersion
    assertThrows(classOf[ControllerMovedException], () => zkClient.updateLeaderAndIsr(initialLeaderIsrs, controllerEpoch = 4, controllerEpochZkVersion + 1))

    // successful updates
    checkUpdateLeaderAndIsrResult(
      leaderIsrs(state = 1, partitionEpoch = 1),
      mutable.ArrayBuffer.empty,
      Map.empty,
      zkClient.updateLeaderAndIsr(leaderIsrs(state = 1, partitionEpoch = 0),controllerEpoch = 4, controllerEpochZkVersion))

    // Try to update with wrong ZK version
    checkUpdateLeaderAndIsrResult(
      Map.empty,
      ArrayBuffer(topicPartition10, topicPartition11),
      Map.empty,
      zkClient.updateLeaderAndIsr(leaderIsrs(state = 1, partitionEpoch = 0),controllerEpoch = 4, controllerEpochZkVersion))

    // Trigger successful, to be retried and failed partitions in same call
    val mixedState = Map(
      topicPartition10 -> LeaderAndIsr(leader = 1, leaderEpoch = 2, isr = List(4, 5), LeaderRecoveryState.RECOVERED, partitionEpoch = 1),
      topicPartition11 -> LeaderAndIsr(leader = 0, leaderEpoch = 2, isr = List(3, 4), LeaderRecoveryState.RECOVERED, partitionEpoch = 0),
      topicPartition20 -> LeaderAndIsr(leader = 0, leaderEpoch = 2, isr = List(3, 4), LeaderRecoveryState.RECOVERED, partitionEpoch = 0))

    checkUpdateLeaderAndIsrResult(
      leaderIsrs(state = 2, partitionEpoch = 2).filter { case (tp, _) => tp == topicPartition10 },
      ArrayBuffer(topicPartition11),
      Map(
        topicPartition20 -> (classOf[NoNodeException], "KeeperErrorCode = NoNode for /brokers/topics/topic2/partitions/0/state")),
      zkClient.updateLeaderAndIsr(mixedState, controllerEpoch = 4, controllerEpochZkVersion))
  }

  private def checkGetDataResponse(
      leaderIsrAndControllerEpochs: Map[TopicPartition,LeaderIsrAndControllerEpoch],
      topicPartition: TopicPartition,
      response: GetDataResponse): Unit = {
    val zkVersion = leaderIsrAndControllerEpochs(topicPartition).leaderAndIsr.partitionEpoch
    assertEquals(Code.OK, response.resultCode)
    assertEquals(TopicPartitionStateZNode.path(topicPartition), response.path)
    assertEquals(Some(topicPartition), response.ctx)
    assertEquals(
      Some(leaderIsrAndControllerEpochs(topicPartition)),
      TopicPartitionStateZNode.decode(response.data, statWithVersion(zkVersion)))
  }

  private def eraseMetadata(response: CreateResponse): CreateResponse =
    response.copy(metadata = ResponseMetadata(0, 0))

  @Test
  def testGetTopicsAndPartitions(): Unit = {
    assertTrue(zkClient.getAllTopicsInCluster().isEmpty)
    assertTrue(zkClient.getAllPartitions.isEmpty)

    zkClient.createRecursive(TopicZNode.path(topic1))
    zkClient.createRecursive(TopicZNode.path(topic2))
    assertEquals(Set(topic1, topic2), zkClient.getAllTopicsInCluster())

    assertTrue(zkClient.getAllPartitions.isEmpty)

    zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)
    assertEquals(Set(topicPartition10, topicPartition11), zkClient.getAllPartitions)
  }

  @Test
  def testCreateAndGetTopicPartitionStatesRaw(): Unit = {
    zkClient.createRecursive(TopicZNode.path(topic1))

    // Mismatch controller epoch zkVersion
    assertThrows(classOf[ControllerMovedException], () => zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion + 1))

    assertEquals(
      Seq(
        CreateResponse(Code.OK, TopicPartitionStateZNode.path(topicPartition10), Some(topicPartition10),
          TopicPartitionStateZNode.path(topicPartition10), ResponseMetadata(0, 0)),
        CreateResponse(Code.OK, TopicPartitionStateZNode.path(topicPartition11), Some(topicPartition11),
          TopicPartitionStateZNode.path(topicPartition11), ResponseMetadata(0, 0))),
      zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)
        .map(eraseMetadata).toList)

    val getResponses = zkClient.getTopicPartitionStatesRaw(topicPartitions10_11)
    assertEquals(2, getResponses.size)
    topicPartitions10_11.zip(getResponses) foreach {case (tp, r) => checkGetDataResponse(initialLeaderIsrAndControllerEpochs, tp, r)}

    // Trying to create existing topicPartition states fails
    assertEquals(
      Seq(
        CreateResponse(Code.NODEEXISTS, TopicPartitionStateZNode.path(topicPartition10), Some(topicPartition10), null, ResponseMetadata(0, 0)),
        CreateResponse(Code.NODEEXISTS, TopicPartitionStateZNode.path(topicPartition11), Some(topicPartition11), null, ResponseMetadata(0, 0))),
      zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion).map(eraseMetadata).toList)
  }

  @Test
  def testSetTopicPartitionStatesRaw(): Unit = {

    def expectedSetDataResponses(topicPartitions: TopicPartition*)(resultCode: Code, stat: Stat) =
      topicPartitions.map { topicPartition =>
        SetDataResponse(resultCode, TopicPartitionStateZNode.path(topicPartition),
          Some(topicPartition), stat, ResponseMetadata(0, 0))
      }

    zkClient.createRecursive(TopicZNode.path(topic1))

    // Trying to set non-existing topicPartition's data results in NONODE responses
    assertEquals(
      expectedSetDataResponses(topicPartition10, topicPartition11)(Code.NONODE, null),
      zkClient.setTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion).map {
        _.copy(metadata = ResponseMetadata(0, 0))}.toList)

    zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)

    assertEquals(
      expectedSetDataResponses(topicPartition10, topicPartition11)(Code.OK, statWithVersion(1)),
      zkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 1, partitionEpoch = 0), controllerEpochZkVersion).map {
        eraseMetadataAndStat}.toList)

    // Mismatch controller epoch zkVersion
    assertThrows(classOf[ControllerMovedException], () => zkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 1, partitionEpoch = 0), controllerEpochZkVersion + 1))

    val getResponses = zkClient.getTopicPartitionStatesRaw(topicPartitions10_11)
    assertEquals(2, getResponses.size)
    topicPartitions10_11.zip(getResponses) foreach {case (tp, r) => checkGetDataResponse(leaderIsrAndControllerEpochs(state = 1, partitionEpoch = 0), tp, r)}

    // Other ZK client can also write the state of a partition
    assertEquals(
      expectedSetDataResponses(topicPartition10, topicPartition11)(Code.OK, statWithVersion(2)),
      otherZkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 2, partitionEpoch = 1), controllerEpochZkVersion).map {
        eraseMetadataAndStat}.toList)
  }

  @Test
  def testReassignPartitionsInProgress(): Unit = {
    assertFalse(zkClient.reassignPartitionsInProgress)
    zkClient.createRecursive(ReassignPartitionsZNode.path)
    assertTrue(zkClient.reassignPartitionsInProgress)
  }

  @Test
  def testGetTopicPartitionStates(): Unit = {
    assertEquals(None, zkClient.getTopicPartitionState(topicPartition10))
    assertEquals(None, zkClient.getLeaderForPartition(topicPartition10))

    zkClient.createRecursive(TopicZNode.path(topic1))

    zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)
    assertEquals(
      initialLeaderIsrAndControllerEpochs,
      zkClient.getTopicPartitionStates(Seq(topicPartition10, topicPartition11))
    )

    assertEquals(
      Some(initialLeaderIsrAndControllerEpochs(topicPartition10)),
      zkClient.getTopicPartitionState(topicPartition10)
    )

    assertEquals(Some(1), zkClient.getLeaderForPartition(topicPartition10))

    val notExistingPartition = new TopicPartition(topic1, 2)
    assertTrue(zkClient.getTopicPartitionStates(Seq(notExistingPartition)).isEmpty)
    assertEquals(
      Map(topicPartition10 -> initialLeaderIsrAndControllerEpochs(topicPartition10)),
      zkClient.getTopicPartitionStates(Seq(topicPartition10, notExistingPartition))
    )

    assertEquals(None, zkClient.getTopicPartitionState(notExistingPartition))
    assertEquals(None, zkClient.getLeaderForPartition(notExistingPartition))

  }

  private def eraseMetadataAndStat(response: SetDataResponse): SetDataResponse = {
    val stat = if (response.stat != null) statWithVersion(response.stat.getVersion) else null
    response.copy(metadata = ResponseMetadata(0, 0), stat = stat)
  }

  @Test
  def testControllerEpochMethods(): Unit = {
    zkClient.deletePath(ControllerEpochZNode.path)

    assertEquals(None, zkClient.getControllerEpoch)

    assertEquals(SetDataResponse(Code.NONODE, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)),
      "Setting non existing nodes should return NONODE results")

    assertEquals(CreateResponse(Code.OK, ControllerEpochZNode.path, None, ControllerEpochZNode.path, ResponseMetadata(0, 0)),
      eraseMetadata(zkClient.createControllerEpochRaw(0)),
      "Creating non existing nodes is OK")
    assertEquals(0, zkClient.getControllerEpoch.get._1)

    assertEquals(CreateResponse(Code.NODEEXISTS, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadata(zkClient.createControllerEpochRaw(0)),
      "Attempt to create existing nodes should return NODEEXISTS")

    assertEquals(SetDataResponse(Code.OK, ControllerEpochZNode.path, None, statWithVersion(1), ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)),
      "Updating existing nodes is OK")
    assertEquals(1, zkClient.getControllerEpoch.get._1)

    assertEquals(SetDataResponse(Code.BADVERSION, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)),
      "Updating with wrong ZK version returns BADVERSION")
  }

  @Test
  def testRegisterZkControllerAfterKRaft(): Unit = {
    // Register KRaft
    var controllerEpochZkVersion = -1
    zkClient.tryRegisterKRaftControllerAsActiveController(3000, 42) match {
      case SuccessfulRegistrationResult(kraftEpoch, zkVersion) =>
        assertEquals(2, kraftEpoch)
        controllerEpochZkVersion = zkVersion
      case FailedRegistrationResult() => fail("Expected to register KRaft as controller in ZK")
    }
    assertEquals(1, controllerEpochZkVersion)

    // Can't register ZK anymore
    assertThrows(classOf[ControllerMovedException], () => zkClient.registerControllerAndIncrementControllerEpoch(1))

    // Delete controller, and try again
    zkClient.deleteController(controllerEpochZkVersion)
    val (newEpoch, newZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(1)
    assertEquals(3, newEpoch)
    assertEquals(2, newZkVersion)

    zkClient.tryRegisterKRaftControllerAsActiveController(3000, 42) match {
      case SuccessfulRegistrationResult(zkEpoch, zkVersion) =>
        assertEquals(4, zkEpoch)
        assertEquals(3, zkVersion)
      case FailedRegistrationResult() => fail("Expected to register KRaft as controller in ZK")
    }
  }

  @Test
  def testConcurrentKRaftControllerClaim(): Unit = {
    // Setup three threads to race on registering a KRaft controller in ZK
    val registeredEpochs = new java.util.concurrent.ConcurrentLinkedQueue[Integer]()
    val registeringNodes = new java.util.concurrent.ConcurrentHashMap[Integer, Integer]()

    def newThread(nodeId: Int): Runnable = {
      () => {
        0.to(999).foreach(epoch =>
          zkClient.tryRegisterKRaftControllerAsActiveController(nodeId, epoch) match {
            case SuccessfulRegistrationResult(writtenEpoch, _) =>
              registeredEpochs.add(writtenEpoch)
              registeringNodes.compute(nodeId, (_, count) => if (count == null) {
                0
              } else {
                count + 1
              })
            case FailedRegistrationResult() =>
          }
        )
      }
    }
    val thread1 = newThread(1)
    val thread2 = newThread(2)
    val thread3 = newThread(3)
    val executor = Executors.newFixedThreadPool(3)
    executor.submit(thread1)
    executor.submit(thread2)
    executor.submit(thread3)
    executor.shutdown()
    executor.awaitTermination(30, TimeUnit.SECONDS)

    assertEquals(1000, registeredEpochs.size())
    val uniqueEpochs = registeredEpochs.asScala.toSet
    assertEquals(1000, uniqueEpochs.size)
  }

  @Test
  def testControllerManagementMethods(): Unit = {
    // No controller
    assertEquals(None, zkClient.getControllerId)
    // Create controller
    val (_, newEpochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(controllerId = 1)
    assertEquals(Some(1), zkClient.getControllerId)
    zkClient.deleteController(newEpochZkVersion)
    assertEquals(None, zkClient.getControllerId)
  }

  @Test
  def testZNodeChangeHandlerForDataChange(): Unit = {
    val mockPath = "/foo"

    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }

      override val path: String = mockPath
    }

    zkClient.registerZNodeChangeHandlerAndCheckExistence(zNodeChangeHandler)
    zkClient.createRecursive(mockPath)
    assertTrue(znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS), "Failed to receive create notification")
  }

  @Test
  def testClusterIdMethods(): Unit = {
    val clusterId = CoreUtils.generateUuidAsBase64()

    zkClient.createOrGetClusterId(clusterId)
    assertEquals(clusterId, zkClient.getClusterId.getOrElse(fail("No cluster id found")))
   }

  @Test
  def testBrokerSequenceIdMethods(): Unit = {
    val sequenceId = zkClient.generateBrokerSequenceId()
    assertEquals(sequenceId + 1, zkClient.generateBrokerSequenceId())
  }

  @Test
  def testCreateTopLevelPaths(): Unit = {
    zkClient.createTopLevelPaths()

    ZkData.PersistentZkPaths.foreach(path => assertTrue(zkClient.pathExists(path)))
  }

  @Test
  def testPreferredReplicaElectionMethods(): Unit = {

    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)

    val electionPartitions = Set(new TopicPartition(topic1, 0), new TopicPartition(topic1, 1))

    zkClient.createPreferredReplicaElection(electionPartitions)
    assertEquals(electionPartitions, zkClient.getPreferredReplicaElection)

    assertThrows(classOf[NodeExistsException], () => zkClient.createPreferredReplicaElection(electionPartitions))

    // Mismatch controller epoch zkVersion
    assertThrows(classOf[ControllerMovedException], () => zkClient.deletePreferredReplicaElection(controllerEpochZkVersion + 1))
    assertEquals(electionPartitions, zkClient.getPreferredReplicaElection)

    zkClient.deletePreferredReplicaElection(controllerEpochZkVersion)
    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)
  }

  private def dataAsString(path: String): Option[String] = {
    val (data, _) = zkClient.getDataAndStat(path)
    data.map(new String(_, UTF_8))
  }

  @Test
  def testDelegationTokenMethods(): Unit = {
    assertFalse(zkClient.pathExists(DelegationTokensZNode.path))
    assertFalse(zkClient.pathExists(DelegationTokenChangeNotificationZNode.path))

    zkClient.createDelegationTokenPaths()
    assertTrue(zkClient.pathExists(DelegationTokensZNode.path))
    assertTrue(zkClient.pathExists(DelegationTokenChangeNotificationZNode.path))

    val tokenId = "token1"
    val owner = SecurityUtils.parseKafkaPrincipal("User:owner1")
    val renewers = List(SecurityUtils.parseKafkaPrincipal("User:renewer1"), SecurityUtils.parseKafkaPrincipal("User:renewer1"))

    val tokenInfo = new TokenInformation(tokenId, owner, renewers.asJava,
      System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis())
    val bytes = new Array[Byte](20)
    Random.nextBytes(bytes)
    val token = new org.apache.kafka.common.security.token.delegation.DelegationToken(tokenInfo, bytes)

    // test non-existent token
    assertTrue(zkClient.getDelegationTokenInfo(tokenId).isEmpty)
    assertFalse(zkClient.deleteDelegationToken(tokenId))

    // create a token
    zkClient.setOrCreateDelegationToken(token)

    //get created token
    assertEquals(tokenInfo, zkClient.getDelegationTokenInfo(tokenId).get)

    //update expiryTime
    tokenInfo.setExpiryTimestamp(System.currentTimeMillis())
    zkClient.setOrCreateDelegationToken(token)

    //test updated token
    assertEquals(tokenInfo, zkClient.getDelegationTokenInfo(tokenId).get)

    //test deleting token
    assertTrue(zkClient.deleteDelegationToken(tokenId))
    assertEquals(None, zkClient.getDelegationTokenInfo(tokenId))
  }

  @Test
  def testConsumerOffsetPath(): Unit = {
    def getConsumersOffsetsZkPath(consumerGroup: String, topic: String, partition: Int): String = {
      s"/consumers/$consumerGroup/offsets/$topic/$partition"
    }

    val consumerGroup = "test-group"
    val topic = "test-topic"
    val partition = 2

    val expectedConsumerGroupOffsetsPath = getConsumersOffsetsZkPath(consumerGroup, topic, partition)
    val actualConsumerGroupOffsetsPath = ConsumerOffset.path(consumerGroup, topic, partition)

    assertEquals(expectedConsumerGroupOffsetsPath, actualConsumerGroupOffsetsPath)
  }

  @Test
  def testAclMethods(): Unit = {
    val mockPath = "/foo"

    assertThrows(classOf[NoNodeException], () => zkClient.getAcl(mockPath))

    assertThrows(classOf[NoNodeException], () => zkClient.setAcl(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala))

    zkClient.createRecursive(mockPath)

    zkClient.setAcl(mockPath, ZooDefs.Ids.READ_ACL_UNSAFE.asScala)

    assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE.asScala, zkClient.getAcl(mockPath))
  }

  @Test
  def testJuteMaxBuffer(): Unit = {

    def assertJuteMaxBufferConfig(clientConfig: ZKClientConfig, expectedValue: String): Unit = {
      val client = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
        zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "KafkaZkClient",
        zkClientConfig = clientConfig)
      try assertEquals(expectedValue, client.currentZooKeeper.getClientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER))
      finally client.close()
    }

    // default case
    assertEquals("4194304", zkClient.currentZooKeeper.getClientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER))

    // Value set directly on ZKClientConfig takes precedence over system property
    System.setProperty(ZKConfig.JUTE_MAXBUFFER, (3000 * 1024).toString)
    try {
      val clientConfig1 = new ZKClientConfig
      clientConfig1.setProperty(ZKConfig.JUTE_MAXBUFFER, (2000 * 1024).toString)
      assertJuteMaxBufferConfig(clientConfig1, expectedValue = "2048000")

      // System property value is used if value is not set in ZKClientConfig
      assertJuteMaxBufferConfig(new ZKClientConfig, expectedValue = "3072000")

    } finally System.clearProperty(ZKConfig.JUTE_MAXBUFFER)
  }

  @Test
  def testFailToUpdateMigrationZNode(): Unit = {
    val (controllerEpoch, stat) = zkClient.getControllerEpoch.get
    var migrationState = new ZkMigrationLeadershipState(3000, 42, 100, 42, Time.SYSTEM.milliseconds(), -1, controllerEpoch, stat.getVersion)
    migrationState = zkClient.getOrCreateMigrationState(migrationState)
    assertEquals(0, migrationState.migrationZkVersion())

    // A batch of migration writes to make. The last one will fail causing the migration znode to not be updated
    val requests_bad = Seq(
      CreateRequest("/foo", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo/bar", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo/bar/spam", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
    )

    migrationState = migrationState.withZkController(controllerEpoch, stat.getVersion)
    zkClient.retryMigrationRequestsUntilConnected(requests_bad, migrationState) match {
      case (zkVersion: Int, requests: Seq[AsyncRequest#Response]) =>
        assertEquals(0, zkVersion)
        assert(requests.take(3).forall(resp => resp.resultCode.equals(Code.OK)))
        assertEquals(Code.NODEEXISTS, requests.last.resultCode)
      case _ => fail()
    }

    // Check state again
    val loadedState = zkClient.getOrCreateMigrationState(ZkMigrationLeadershipState.EMPTY)
    assertEquals(0, loadedState.migrationZkVersion())

    // Resend the same requests, with the last one succeeding this time. This will result in NODEEXISTS, but
    // should still update the migration state
    val requests_good = Seq(
      CreateRequest("/foo", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo/bar", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo/bar/spam", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
      CreateRequest("/foo/bar/eggs", Array(), zkClient.defaultAcls("/foo"), CreateMode.PERSISTENT),
    )

    migrationState = migrationState.withZkController(controllerEpoch, stat.getVersion)
    zkClient.retryMigrationRequestsUntilConnected(requests_good, migrationState) match {
      case (zkVersion: Int, requests: Seq[AsyncRequest#Response]) =>
        assertEquals(1, zkVersion)
        assert(requests.take(3).forall(resp => resp.resultCode.equals(Code.NODEEXISTS)))
        assertEquals(Code.OK, requests.last.resultCode)
      case _ => fail()
    }
  }

  class ExpiredKafkaZkClient private (zooKeeperClient: ZooKeeperClient, isSecure: Boolean, time: Time)
    extends KafkaZkClient(zooKeeperClient, isSecure, time) {
    // Overwriting this method from the parent class to force the client to re-register the Broker.
    override def shouldReCreateEphemeralZNode(ephemeralOwnerId: Long): Boolean = {
      true
    }

    def getPathCzxid(path: String): Long = {
      val getDataRequest = GetDataRequest(path)
      val getDataResponse = retryRequestUntilConnected(getDataRequest)

      getDataResponse.stat.getCzxid
    }
  }

  private object ExpiredKafkaZkClient {
    def apply(connectString: String,
              isSecure: Boolean,
              sessionTimeoutMs: Int,
              connectionTimeoutMs: Int,
              maxInFlightRequests: Int,
              time: Time,
              metricGroup: String = "kafka.server",
              metricType: String = "SessionExpireListener") = {
      val zooKeeperClient = new ZooKeeperClient(connectString, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests,
        time, metricGroup, metricType, new ZKClientConfig, "ExpiredKafkaZkClient")
      new ExpiredKafkaZkClient(zooKeeperClient, isSecure, time)
    }
  }
}
