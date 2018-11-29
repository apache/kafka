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

import java.util.{Collections, Properties, UUID}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{CountDownLatch, TimeUnit}

import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.cluster.{Broker, EndPoint}
import kafka.log.LogConfig
import kafka.security.auth._
import kafka.server.ConfigType
import kafka.utils.CoreUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.TokenInformation
import org.apache.kafka.common.utils.{SecurityUtils, Time}
import org.apache.zookeeper.KeeperException.{Code, NoNodeException, NodeExistsException}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}
import scala.util.Random
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper._
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.Stat

class KafkaZkClientTest extends ZooKeeperTestHarness {

  private val group = "my-group"
  private val topic1 = "topic1"
  private val topic2 = "topic2"

  val topicPartition10 = new TopicPartition(topic1, 0)
  val topicPartition11 = new TopicPartition(topic1, 1)
  val topicPartition20 = new TopicPartition(topic2, 0)
  val topicPartitions10_11 = Seq(topicPartition10, topicPartition11)
  val controllerEpochZkVersion = 0

  var otherZkClient: KafkaZkClient = _
  var expiredSessionZkClient: ExpiredKafkaZkClient = _

  @Before
  override def setUp(): Unit = {
    super.setUp()
    zkClient.createControllerEpochRaw(1)
    otherZkClient = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM)
    expiredSessionZkClient = ExpiredKafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled),
      zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM)
  }

  @After
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
  def testSetAndGetConsumerOffset() {
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
  def testGetConsumerOffsetNoData() {
    zkClient.createRecursive(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition))
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
  }

  @Test
  def testDeleteRecursive() {
    zkClient.deleteRecursive("/delete/does-not-exist")

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    assertTrue(zkClient.deleteRecursive("/delete"))
    assertFalse(zkClient.pathExists("/delete"))

    intercept[IllegalArgumentException](zkClient.deleteRecursive("delete-invalid-path"))
  }

  @Test
  def testDeleteRecursiveWithControllerEpochVersionCheck(): Unit = {
    assertFalse(zkClient.deleteRecursive("/delete/does-not-exist", controllerEpochZkVersion))

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    intercept[ControllerMovedException](
      zkClient.deleteRecursive("/delete", controllerEpochZkVersion + 1))

    assertTrue(zkClient.deleteRecursive("/delete", controllerEpochZkVersion))
    assertFalse(zkClient.pathExists("/delete"))

    intercept[IllegalArgumentException](zkClient.deleteRecursive(
      "delete-invalid-path", controllerEpochZkVersion))
  }

  @Test
  def testCreateRecursive() {
    zkClient.createRecursive("/create-newrootpath")
    assertTrue(zkClient.pathExists("/create-newrootpath"))

    zkClient.createRecursive("/create/some/random/long/path")
    assertTrue(zkClient.pathExists("/create/some/random/long/path"))
    zkClient.createRecursive("/create/some/random/long/path", throwIfPathExists = false) // no errors if path already exists

    intercept[IllegalArgumentException](zkClient.createRecursive("create-invalid-path"))
  }

  @Test
  def testTopicAssignmentMethods() {
    assertTrue(zkClient.getAllTopicsInCluster.isEmpty)

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
    zkClient.createTopicAssignment(topic1, assignment)

    assertTrue(zkClient.topicExists(topic1))

    val expectedAssignment = assignment map { topicAssignment =>
      val partition = topicAssignment._1.partition
      val assignment = topicAssignment._2
      partition -> assignment
    }

    assertEquals(assignment.size, zkClient.getTopicPartitionCount(topic1).get)
    assertEquals(expectedAssignment, zkClient.getPartitionAssignmentForTopics(Set(topic1)).get(topic1).get)
    assertEquals(Set(0, 1, 2), zkClient.getPartitionsForTopics(Set(topic1)).get(topic1).get.toSet)
    assertEquals(Set(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition(topic1, 2)).toSet)

    val updatedAssignment = assignment - new TopicPartition(topic1, 2)

    zkClient.setTopicAssignment(topic1, updatedAssignment)
    assertEquals(updatedAssignment.size, zkClient.getTopicPartitionCount(topic1).get)

    // add second topic
    val secondAssignment = Map(
      new TopicPartition(topic2, 0) -> Seq(0, 1),
      new TopicPartition(topic2, 1) -> Seq(0, 1)
    )

    zkClient.createTopicAssignment(topic2, secondAssignment)

    assertEquals(Set(topic1, topic2), zkClient.getAllTopicsInCluster.toSet)
  }

  @Test
  def testGetDataAndVersion() {
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
  def testConditionalUpdatePath() {
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
    assertEquals("Failed for non existing parent ZK node", Seq.empty, zkClient.getAllIsrChangeNotifications)
    assertEquals("Failed for non existing parent ZK node", Seq.empty, zkClient.getPartitionsFromIsrChangeNotifications(Seq("0000000000")))

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
    intercept[ControllerMovedException](zkClient.deleteIsrChangeNotifications(Seq("0000000001"), controllerEpochZkVersion + 1))
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
    assertEquals("getAllLogDirEventNotifications failed for non existing parent ZK node",
      Seq.empty, zkClient.getAllLogDirEventNotifications)
    assertEquals("getBrokerIdsFromLogDirEvents failed for non existing parent ZK node",
      Seq.empty, zkClient.getBrokerIdsFromLogDirEvents(Seq("0000000000")))

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

    intercept[ControllerMovedException](zkClient.deleteLogDirEventNotifications(Seq("0000000000", "0000000002"), controllerEpochZkVersion + 1))
    assertEquals(Seq("0000000000", "0000000001", "0000000002"), zkClient.getAllLogDirEventNotifications)

    zkClient.deleteLogDirEventNotifications(Seq("0000000000", "0000000002"), controllerEpochZkVersion)

    assertEquals(Seq("0000000001"), zkClient.getAllLogDirEventNotifications)

    zkClient.propagateLogDirEvent(anotherBrokerId)

    zkClient.deleteLogDirEventNotifications(controllerEpochZkVersion)
    assertEquals(Seq.empty, zkClient.getAllLogDirEventNotifications)
  }

  @Test
  def testSetGetAndDeletePartitionReassignment() {
    zkClient.createRecursive(AdminZNode.path)

    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    val reassignment = Map(
      new TopicPartition("topic_a", 0) -> Seq(0, 1, 3),
      new TopicPartition("topic_a", 1) -> Seq(2, 1, 3),
      new TopicPartition("topic_b", 0) -> Seq(4, 5),
      new TopicPartition("topic_c", 0) -> Seq(5, 3)
    )

    // Should throw ControllerMovedException if the controller epoch zkVersion does not match
    intercept[ControllerMovedException](zkClient.setOrCreatePartitionReassignment(reassignment, controllerEpochZkVersion + 1))

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
  def testGetDataAndStat() {
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
  def testGetChildren() {
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
  def testAclManagementMethods() {
    ZkAclStore.stores.foreach(store => {
      assertFalse(zkClient.pathExists(store.aclPath))
      assertFalse(zkClient.pathExists(store.changeStore.aclChangePath))
      ResourceType.values.foreach(resource => assertFalse(zkClient.pathExists(store.path(resource))))
    })

    // create acl paths
    zkClient.createAclPaths

    ZkAclStore.stores.foreach(store => {
      assertTrue(zkClient.pathExists(store.aclPath))
      assertTrue(zkClient.pathExists(store.changeStore.aclChangePath))
      ResourceType.values.foreach(resource => assertTrue(zkClient.pathExists(store.path(resource))))

      val resource1 = new Resource(Topic, UUID.randomUUID().toString, store.patternType)
      val resource2 = new Resource(Topic, UUID.randomUUID().toString, store.patternType)

      // try getting acls for non-existing resource
      var versionedAcls = zkClient.getVersionedAclsForResource(resource1)
      assertTrue(versionedAcls.acls.isEmpty)
      assertEquals(ZkVersion.UnknownVersion, versionedAcls.zkVersion)
      assertFalse(zkClient.resourceExists(resource1))


      val acl1 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), Deny, "host1" , Read)
      val acl2 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Allow, "*", Read)
      val acl3 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Deny, "host1", Read)

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
      assertTrue(ResourceType.values.map( rt => rt.name).toSet == zkClient.getResourceTypes(store.patternType).toSet)

      //get resource name
      val resourceNames = zkClient.getResourceNames(store.patternType, Topic)
      assertEquals(2, resourceNames.size)
      assertTrue(Set(resource1.name,resource2.name) == resourceNames.toSet)

      //delete resource
      assertTrue(zkClient.deleteResource(resource1))
      assertFalse(zkClient.resourceExists(resource1))

      //delete with invalid expected zk version
      assertFalse(zkClient.conditionalDelete(resource2, 10))
      //delete with valid expected zk version
      assertTrue(zkClient.conditionalDelete(resource2, 0))

      zkClient.createAclChangeNotification(Resource(Group, "resource1", store.patternType))
      zkClient.createAclChangeNotification(Resource(Topic, "resource2", store.patternType))

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
  def testDeleteTopicPathMethods() {
    assertFalse(zkClient.isTopicMarkedForDeletion(topic1))
    assertTrue(zkClient.getTopicDeletions.isEmpty)

    zkClient.createDeleteTopicPath(topic1)
    zkClient.createDeleteTopicPath(topic2)

    assertTrue(zkClient.isTopicMarkedForDeletion(topic1))
    assertEquals(Set(topic1, topic2), zkClient.getTopicDeletions.toSet)

    intercept[ControllerMovedException](zkClient.deleteTopicDeletions(Seq(topic1, topic2), controllerEpochZkVersion + 1))
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
    intercept[NoNodeException] {
      zkClient.createTokenChangeNotification("delegationToken")
    }
    zkClient.createDelegationTokenPaths()

    zkClient.createTokenChangeNotification("delegationToken")
    assertPathExistenceAndData("/delegation_token/token_changes/token_change_0000000000", "delegationToken")
  }

  @Test
  def testEntityConfigManagementMethods() {
    assertTrue(zkClient.getEntityConfigs(ConfigType.Topic, topic1).isEmpty)

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.Topic, topic1))

    logProps.remove(LogConfig.CleanupPolicyProp)
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.Topic, topic1))

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic2, logProps)
    assertEquals(Set(topic1, topic2), zkClient.getAllEntitiesWithConfig(ConfigType.Topic).toSet)

    zkClient.deleteTopicConfigs(Seq(topic1, topic2), controllerEpochZkVersion)
    assertTrue(zkClient.getEntityConfigs(ConfigType.Topic, topic1).isEmpty)
  }

  @Test
  def testCreateConfigChangeNotification(): Unit = {
    assertFalse(zkClient.pathExists(ConfigEntityChangeNotificationZNode.path))

    // The parent path is created if needed
    zkClient.createConfigChangeNotification(ConfigEntityZNode.path(ConfigType.Topic, topic1))
    assertPathExistenceAndData(
      "/config/changes/config_change_0000000000",
      """{"version":2,"entity_path":"/config/topics/topic1"}""")

    // Creation does not fail if the parent path exists
    zkClient.createConfigChangeNotification(ConfigEntityZNode.path(ConfigType.Topic, topic2))
    assertPathExistenceAndData(
      "/config/changes/config_change_0000000001",
      """{"version":2,"entity_path":"/config/topics/topic2"}""")
  }

  private def createLogProps(bytesProp: Int): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, bytesProp.toString)
    logProps.put(LogConfig.SegmentIndexBytesProp, bytesProp.toString)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    logProps
  }

  private val logProps = createLogProps(1024)

  @Test
  def testGetLogConfigs(): Unit = {
    val emptyConfig = LogConfig(Collections.emptyMap())
    assertEquals("Non existent config, no defaults",
      (Map(topic1 -> emptyConfig), Map.empty),
      zkClient.getLogConfigs(Seq(topic1), Collections.emptyMap()))

    val logProps2 = createLogProps(2048)

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic1, logProps)
    assertEquals("One existing and one non-existent topic",
      (Map(topic1 -> LogConfig(logProps), topic2 -> emptyConfig), Map.empty),
      zkClient.getLogConfigs(Seq(topic1, topic2), Collections.emptyMap()))

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic2, logProps2)
    assertEquals("Two existing topics",
      (Map(topic1 -> LogConfig(logProps), topic2 -> LogConfig(logProps2)), Map.empty),
      zkClient.getLogConfigs(Seq(topic1, topic2), Collections.emptyMap()))

    val logProps1WithMoreValues = createLogProps(1024)
    logProps1WithMoreValues.put(LogConfig.SegmentJitterMsProp, "100")
    logProps1WithMoreValues.put(LogConfig.SegmentBytesProp, "1024")

    assertEquals("Config with defaults",
      (Map(topic1 -> LogConfig(logProps1WithMoreValues)), Map.empty),
      zkClient.getLogConfigs(Seq(topic1),
        Map[String, AnyRef](LogConfig.SegmentJitterMsProp -> "100", LogConfig.SegmentBytesProp -> "128").asJava))
  }

  private def createBrokerInfo(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol,
                               rack: Option[String] = None): BrokerInfo =
    BrokerInfo(Broker(id, Seq(new EndPoint(host, port, ListenerName.forSecurityProtocol
    (securityProtocol), securityProtocol)), rack = rack), ApiVersion.latestVersion, jmxPort = port + 10)

  @Test
  def testRegisterBrokerInfo(): Unit = {
    zkClient.createTopLevelPaths()

    val brokerInfo = createBrokerInfo(1, "test.host", 9999, SecurityProtocol.PLAINTEXT)
    val differentBrokerInfoWithSameId = createBrokerInfo(1, "test.host2", 9995, SecurityProtocol.SSL)

    zkClient.registerBroker(brokerInfo)
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))
    assertEquals("Other ZK clients can read broker info", Some(brokerInfo.broker), otherZkClient.getBroker(1))

    // Node exists, owned by current session - no error, no update
    zkClient.registerBroker(differentBrokerInfoWithSameId)
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))

    // Other client tries to register broker with same id causes failure, info is not changed in ZK
    intercept[NodeExistsException] {
      otherZkClient.registerBroker(differentBrokerInfoWithSameId)
    }
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

    assertNotEquals("The Czxid of original ephemeral znode should be different " +
      "from the new ephemeral znode Czxid", originalCzxid, newCzxid)
  }

  @Test
  def testGetBrokerMethods(): Unit = {
    zkClient.createTopLevelPaths()

    assertEquals(Seq.empty,zkClient.getAllBrokersInCluster)
    assertEquals(Seq.empty, zkClient.getSortedBrokerList)
    assertEquals(None, zkClient.getBroker(0))

    val brokerInfo0 = createBrokerInfo(0, "test.host0", 9998, SecurityProtocol.PLAINTEXT)
    val brokerInfo1 = createBrokerInfo(1, "test.host1", 9999, SecurityProtocol.SSL)

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
    intercept[NoNodeException]{
      zkClient.updateBrokerInfo(originalBrokerInfo)
    }

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

  private def leaderIsrAndControllerEpochs(state: Int, zkVersion: Int): Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    Map(
      topicPartition10 -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(leader = 1, leaderEpoch = state, isr = List(2 + state, 3 + state), zkVersion = zkVersion),
        controllerEpoch = 4),
      topicPartition11 -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(leader = 0, leaderEpoch = state + 1, isr = List(1 + state, 2 + state), zkVersion = zkVersion),
        controllerEpoch = 4))

  val initialLeaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    leaderIsrAndControllerEpochs(0, 0)

  val initialLeaderIsrs: Map[TopicPartition, LeaderAndIsr] = initialLeaderIsrAndControllerEpochs.mapValues(_.leaderAndIsr)
  private def leaderIsrs(state: Int, zkVersion: Int): Map[TopicPartition, LeaderAndIsr] =
    leaderIsrAndControllerEpochs(state, zkVersion).mapValues(_.leaderAndIsr)

  private def checkUpdateLeaderAndIsrResult(
                  expectedSuccessfulPartitions: Map[TopicPartition, LeaderAndIsr],
                  expectedPartitionsToRetry: Seq[TopicPartition],
                  expectedFailedPartitions: Map[TopicPartition, (Class[_], String)],
                  actualUpdateLeaderAndIsrResult: UpdateLeaderAndIsrResult): Unit = {
    val failedPartitionsExcerpt =
      actualUpdateLeaderAndIsrResult.failedPartitions.mapValues(e => (e.getClass, e.getMessage))
    assertEquals("Permanently failed updates do not match expected",
      expectedFailedPartitions, failedPartitionsExcerpt)
    assertEquals("Retriable updates (due to BADVERSION) do not match expected",
      expectedPartitionsToRetry, actualUpdateLeaderAndIsrResult.partitionsToRetry)
    assertEquals("Successful updates do not match expected",
      expectedSuccessfulPartitions, actualUpdateLeaderAndIsrResult.successfulPartitions)
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
    intercept[ControllerMovedException](zkClient.updateLeaderAndIsr(initialLeaderIsrs, controllerEpoch = 4, controllerEpochZkVersion + 1))

    // successful updates
    checkUpdateLeaderAndIsrResult(
      leaderIsrs(state = 1, zkVersion = 1),
      mutable.ArrayBuffer.empty,
      Map.empty,
      zkClient.updateLeaderAndIsr(leaderIsrs(state = 1, zkVersion = 0),controllerEpoch = 4, controllerEpochZkVersion))

    // Try to update with wrong ZK version
    checkUpdateLeaderAndIsrResult(
      Map.empty,
      ArrayBuffer(topicPartition10, topicPartition11),
      Map.empty,
      zkClient.updateLeaderAndIsr(leaderIsrs(state = 1, zkVersion = 0),controllerEpoch = 4, controllerEpochZkVersion))

    // Trigger successful, to be retried and failed partitions in same call
    val mixedState = Map(
      topicPartition10 -> LeaderAndIsr(leader = 1, leaderEpoch = 2, isr = List(4, 5), zkVersion = 1),
      topicPartition11 -> LeaderAndIsr(leader = 0, leaderEpoch = 2, isr = List(3, 4), zkVersion = 0),
      topicPartition20 -> LeaderAndIsr(leader = 0, leaderEpoch = 2, isr = List(3, 4), zkVersion = 0))

    checkUpdateLeaderAndIsrResult(
      leaderIsrs(state = 2, zkVersion = 2).filterKeys{_ == topicPartition10},
      ArrayBuffer(topicPartition11),
      Map(
        topicPartition20 -> (classOf[NoNodeException], "KeeperErrorCode = NoNode for /brokers/topics/topic2/partitions/0/state")),
      zkClient.updateLeaderAndIsr(mixedState, controllerEpoch = 4, controllerEpochZkVersion))
  }

  private def checkGetDataResponse(
      leaderIsrAndControllerEpochs: Map[TopicPartition,LeaderIsrAndControllerEpoch],
      topicPartition: TopicPartition,
      response: GetDataResponse): Unit = {
    val zkVersion = leaderIsrAndControllerEpochs(topicPartition).leaderAndIsr.zkVersion
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
    assertTrue(zkClient.getAllTopicsInCluster.isEmpty)
    assertTrue(zkClient.getAllPartitions.isEmpty)

    zkClient.createRecursive(TopicZNode.path(topic1))
    zkClient.createRecursive(TopicZNode.path(topic2))
    assertEquals(Set(topic1, topic2), zkClient.getAllTopicsInCluster.toSet)

    assertTrue(zkClient.getAllPartitions.isEmpty)

    zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion)
    assertEquals(Set(topicPartition10, topicPartition11), zkClient.getAllPartitions)
  }

  @Test
  def testCreateAndGetTopicPartitionStatesRaw(): Unit = {
    zkClient.createRecursive(TopicZNode.path(topic1))

    // Mismatch controller epoch zkVersion
    intercept[ControllerMovedException](zkClient.createTopicPartitionStatesRaw(initialLeaderIsrAndControllerEpochs, controllerEpochZkVersion + 1))

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
      zkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 1, zkVersion = 0), controllerEpochZkVersion).map {
        eraseMetadataAndStat}.toList)

    // Mismatch controller epoch zkVersion
    intercept[ControllerMovedException](zkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 1, zkVersion = 0), controllerEpochZkVersion + 1))

    val getResponses = zkClient.getTopicPartitionStatesRaw(topicPartitions10_11)
    assertEquals(2, getResponses.size)
    topicPartitions10_11.zip(getResponses) foreach {case (tp, r) => checkGetDataResponse(leaderIsrAndControllerEpochs(state = 1, zkVersion = 0), tp, r)}

    // Other ZK client can also write the state of a partition
    assertEquals(
      expectedSetDataResponses(topicPartition10, topicPartition11)(Code.OK, statWithVersion(2)),
      otherZkClient.setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs(state = 2, zkVersion = 1), controllerEpochZkVersion).map {
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

    assertEquals("Setting non existing nodes should return NONODE results",
      SetDataResponse(Code.NONODE, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)))

    assertEquals("Creating non existing nodes is OK",
      CreateResponse(Code.OK, ControllerEpochZNode.path, None, ControllerEpochZNode.path, ResponseMetadata(0, 0)),
      eraseMetadata(zkClient.createControllerEpochRaw(0)))
    assertEquals(0, zkClient.getControllerEpoch.get._1)

    assertEquals("Attemt to create existing nodes should return NODEEXISTS",
      CreateResponse(Code.NODEEXISTS, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadata(zkClient.createControllerEpochRaw(0)))

    assertEquals("Updating existing nodes is OK",
      SetDataResponse(Code.OK, ControllerEpochZNode.path, None, statWithVersion(1), ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)))
    assertEquals(1, zkClient.getControllerEpoch.get._1)

    assertEquals("Updating with wrong ZK version returns BADVERSION",
      SetDataResponse(Code.BADVERSION, ControllerEpochZNode.path, None, null, ResponseMetadata(0, 0)),
      eraseMetadataAndStat(zkClient.setControllerEpochRaw(1, 0)))
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
    assertTrue("Failed to receive create notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testClusterIdMethods() {
    val clusterId = CoreUtils.generateUuidAsBase64

    zkClient.createOrGetClusterId(clusterId)
    assertEquals(clusterId, zkClient.getClusterId.getOrElse(fail("No cluster id found")))
   }

  @Test
  def testBrokerSequenceIdMethods() {
    val sequenceId = zkClient.generateBrokerSequenceId()
    assertEquals(sequenceId + 1, zkClient.generateBrokerSequenceId)
  }

  @Test
  def testCreateTopLevelPaths() {
    zkClient.createTopLevelPaths()

    ZkData.PersistentZkPaths.foreach(path => assertTrue(zkClient.pathExists(path)))
  }

  @Test
  def testPreferredReplicaElectionMethods() {

    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)

    val electionPartitions = Set(new TopicPartition(topic1, 0), new TopicPartition(topic1, 1))

    zkClient.createPreferredReplicaElection(electionPartitions)
    assertEquals(electionPartitions, zkClient.getPreferredReplicaElection)

    intercept[NodeExistsException] {
      zkClient.createPreferredReplicaElection(electionPartitions)
    }

    // Mismatch controller epoch zkVersion
    intercept[ControllerMovedException](zkClient.deletePreferredReplicaElection(controllerEpochZkVersion + 1))
    assertEquals(electionPartitions, zkClient.getPreferredReplicaElection)

    zkClient.deletePreferredReplicaElection(controllerEpochZkVersion)
    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)
  }

  private def dataAsString(path: String): Option[String] = {
    val (data, _) = zkClient.getDataAndStat(path)
    data.map(new String(_, UTF_8))
  }

  @Test
  def testDelegationTokenMethods() {
    assertFalse(zkClient.pathExists(DelegationTokensZNode.path))
    assertFalse(zkClient.pathExists(DelegationTokenChangeNotificationZNode.path))

    zkClient.createDelegationTokenPaths
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

    intercept[NoNodeException] {
      zkClient.getAcl(mockPath)
    }

    intercept[NoNodeException] {
      zkClient.setAcl(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)
    }

    zkClient.createRecursive(mockPath)

    zkClient.setAcl(mockPath, ZooDefs.Ids.READ_ACL_UNSAFE.asScala)

    assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE.asScala, zkClient.getAcl(mockPath))
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
        time, metricGroup, metricType)
      new ExpiredKafkaZkClient(zooKeeperClient, isSecure, time)
    }
  }
}
