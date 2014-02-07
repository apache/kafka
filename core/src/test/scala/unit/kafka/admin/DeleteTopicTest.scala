package kafka.admin

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import junit.framework.Assert._
import kafka.utils.{ZkUtils, TestUtils}
import kafka.server.{KafkaServer, KafkaConfig}
import org.junit.Test
import kafka.common._
import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.producer.KeyedMessage
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo

class DeleteTopicTest extends JUnit3Suite with ZooKeeperTestHarness {

  @Test
  def testDeleteTopicWithAllAliveReplicas() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicWithRecoveredFollower() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // check if all replicas but the one that is shut down has deleted the log
    assertTrue("Replicas 0,1 have not deleted log in 1000ms", TestUtils.waitUntilTrue(() =>
      servers.filter(s => s.config.brokerId != follower.config.brokerId)
        .foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty), 1000))
    // ensure topic deletion is halted
    assertTrue("Admin path /admin/delete_topic/test path deleted in 1000ms even when a follower replica is down",
      TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)), 500))
    // restart follower replica
    follower.startup()
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicOnControllerFailover() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // shut down the controller to trigger controller failover during delete topic
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    controller.shutdown()
    // ensure topic deletion is halted
    assertTrue("Admin path /admin/delete_topic/test path deleted in 500ms even when a replica is down",
      TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)), 500))
    // restart follower replica
    controller.startup()
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    assertTrue("Admin path /admin/delete_topic/test path not deleted in 4000ms even after a follower replica is restarted",
      TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)), 4000))
    assertTrue("Topic path /brokers/topics/test not deleted after /admin/delete_topic/test path is deleted",
      TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)), 100))
    // ensure that logs from all replicas are deleted if delete topic is marked successful in zookeeper
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty))
    servers.foreach(_.shutdown())
  }

  @Test
  def testRequestHandlingDuringDeleteTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // shut down one follower replica
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // test if produce requests are failed with UnknownTopicOrPartitionException during delete topic
    val props1 = new Properties()
    props1.put("metadata.broker.list", servers.map(s => s.config.hostName + ":" + s.config.port).mkString(","))
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("request.required.acks", "1")
    val producerConfig1 = new ProducerConfig(props1)
    val producer1 = new Producer[String, String](producerConfig1)
    try{
      producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Test should fail because the topic is being deleted")
    } catch {
      case e: FailedToSendMessageException =>
      case oe: Throwable => fail("fails with exception", oe)
    } finally {
      producer1.close()
    }
    // test if fetch requests fail during delete topic
    servers.filter(s => s.config.brokerId != follower.config.brokerId).foreach { server =>
      val consumer = new SimpleConsumer(server.config.hostName, server.config.port, 1000000, 64*1024, "")
      val request = new FetchRequestBuilder()
        .clientId("test-client")
        .addFetch(topic, 0, 0, 10000)
        .build()
      val fetched = consumer.fetch(request)
      val fetchResponse = fetched.data(topicAndPartition)
      assertTrue("Fetch should fail with UnknownTopicOrPartitionCode", fetchResponse.error == ErrorMapping.UnknownTopicOrPartitionCode)
    }
    // test if offset requests fail during delete topic
    servers.filter(s => s.config.brokerId != follower.config.brokerId).foreach { server =>
      val consumer = new SimpleConsumer(server.config.hostName, server.config.port, 1000000, 64*1024, "")
      val offsetRequest = new OffsetRequest(Map(topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
      val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
      val errorCode = offsetResponse.partitionErrorAndOffsets(topicAndPartition).error
      assertTrue("Offset request should fail with UnknownTopicOrPartitionCode", errorCode == ErrorMapping.UnknownTopicOrPartitionCode)
      // test if offset fetch requests fail during delete topic
      val offsetFetchRequest = new OffsetFetchRequest("test-group", Seq(topicAndPartition))
      val offsetFetchResponse = consumer.fetchOffsets(offsetFetchRequest)
      val offsetFetchErrorCode = offsetFetchResponse.requestInfo(topicAndPartition).error
      assertTrue("Offset fetch request should fail with UnknownTopicOrPartitionCode",
        offsetFetchErrorCode == ErrorMapping.UnknownTopicOrPartitionCode)
      // TODO: test if offset commit requests fail during delete topic
    }
    // restart follower replica
    follower.startup()
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPreferredReplicaElectionDuringDeleteTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    // shut down the controller to move the leader to a non preferred replica before delete topic
    val preferredReplicaId = 0
    val preferredReplica = servers.filter(s => s.config.brokerId == preferredReplicaId).head
    preferredReplica.shutdown()
    preferredReplica.startup()
    val newLeaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 3000, leaderIdOpt)
    assertTrue("New leader should be elected prior to delete topic", newLeaderIdOpt.isDefined)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // test preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(topicAndPartition))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val leaderAfterPreferredReplicaElectionOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000, newLeaderIdOpt)
    assertTrue("Preferred replica election should not move leader during delete topic",
      leaderAfterPreferredReplicaElectionOpt.isEmpty || leaderAfterPreferredReplicaElectionOpt.get == newLeaderIdOpt.get)
    val newControllerId = ZkUtils.getController(zkClient)
    val newController = servers.filter(s => s.config.brokerId == newControllerId).head
    assertFalse("Preferred replica election should fail",
      newController.kafkaController.controllerContext.partitionsUndergoingPreferredReplicaElection.contains(topicAndPartition))
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringPreferredReplicaElection() {
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    // shut down the controller to move the leader to a non preferred replica before delete topic
    val preferredReplicaId = 0
    val preferredReplica = servers.filter(s => s.config.brokerId == preferredReplicaId).head
    preferredReplica.shutdown()
    preferredReplica.startup()
    val newLeaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 3000, leaderIdOpt)
    assertTrue("New leader should be elected prior to delete topic", newLeaderIdOpt.isDefined)
    // test preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(topicAndPartition))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    // start topic deletion during preferred replica election. This should halt topic deletion but eventually
    // complete it successfully
    AdminUtils.deleteTopic(zkClient, topic)
    val newControllerId = ZkUtils.getController(zkClient)
    val newController = servers.filter(s => s.config.brokerId == newControllerId).head
    assertTrue("Preferred replica election should succeed after 1000ms", TestUtils.waitUntilTrue(() =>
      !newController.kafkaController.controllerContext.partitionsUndergoingPreferredReplicaElection.contains(topicAndPartition), 1000))
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    // create brokers
    val allServers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    assertTrue("Replicas for topic test not created in 1000ms", TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined), 1000))
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // start partition reassignment at the same time right after delete topic. In this case, reassignment will fail since
    // the topic is being deleted
    // reassign partition 0
    val oldAssignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment should fail for [test,0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentFailed;
    }, 1000)
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should fail",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicAndPartition))
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    assertEquals("Partition should not be reassigned to 0, 1, 2", oldAssignedReplicas, assignedReplicas)
    verifyTopicDeletion(topic, servers)
    allServers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringPartitionReassignment() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    // create brokers
    val allServers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    assertTrue("Replicas for topic test not created in 1000ms", TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined), 1000))
    var leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    // start partition reassignment at the same time right before delete topic. In this case, reassignment will succeed
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
    }, 1000)
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should complete",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicAndPartition))
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    assertEquals("Partition should be reassigned to 1,2,3", newReplicas, assignedReplicas)
    verifyTopicDeletion(topic, allServers)
    allServers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringAddPartition() {
    val topic = "test"
    val servers = createTestTopicAndCluster(topic)
    // add partitions to topic
    val topicAndPartition = TopicAndPartition(topic, 0)
    val newPartition = TopicAndPartition(topic, 1)
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2")
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // test if topic deletion is resumed
   verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not for new partition [test,1] not deleted after delete topic is complete", TestUtils.waitUntilTrue(() =>
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty), 1000))
    servers.foreach(_.shutdown())
  }

  @Test
  def testAddPartitionDuringDeleteTopic() {
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // add partitions to topic
    val newPartition = TopicAndPartition(topic, 1)
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2")
    verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty))
    servers.foreach(_.shutdown())
  }

  @Test
  def testRecreateTopicAfterDeletion() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    // re-create topic on same replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until leader is elected
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("New leader should be elected after re-creating topic test", leaderIdOpt.isDefined)
    // check if all replica logs are created
    assertTrue("Replicas for topic test not created in 1000ms", TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined), 1000))
    servers.foreach(_.shutdown())
  }

  @Test
  def testTopicConfigChangesDuringDeleteTopic() {
    val topic = "test"
    val servers = createTestTopicAndCluster(topic)
    val topicConfigs = new Properties()
    topicConfigs.put("segment.ms", "1000000")
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    // make topic config changes
    try {
      AdminUtils.changeTopicConfig(zkClient, topic, topicConfigs)
      fail("Should fail with AdminOperationException for topic doesn't exist")
    } catch {
      case e: AdminOperationException => // expected
    }
    servers.foreach(_.shutdown())
  }

  private def createTestTopicAndCluster(topic: String): Seq[KafkaServer] = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topicAndPartition = TopicAndPartition(topic, 0)
    // create brokers
    val servers = TestUtils.createBrokerConfigs(3).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    assertTrue("Replicas for topic test not created in 1000ms", TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined), 1000))
    servers
  }

  private def verifyTopicDeletion(topic: String, servers: Seq[KafkaServer]) {
    val topicAndPartition = TopicAndPartition(topic, 0)
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    assertTrue("Admin path /admin/delete_topic/test path not deleted in 1000ms even after a replica is restarted",
      TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)), 1000))
    assertTrue("Topic path /brokers/topics/test not deleted after /admin/delete_topic/test path is deleted",
      TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)), 100))
    // ensure that logs from all replicas are deleted if delete topic is marked successful in zookeeper
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty))
  }
}