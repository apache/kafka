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
package kafka.zk.migration

import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.coordinator.transaction.{ProducerIdManager, ZkProducerIdManager}
import org.apache.kafka.common.config.{ConfigResource, SslConfigs, TopicConfig}
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.{ConfigRecord, MetadataRecordType, PartitionRecord, ProducerIdsRecord, TopicRecord}
import org.apache.kafka.common.{DirectoryId, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.migration.TopicMigrationClient.{TopicVisitor, TopicVisitorInterest}
import org.apache.kafka.metadata.migration.{KRaftMigrationZkWriter, ZkMigrationLeadershipState}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.config.ConfigType
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/**
 * ZooKeeper integration tests that verify the interoperability of KafkaZkClient and ZkMigrationClient.
 */
class ZkMigrationClientTest extends ZkMigrationTestHarness {

  @Test
  def testMigrateEmptyZk(): Unit = {
    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()

    migrationClient.readAllMetadata(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(0, brokers.size())
    assertEquals(0, batches.size())
  }

  @Test
  def testEmptyWrite(): Unit = {
    val (zkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(), migrationState)
    assertEquals(migrationState.migrationZkVersion(), zkVersion)
    assertTrue(responses.isEmpty)
  }

  @Test
  def testUpdateExistingPartitions(): Unit = {
    // Create a topic and partition state in ZK like KafkaController would
    val assignment = Map(
      new TopicPartition("test", 0) -> List(0, 1, 2),
      new TopicPartition("test", 1) -> List(1, 2, 3)
    )
    zkClient.createTopicAssignment("test", Some(Uuid.randomUuid()), assignment)

    val leaderAndIsrs = Map(
      new TopicPartition("test", 0) -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(0, 5, List(0, 1, 2), LeaderRecoveryState.RECOVERED, -1), 1),
      new TopicPartition("test", 1) -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(1, 5, List(1, 2, 3), LeaderRecoveryState.RECOVERED, -1), 1)
    )
    zkClient.createTopicPartitionStatesRaw(leaderAndIsrs, 0)

    // Now verify that we can update it with migration client
    assertEquals(0, migrationState.migrationZkVersion())

    val partitions = Map(
      0 -> new PartitionRegistration.Builder()
        .setReplicas(Array(0, 1, 2))
        .setDirectories(DirectoryId.migratingArray(3))
        .setIsr(Array(1, 2))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(6)
        .setPartitionEpoch(-1)
        .build(),
      1 -> new PartitionRegistration.Builder()
        .setReplicas(Array(1, 2, 3))
        .setDirectories(DirectoryId.migratingArray(3))
        .setIsr(Array(3))
        .setLeader(3)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(7)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.topicClient().updateTopicPartitions(Map("test" -> partitions).asJava, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Read back with Zk client
    val partition0 = zkClient.getTopicPartitionState(new TopicPartition("test", 0)).get.leaderAndIsr
    assertEquals(1, partition0.leader)
    assertEquals(6, partition0.leaderEpoch)
    assertEquals(List(1, 2), partition0.isr)

    val partition1 = zkClient.getTopicPartitionState(new TopicPartition("test", 1)).get.leaderAndIsr
    assertEquals(3, partition1.leader)
    assertEquals(7, partition1.leaderEpoch)
    assertEquals(List(3), partition1.isr)

    // Delete whole topic
    migrationState = migrationClient.topicClient().deleteTopic("test", migrationState)
    assertEquals(2, migrationState.migrationZkVersion())
  }

  @Test
  def testCreateNewTopic(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val partitions = Map(
      0 -> new PartitionRegistration.Builder()
        .setReplicas(Array(0, 1, 2))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(0, 1, 2))
        .setLeader(0)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build(),
      1 -> new PartitionRegistration.Builder()
        .setReplicas(Array(1, 2, 3))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(1, 2, 3))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.topicClient().createTopic("test", Uuid.randomUuid(), partitions, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Read back with Zk client
    val partition0 = zkClient.getTopicPartitionState(new TopicPartition("test", 0)).get.leaderAndIsr
    assertEquals(0, partition0.leader)
    assertEquals(0, partition0.leaderEpoch)
    assertEquals(List(0, 1, 2), partition0.isr)

    val partition1 = zkClient.getTopicPartitionState(new TopicPartition("test", 1)).get.leaderAndIsr
    assertEquals(1, partition1.leader)
    assertEquals(0, partition1.leaderEpoch)
    assertEquals(List(1, 2, 3), partition1.isr)
  }

  @Test
  def testIdempotentCreateTopics(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val partitions = Map(
      0 -> new PartitionRegistration.Builder()
        .setReplicas(Array(0, 1, 2))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(0, 1, 2))
        .setLeader(0)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build(),
      1 -> new PartitionRegistration.Builder()
        .setReplicas(Array(1, 2, 3))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(1, 2, 3))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    val topicId = Uuid.randomUuid()
    migrationState = migrationClient.topicClient().createTopic("test", topicId, partitions, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    migrationState = migrationClient.topicClient().createTopic("test", topicId, partitions, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())
  }

  @Test
  def testClaimAbsentController(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.zkControllerEpochZkVersion())
  }

  @Test
  def testExistingKRaftControllerClaim(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.zkControllerEpochZkVersion())

    // We don't require a KRaft controller to release the controller in ZK before another KRaft controller
    // can claim it. This is because KRaft leadership comes from Raft and we are just synchronizing it to ZK.
    var otherNodeState = ZkMigrationLeadershipState.EMPTY
      .withNewKRaftController(3001, 43)
      .withKRaftMetadataOffsetAndEpoch(100, 42)
    otherNodeState = migrationClient.claimControllerLeadership(otherNodeState)
    assertEquals(2, otherNodeState.zkControllerEpochZkVersion())
    assertEquals(3001, otherNodeState.kraftControllerId())
    assertEquals(43, otherNodeState.kraftControllerEpoch())
  }

  @Test
  def testNonIncreasingKRaftEpoch(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch)
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(1, migrationState.zkControllerEpochZkVersion())

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch - 1)
    val t1 = assertThrows(classOf[ControllerMovedException], () => migrationClient.claimControllerLeadership(migrationState))
    assertEquals("Cannot register KRaft controller 3001 with epoch 41 as the current controller register in ZK has the same or newer epoch 42.", t1.getMessage)

    migrationState = migrationState.withNewKRaftController(3001, InitialControllerEpoch)
    val t2 = assertThrows(classOf[ControllerMovedException], () => migrationClient.claimControllerLeadership(migrationState))
    assertEquals("Cannot register KRaft controller 3001 with epoch 42 as the current controller register in ZK has the same or newer epoch 42.", t2.getMessage)

    migrationState = migrationState.withNewKRaftController(3001, 100)
    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(migrationState.kraftControllerEpoch(), 100)
    assertEquals(migrationState.kraftControllerId(), 3001)
  }

  @Test
  def testClaimAndReleaseExistingController(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val (epoch, zkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(100)
    assertEquals(epoch, 2)
    assertEquals(zkVersion, 1)

    migrationState = migrationClient.claimControllerLeadership(migrationState)
    assertEquals(2, migrationState.zkControllerEpochZkVersion())
    zkClient.getControllerEpoch match {
      case Some((zkEpoch, stat)) =>
        assertEquals(3, zkEpoch)
        assertEquals(2, stat.getVersion)
      case None => fail()
    }
    assertEquals(3000, zkClient.getControllerId.get)
    assertThrows(classOf[ControllerMovedException], () => zkClient.registerControllerAndIncrementControllerEpoch(100))

    migrationState = migrationClient.releaseControllerLeadership(migrationState)
    val (epoch1, zkVersion1) = zkClient.registerControllerAndIncrementControllerEpoch(100)
    assertEquals(epoch1, 4)
    assertEquals(zkVersion1, 3)
  }

  @Test
  def testReadMigrateAndWriteProducerId(): Unit = {
    // allocate some producer id blocks
    ZkProducerIdManager.getNewProducerIdBlock(1, zkClient, this)
    ZkProducerIdManager.getNewProducerIdBlock(2, zkClient, this)
    val block = ZkProducerIdManager.getNewProducerIdBlock(3, zkClient, this)

    // Migrate the producer ID state to KRaft as a record
    val records = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()
    migrationClient.migrateProducerId(batch => records.add(batch))
    assertEquals(1, records.size())
    assertEquals(1, records.get(0).size())
    val record = records.get(0).get(0).message().asInstanceOf[ProducerIdsRecord]

    // Ensure the block stored in KRaft is the _next_ block since that is what will be served
    // to the next ALLOCATE_PRODUCER_IDS caller
    assertEquals(block.nextBlockFirstId(), record.nextProducerId())

    // Update next producer ID via migration client
    migrationState = migrationClient.writeProducerId(6000, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    val manager = ProducerIdManager.zk(1, zkClient)
    val producerId = manager.generateProducerId() match {
      case Failure(e) => fail("Encountered error when generating producer id", e)
      case Success(value) => value
    }
    assertEquals(7000, producerId)
  }

  @Test
  def testMigrateTopicConfigs(): Unit = {
    val props = new Properties()
    props.put(TopicConfig.FLUSH_MS_CONFIG, "60000")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "300000")
    adminZkClient.createTopicWithAssignment("test", props, Map(0 -> Seq(0, 1, 2), 1 -> Seq(1, 2, 0), 2 -> Seq(2, 0, 1)), usesTopicId = true)

    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()
    migrationClient.migrateTopics(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(1, batches.size())
    val configs = batches.get(0)
      .asScala
      .map {_.message() }
      .filter(message => MetadataRecordType.fromId(message.apiKey()).equals(MetadataRecordType.CONFIG_RECORD))
      .map { _.asInstanceOf[ConfigRecord] }
      .map { record => record.name() -> record.value()}
      .toMap
    assertEquals(2, configs.size)
    assertTrue(configs.contains(TopicConfig.FLUSH_MS_CONFIG))
    assertEquals("60000", configs(TopicConfig.FLUSH_MS_CONFIG))
    assertTrue(configs.contains(TopicConfig.RETENTION_MS_CONFIG))
    assertEquals("300000", configs(TopicConfig.RETENTION_MS_CONFIG))
  }

  @Test
  def testTopicAndBrokerConfigsMigrationWithSnapshots(): Unit = {
    val kraftWriter = new KRaftMigrationZkWriter(migrationClient, fail(_))

    // Add add some topics and broker configs and create new image.
    val topicName = "testTopic"
    val partition = 0
    val tp = new TopicPartition(topicName, partition)
    val leaderPartition = 1
    val leaderEpoch = 100
    val partitionEpoch = 10
    val brokerId = "1"
    val replicas = List(1, 2, 3).map(int2Integer).asJava
    val topicId = Uuid.randomUuid()
    val props = new Properties()
    props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1") // normal config
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SECRET) // sensitive config

    //    // Leave Zk in an incomplete state.
    //    zkClient.createTopicAssignment(topicName, Some(topicId), Map(tp -> Seq(1)))

    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new TopicRecord()
      .setTopicId(topicId)
      .setName(topicName)
    )
    delta.replay(new PartitionRecord()
      .setTopicId(topicId)
      .setIsr(replicas)
      .setLeader(leaderPartition)
      .setReplicas(replicas)
      .setAddingReplicas(List.empty.asJava)
      .setRemovingReplicas(List.empty.asJava)
      .setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(partitionEpoch)
      .setPartitionId(partition)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
    )
    // Use same props for the broker and topic.
    props.asScala.foreach { case (key, value) =>
      delta.replay(new ConfigRecord()
        .setName(key)
        .setValue(value)
        .setResourceName(topicName)
        .setResourceType(ConfigResource.Type.TOPIC.id())
      )
      delta.replay(new ConfigRecord()
        .setName(key)
        .setValue(value)
        .setResourceName(brokerId)
        .setResourceType(ConfigResource.Type.BROKER.id())
      )
    }
    val image = delta.apply(MetadataProvenance.EMPTY)

    // Handle migration using the generated snapshot.
    kraftWriter.handleSnapshot(image, (_, _, operation) => {
      migrationState = operation(migrationState)
    })

    // Verify topic state.
    val topicIdReplicaAssignment =
      zkClient.getReplicaAssignmentAndTopicIdForTopics(Set(topicName))
    assertEquals(1, topicIdReplicaAssignment.size)
    topicIdReplicaAssignment.foreach { assignment =>
      assertEquals(topicName, assignment.topic)
      assertEquals(Some(topicId), assignment.topicId)
      assertEquals(Map(tp -> ReplicaAssignment(replicas.asScala.map(Integer2int).toSeq)),
        assignment.assignment)
    }

    // Verify the topic partition states.
    val topicPartitionState = zkClient.getTopicPartitionState(tp)
    assertTrue(topicPartitionState.isDefined)
    topicPartitionState.foreach { state =>
      assertEquals(leaderPartition, state.leaderAndIsr.leader)
      assertEquals(leaderEpoch, state.leaderAndIsr.leaderEpoch)
      assertEquals(LeaderRecoveryState.RECOVERED, state.leaderAndIsr.leaderRecoveryState)
      assertEquals(replicas.asScala.map(Integer2int).toList, state.leaderAndIsr.isr)
    }

    // Verify the broker and topic configs (including sensitive configs).
    val brokerProps = zkClient.getEntityConfigs(ConfigType.BROKER, brokerId)
    val topicProps = zkClient.getEntityConfigs(ConfigType.TOPIC, topicName)
    assertEquals(2, brokerProps.size())

    brokerProps.asScala.foreach { case (key, value) =>
      if (key == SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) {
        assertEquals(SECRET, encoder.decode(value).value)
      } else {
        assertEquals(props.getProperty(key), value)
      }
    }

    topicProps.asScala.foreach { case (key, value) =>
      if (key == SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) {
        assertEquals(SECRET, encoder.decode(value).value)
      } else {
        assertEquals(props.getProperty(key), value)
      }
    }
  }

  @Test
  def testUpdateExistingTopicWithNewAndChangedPartitions(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())

    val topicId = Uuid.randomUuid()
    val partitions = Map(
      0 -> new PartitionRegistration.Builder()
        .setReplicas(Array(0, 1, 2))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(0, 1, 2))
        .setLeader(0)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build(),
      1 -> new PartitionRegistration.Builder()
        .setReplicas(Array(1, 2, 3))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(1, 2, 3))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.topicClient().createTopic("test", topicId, partitions, migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    // Change assignment in partitions and update the topic assignment. See the change is
    // reflected.
    val changedPartitions = Map(
      0 -> new PartitionRegistration.Builder()
        .setReplicas(Array(1, 2, 3))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(1, 2, 3))
        .setLeader(0)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build(),
      1 -> new PartitionRegistration.Builder()
        .setReplicas(Array(0, 1, 2))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(0, 1, 2))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => Integer.valueOf(k) -> v }.asJava
    migrationState = migrationClient.topicClient().updateTopic("test", topicId, changedPartitions, migrationState)
    assertEquals(2, migrationState.migrationZkVersion())

    // Read the changed partition with zkClient.
    val topicReplicaAssignmentFromZk = zkClient.getReplicaAssignmentAndTopicIdForTopics(Set("test"))
    assertEquals(1, topicReplicaAssignmentFromZk.size)
    assertEquals(Some(topicId), topicReplicaAssignmentFromZk.head.topicId)
    topicReplicaAssignmentFromZk.head.assignment.foreach { case (tp, assignment) =>
      tp.partition() match {
        case p if p <=1 =>
          assertEquals(changedPartitions.get(p).replicas.toSeq, assignment.replicas)
          assertEquals(changedPartitions.get(p).addingReplicas.toSeq, assignment.addingReplicas)
          assertEquals(changedPartitions.get(p).removingReplicas.toSeq, assignment.removingReplicas)
        case p => fail(s"Found unknown partition $p")
      }
    }

    // Add a new Partition.
    val newPartition = Map(
      2 -> new PartitionRegistration.Builder()
        .setReplicas(Array(2, 3, 4))
        .setDirectories(DirectoryId.unassignedArray(3))
        .setIsr(Array(2, 3, 4))
        .setLeader(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
        .setLeaderEpoch(0)
        .setPartitionEpoch(-1)
        .build()
    ).map { case (k, v) => int2Integer(k) -> v }.asJava
    migrationState = migrationClient.topicClient().createTopicPartitions(Map("test" -> newPartition).asJava, migrationState)
    assertEquals(3, migrationState.migrationZkVersion())

    // Read new partition from Zk.
    val newPartitionFromZk = zkClient.getTopicPartitionState(new TopicPartition("test", 2))
    assertTrue(newPartitionFromZk.isDefined)
    newPartitionFromZk.foreach { part =>
      val expectedPartition = newPartition.get(2)
      assertEquals(expectedPartition.leader, part.leaderAndIsr.leader)
      // Since KRaft increments partition epoch on change.
      assertEquals(expectedPartition.partitionEpoch + 1, part.leaderAndIsr.partitionEpoch)
      assertEquals(expectedPartition.leaderEpoch, part.leaderAndIsr.leaderEpoch)
      assertEquals(expectedPartition.leaderRecoveryState, part.leaderAndIsr.leaderRecoveryState)
      assertEquals(expectedPartition.isr.toList, part.leaderAndIsr.isr)
    }
  }

  /**
   * A partition can be present in a topic's replica assignment without having any partition state, for
   * instance when a controller fails in between writing the two. Verify that such partitions are only
   * visited when the caller asks for them, and that the other partitions and topics are visited either way.
   */
  @Test
  def testIterateTopicsWithMissingPartitionState(): Unit = {
    val topicNames = Seq("test-a", "test-b")
    topicNames.foreach { topicName =>
      val assignment = Map(
        new TopicPartition(topicName, 0) -> List(0, 1, 2),
        new TopicPartition(topicName, 1) -> List(1, 2, 3)
      )
      zkClient.createTopicAssignment(topicName, Some(Uuid.randomUuid()), assignment)
      // Only write the state of partition 0, leaving partition 1 without any state
      zkClient.createTopicPartitionStatesRaw(Map(
        new TopicPartition(topicName, 0) -> LeaderIsrAndControllerEpoch(
          LeaderAndIsr(0, 5, List(0, 1, 2), LeaderRecoveryState.RECOVERED, -1), 1)
      ), 0)
    }

    val skippingVisitor = new CapturingTopicVisitor()
    migrationClient.topicClient().iterateTopics(
      java.util.EnumSet.of(TopicVisitorInterest.TOPICS, TopicVisitorInterest.PARTITIONS),
      skippingVisitor)
    assertEquals(topicNames.toSet, skippingVisitor.visitedTopics.toSet)
    assertEquals(
      topicNames.map(new TopicPartition(_, 0)).toSet,
      skippingVisitor.visitedPartitions.keySet.toSet)

    val synthesizingVisitor = new CapturingTopicVisitor()
    migrationClient.topicClient().iterateTopics(
      java.util.EnumSet.of(
        TopicVisitorInterest.TOPICS,
        TopicVisitorInterest.PARTITIONS,
        TopicVisitorInterest.PARTITIONS_WITHOUT_STATE),
      synthesizingVisitor)
    assertEquals(topicNames.toSet, synthesizingVisitor.visitedTopics.toSet)
    assertEquals(
      topicNames.flatMap(topicName => Seq(0, 1).map(new TopicPartition(topicName, _))).toSet,
      synthesizingVisitor.visitedPartitions.keySet.toSet)
    topicNames.foreach { topicName =>
      val synthesized = synthesizingVisitor.visitedPartitions(new TopicPartition(topicName, 1))
      assertEquals(1, synthesized.leader)
      assertEquals(Seq(1, 2, 3), synthesized.isr.toSeq)
      assertEquals(0, synthesized.leaderEpoch)
      assertEquals(0, synthesized.partitionEpoch)
      assertEquals(LeaderRecoveryState.RECOVERED, synthesized.leaderRecoveryState)
    }
  }

  /**
   * Simulate a controller that failed after writing a new partition to the topic assignment in ZK, but
   * before writing that partition's state. The next controller must create the missing partition state
   * when it syncs the KRaft state to ZK (KAFKA-21142).
   */
  @Test
  def testSnapshotCreatesMissingPartitionState(): Unit = {
    val topicName = "test"
    val topicId = Uuid.randomUuid()
    val newPartition = new TopicPartition(topicName, 2)
    val assignment = Map(
      new TopicPartition(topicName, 0) -> List(0, 1, 2),
      new TopicPartition(topicName, 1) -> List(1, 2, 3),
      newPartition -> List(2, 3, 4)
    )
    zkClient.createTopicAssignment(topicName, Some(topicId), assignment)
    zkClient.createTopicPartitionStatesRaw(Map(
      new TopicPartition(topicName, 0) -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(0, 5, List(0, 1, 2), LeaderRecoveryState.RECOVERED, -1), 1),
      new TopicPartition(topicName, 1) -> LeaderIsrAndControllerEpoch(
        LeaderAndIsr(1, 5, List(1, 2, 3), LeaderRecoveryState.RECOVERED, -1), 1)
    ), 0)
    assertTrue(zkClient.getTopicPartitionState(newPartition).isEmpty)

    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName))
    assignment.foreach { case (topicPartition, replicas) =>
      delta.replay(new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(topicPartition.partition())
        .setReplicas(replicas.map(int2Integer).asJava)
        .setAddingReplicas(List.empty.asJava)
        .setRemovingReplicas(List.empty.asJava)
        .setIsr(replicas.map(int2Integer).asJava)
        .setLeader(replicas.head)
        .setLeaderEpoch(5)
        .setPartitionEpoch(10)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()))
    }

    val kraftWriter = new KRaftMigrationZkWriter(migrationClient, fail(_))
    kraftWriter.handleSnapshot(delta.apply(MetadataProvenance.EMPTY), (_, _, operation) => {
      migrationState = operation(migrationState)
    })

    val newPartitionState = zkClient.getTopicPartitionState(newPartition)
    assertTrue(newPartitionState.isDefined, s"Expected $newPartition to have been created in ZK")
    newPartitionState.foreach { state =>
      assertEquals(2, state.leaderAndIsr.leader)
      assertEquals(5, state.leaderAndIsr.leaderEpoch)
      assertEquals(List(2, 3, 4), state.leaderAndIsr.isr)
      assertEquals(LeaderRecoveryState.RECOVERED, state.leaderAndIsr.leaderRecoveryState)
    }
  }

  private class CapturingTopicVisitor extends TopicVisitor {
    val visitedTopics = new mutable.ArrayBuffer[String]()
    val visitedPartitions = new mutable.HashMap[TopicPartition, PartitionRegistration]()

    override def visitTopic(
      topicName: String,
      topicId: Uuid,
      assignments: java.util.Map[Integer, java.util.List[Integer]]
    ): Unit = {
      visitedTopics += topicName
    }

    override def visitPartition(
      topicIdPartition: TopicIdPartition,
      partitionRegistration: PartitionRegistration
    ): Unit = {
      visitedPartitions.put(topicIdPartition.topicPartition(), partitionRegistration)
    }
  }
}
