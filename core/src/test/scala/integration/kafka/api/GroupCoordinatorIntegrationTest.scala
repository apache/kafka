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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, Type}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, ConsumerGroupDescription}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, GroupProtocol, OffsetAndMetadata}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{GroupIdNotFoundException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.{ConsumerGroupState, GroupState, GroupType, KafkaFuture, TopicCollection, TopicPartition}
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.internal.CompressionType
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.{GroupProtocol => StreamsGroupProtocol}
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Timeout

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionException

@Timeout(120)
class GroupCoordinatorIntegrationTest(cluster: ClusterInstance) {

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, value = "1"),
      new ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "false"),
    )
  )
  def testGroupCoordinatorPropagatesOffsetsTopicCompressionCodec(): Unit = {
    withConsumer(groupId = "group", groupProtocol = GroupProtocol.CLASSIC) { consumer =>
      consumer.commitSync(java.util.Map.of(
        new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), new OffsetAndMetadata(10, "")
      ))

      val logManager = cluster.brokers().asScala.head._2.logManager
      def getGroupMetadataLogOpt: Option[UnifiedLog] =
        logManager.getLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))

      TestUtils.waitUntilTrue(() => getGroupMetadataLogOpt.exists(_.logSegments.asScala.exists(_.log.batches.asScala.nonEmpty)),
        "Commit message not appended in time")

      val logSegments = getGroupMetadataLogOpt.get.logSegments.asScala
      val incorrectCompressionCodecs = logSegments
        .flatMap(_.log.batches.asScala.map(_.compressionType))
        .filter(_ != CompressionType.GZIP)

      assertEquals(Seq.empty, incorrectCompressionCodecs, "Incorrect compression codecs should be empty")
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithConsumerGroupMemberJoiningAndLeaving(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a consumer group grp1 with one member. The member subscribes to foo and leaves. This creates
      // a mix of group records with tombstones to delete the member.
      withConsumer(groupId = "grp1", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment.asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }

    // Force a compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. If replaying any of the records fails, the
    // group coordinator won't be available.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp1"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp1", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverCompactingPartitionWithManualOffsetCommitsAndConsumerGroupMemberUnsubscribingAndResubscribing(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a consumer group grp2 with one member. The member subscribes to foo, manually commits offsets,
      // unsubscribes and finally re-subscribes to foo. This creates a mix of group records with tombstones
      // and ensure that all the offset commit records are before the consumer group records due to the
      // rebalance after the commit sync.
      withConsumer(groupId = "grp2", groupProtocol = GroupProtocol.CONSUMER, enableAutoCommit = false) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
        consumer.commitSync()
        consumer.unsubscribe()
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }

    // Force a compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. If replaying any of the records fails, the
    // group coordinator won't be available.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp2"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp2", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithConsumerGroupDeleted(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a consumer group grp3 with one member. The member subscribes to foo and leaves the group. Then
      // the group is deleted. This creates tombstones to delete the member, the group and the offsets.
      withConsumer(groupId = "grp3", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      admin
        .deleteConsumerGroups(java.util.List.of("grp3"))
        .deletedGroups()
        .get("grp3")
        .get(10, TimeUnit.SECONDS)
    }

    // Force a compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. If replaying any of the records fails, the
    // group coordinator won't be available.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp3"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedDeadGroup(groups, "grp3")
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedConsumerGroup(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a classic group grp4 with one member. Upgrades the group to the consumer
      // protocol.
      withConsumer(groupId = "grp4", groupProtocol = GroupProtocol.CLASSIC) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      withConsumer(groupId = "grp4", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }

    // Force a compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. If replaying any of the records fails, the
    // group coordinator won't be available.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp4"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp4", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedConsumerGroupAndTombstoneRemoved(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a classic group with one member and commit offsets.
      withConsumer(groupId = "grp4", groupProtocol = GroupProtocol.CLASSIC, enableAutoCommit = false) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
        consumer.commitSync()
      }

      // Set delete.retention.ms=0 before the tombstone is written so that
      // compaction will remove it.
      configureDeleteRetention()

      // Upgrade the group to the consumer protocol. Don't commit offsets
      // so the classic group's offset commits survive compaction.
      withConsumer(groupId = "grp4", groupProtocol = GroupProtocol.CONSUMER, enableAutoCommit = false) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }

    // Force compaction twice to remove tombstones: the first pass sets
    // deleteHorizonMs, and the second pass removes them.
    rollAndCompactConsumerOffsets()
    writeOneOffsetCommit()
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. Without the fix for KAFKA-20254, the offset
    // commit records create a simple classic group during replay and the
    // consumer group records fail to load.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp4"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp4", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedSimpleConsumerGroup(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a simple classic group by committing offsets directly
      // without subscribing. This only writes offset commit records
      // without any GroupMetadata records.
      withConsumer(groupId = "grp6", groupProtocol = GroupProtocol.CLASSIC, enableAutoCommit = false) { consumer =>
        val tp = new TopicPartition("foo", 0)
        consumer.assign(java.util.List.of(tp))
        consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(0)))
      }

      // Upgrade the group to the consumer protocol. Don't commit offsets
      // so the simple classic group's offset commits survive compaction.
      withConsumer(groupId = "grp6", groupProtocol = GroupProtocol.CONSUMER, enableAutoCommit = false) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }

    // Force a compaction. Since a simple classic group has no
    // GroupMetadata records, there are no tombstones — the offset
    // commit records always survive compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. Without the fix for KAFKA-20254, the offset
    // commit records create a simple classic group during replay and the
    // consumer group records fail to load.
    withAdmin { admin =>
      val groups = admin
        .describeConsumerGroups(java.util.List.of("grp6"))
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp6", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedStreamsGroup(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a classic group with one member.
      withConsumer(groupId = "grp5", groupProtocol = GroupProtocol.CLASSIC) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      // Upgrade the group to the streams protocol.
      withStreamsApp(applicationId = "grp5", inputTopic = "foo")
    }

    // Force a compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. If replaying any of the records fails, the
    // group coordinator won't be available.
    withAdmin { admin =>
      val groups = admin
        .describeStreamsGroups(java.util.List.of("grp5"))
        .describedGroups()
        .asScala
        .toMap

      val group = groups("grp5").get(10, TimeUnit.SECONDS)
      assertEquals("grp5", group.groupId)
      assertEquals(GroupState.EMPTY, group.groupState)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedStreamsGroupAndTombstoneRemoved(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a classic group with one member and commit offsets.
      withConsumer(groupId = "grp5", groupProtocol = GroupProtocol.CLASSIC, enableAutoCommit = false) { consumer =>
        consumer.subscribe(java.util.List.of("foo"))
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
        consumer.commitSync()
      }

      // Set delete.retention.ms=0 before the tombstone is written so that
      // compaction will remove it.
      configureDeleteRetention()

      // Upgrade the group to the streams protocol.
      withStreamsApp(applicationId = "grp5", inputTopic = "foo")
    }

    // Force compaction twice to remove tombstones: the first pass sets
    // deleteHorizonMs, and the second pass removes them.
    rollAndCompactConsumerOffsets()
    writeOneOffsetCommit()
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. Without the fix for KAFKA-20254, the offset
    // commit records create a simple classic group during replay and the
    // streams group records fail to load.
    withAdmin { admin =>
      val groups = admin
        .describeStreamsGroups(java.util.List.of("grp5"))
        .describedGroups()
        .asScala
        .toMap

      val group = groups("grp5").get(10, TimeUnit.SECONDS)
      assertEquals("grp5", group.groupId)
      assertEquals(GroupState.EMPTY, group.groupState)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testCoordinatorFailoverAfterCompactingPartitionWithUpgradedSimpleStreamsGroup(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      // Create a simple classic group by committing offsets directly
      // without subscribing. This only writes offset commit records
      // without any GroupMetadata records.
      withConsumer(groupId = "grp7", groupProtocol = GroupProtocol.CLASSIC, enableAutoCommit = false) { consumer =>
        val tp = new TopicPartition("foo", 0)
        consumer.assign(java.util.List.of(tp))
        consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(0)))
      }

      // Upgrade the group to the streams protocol.
      withStreamsApp(applicationId = "grp7", inputTopic = "foo")
    }

    // Force a compaction. Since a simple classic group has no
    // GroupMetadata records, there are no tombstones — the offset
    // commit records always survive compaction.
    rollAndCompactConsumerOffsets()

    // Restart the broker to reload the group coordinator.
    cluster.shutdownBroker(0)
    cluster.startBroker(0)

    // Verify the state of the groups to ensure that the group coordinator
    // was correctly loaded. Without the fix for KAFKA-20254, the offset
    // commit records create a simple classic group during replay and the
    // streams group records fail to load.
    withAdmin { admin =>
      val groups = admin
        .describeStreamsGroups(java.util.List.of("grp7"))
        .describedGroups()
        .asScala
        .toMap

      val group = groups("grp7").get(10, TimeUnit.SECONDS)
      assertEquals("grp7", group.groupId)
      assertEquals(GroupState.EMPTY, group.groupState)
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    )
  )
  def testRecreatingConsumerOffsetsTopic(): Unit = {
    withAdmin { admin =>
      TestUtils.createTopicWithAdminRaw(
        admin = admin,
        topic = "foo",
        numPartitions = 3
      )

      withConsumer(groupId = "group", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(List("foo").asJava)
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      admin
        .deleteTopics(TopicCollection.ofTopicNames(List(Topic.GROUP_METADATA_TOPIC_NAME).asJava))
        .all()
        .get()

      TestUtils.waitUntilTrue(() => {
        try {
          admin
            .describeTopics(TopicCollection.ofTopicNames(List(Topic.GROUP_METADATA_TOPIC_NAME).asJava))
            .topicNameValues()
            .get(Topic.GROUP_METADATA_TOPIC_NAME)
            .get(JTestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS)
          false
        } catch {
          case e: ExecutionException =>
            e.getCause.isInstanceOf[UnknownTopicOrPartitionException]
        }
      }, msg = s"${Topic.GROUP_METADATA_TOPIC_NAME} was not deleted")

      withConsumer(groupId = "group", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(List("foo").asJava)
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }
    }
  }

  private def writeOneOffsetCommit(): Unit = {
    // Write a single offset commit to create dirty data past the cleaner
    // checkpoint so the cleaner will re-process previously compacted
    // segments on the next compaction pass.
    withConsumer(groupId = "compaction-trigger", groupProtocol = GroupProtocol.CLASSIC, enableAutoCommit = false) { consumer =>
      val tp = new TopicPartition("foo", 0)
      consumer.assign(java.util.List.of(tp))
      consumer.commitSync(java.util.Map.of(tp, new OffsetAndMetadata(0)))
    }
  }

  private def configureDeleteRetention(): Unit = {
    withAdmin { admin =>
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME)
      admin.incrementalAlterConfigs(java.util.Map.of(resource, java.util.List.of(
        new AlterConfigOp(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "0"), AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.0"), AlterConfigOp.OpType.SET)
      ))).all().get()
    }
  }

  private def rollAndCompactConsumerOffsets(): Unit = {
    val tp = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val broker = cluster.brokers.asScala.head._2
    val log = broker.logManager.getLog(tp).get
    val endOffset = log.logEndOffset
    log.roll()
    assertTrue(broker.logManager.cleaner.awaitCleaned(tp, endOffset, 60000L))
  }

  private def withAdmin(f: Admin => Unit): Unit = {
    val admin: Admin = cluster.admin()
    try {
      f(admin)
    } finally {
      admin.close()
    }
  }

  private def withConsumer(
    groupId: String,
    groupProtocol: GroupProtocol,
    enableAutoCommit: Boolean = true
  )(f: Consumer[Array[Byte], Array[Byte]] => Unit): Unit = {
    val consumer = TestUtils.createConsumer(
      brokerList = cluster.bootstrapServers(),
      groupProtocol = groupProtocol,
      groupId = groupId,
      enableAutoCommit = enableAutoCommit
    )
    try {
      f(consumer)
    } finally {
      consumer.close()
    }
  }

  private def withStreamsApp(
    applicationId: String,
    inputTopic: String
  ): Unit = {
    val builder = new StreamsBuilder()
    builder.stream(inputTopic)

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)
    props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, StreamsGroupProtocol.STREAMS.name())
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "500")
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100")

    val streams = new KafkaStreams(builder.build(), props)
    try {
      streams.start()
      TestUtils.waitUntilTrue(
        () => streams.state() == KafkaStreams.State.RUNNING,
        msg = "Streams app did not reach RUNNING state"
      )
    } finally {
      streams.close(Duration.ofSeconds(30))
      streams.cleanUp()
    }
  }

  private def assertDescribedGroup(
    groups: Map[String, KafkaFuture[ConsumerGroupDescription]],
    groupId: String,
    groupType: GroupType,
    state: ConsumerGroupState
  ): Unit = {
    val group = groups(groupId).get(10, TimeUnit.SECONDS)

    assertEquals(groupId, group.groupId)
    assertEquals(groupType, group.`type`)
    assertEquals(state, group.state)
    assertEquals(java.util.List.of, group.members)
  }

  private def assertDescribedDeadGroup(
    groups: Map[String, KafkaFuture[ConsumerGroupDescription]],
    groupId: String
  ): Unit = {
    try {
      groups(groupId).get(10, TimeUnit.SECONDS)
      fail(s"Group $groupId should not be found")
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[GroupIdNotFoundException])
        assertEquals(s"Group $groupId not found.", e.getCause.getMessage)
    }
  }
}
