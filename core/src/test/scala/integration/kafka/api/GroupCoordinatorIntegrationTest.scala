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

import kafka.log.UnifiedLog
import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, Type}
import org.apache.kafka.common.test.api.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, ConsumerGroupDescription}
import org.apache.kafka.clients.consumer.{Consumer, GroupProtocol, OffsetAndMetadata}
import org.apache.kafka.common.{ConsumerGroupState, GroupType, KafkaFuture, TopicPartition}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith

import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
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
      consumer.commitSync(Map(
        new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0) -> new OffsetAndMetadata(10, "")
      ).asJava)

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
        consumer.subscribe(List("foo").asJava)
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
        .describeConsumerGroups(List("grp1").asJava)
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
        consumer.subscribe(List("foo").asJava)
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
        consumer.commitSync()
        consumer.unsubscribe()
        consumer.subscribe(List("foo").asJava)
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
        .describeConsumerGroups(List("grp2").asJava)
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
        consumer.subscribe(List("foo").asJava)
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      admin
        .deleteConsumerGroups(List("grp3").asJava)
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
        .describeConsumerGroups(List("grp3").asJava)
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp3", GroupType.CLASSIC, ConsumerGroupState.DEAD)
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
        consumer.subscribe(List("foo").asJava)
        TestUtils.waitUntilTrue(() => {
          consumer.poll(Duration.ofMillis(50))
          consumer.assignment().asScala.nonEmpty
        }, msg = "Consumer did not get an non empty assignment")
      }

      withConsumer(groupId = "grp4", groupProtocol = GroupProtocol.CONSUMER) { consumer =>
        consumer.subscribe(List("foo").asJava)
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
        .describeConsumerGroups(List("grp4").asJava)
        .describedGroups()
        .asScala
        .toMap

      assertDescribedGroup(groups, "grp4", GroupType.CONSUMER, ConsumerGroupState.EMPTY)
    }
  }

  private def rollAndCompactConsumerOffsets(): Unit = {
    val tp = new TopicPartition("__consumer_offsets", 0)
    val broker = cluster.brokers.asScala.head._2
    val log = broker.logManager.getLog(tp).get
    log.roll()
    assertTrue(broker.logManager.cleaner.awaitCleaned(tp, 0))
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
    assertEquals(Collections.emptyList, group.members)
  }
}
