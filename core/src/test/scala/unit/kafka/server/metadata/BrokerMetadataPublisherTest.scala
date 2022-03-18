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

package unit.kafka.server.metadata

import kafka.log.UnifiedLog
import kafka.server.metadata.BrokerMetadataPublisher
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataImageTest, TopicImage, TopicsImage}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.metadata.PartitionRegistration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import scala.jdk.CollectionConverters._

class BrokerMetadataPublisherTest {
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

}
