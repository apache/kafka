/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server.epoch

import kafka.server.OffsetsForLeaderEpoch
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.EpochEndOffset
import org.easymock.EasyMock._
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.mutable

class OffsetsForLeaderEpochTest {

  @Test
  def shouldGetEpochsFromReplica(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])
    val replica = createNiceMock(classOf[kafka.cluster.Replica])
    val cache = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])

    //Given
    val tp = new TopicPartition("topic", 1)
    val offset = 42
    val epochRequested: Integer = 5
    val request = mutable.Map(tp -> epochRequested).asJava

    //Stubs
    expect(replicaManager.getLeaderReplicaIfLocal(tp)).andReturn(replica)
    expect(replica.epochs).andReturn(Some(cache))
    expect(cache.endOffsetFor(epochRequested)).andReturn(offset)
    replay(replica, replicaManager, cache)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(new EpochEndOffset(Errors.NONE, offset), response.get(tp))
  }

  @Test
  def shonuldReturnNoLeaderForPartitionIfThrown(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])

    //Given
    val tp = new TopicPartition("topic", 1)
    val epochRequested: Integer = 5
    val request = mutable.Map(tp -> epochRequested).asJava

    //Stubs
    expect(replicaManager.getLeaderReplicaIfLocal(tp)).andThrow(new NotLeaderForPartitionException())
    replay(replicaManager)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH_OFFSET), response.get(tp))
  }

  @Test
  def shouldReturnUnknownTopicOrPartitionIfThrown(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])

    //Given
    val tp = new TopicPartition("topic", 1)
    val epochRequested: Integer = 5
    val request = mutable.Map(tp -> epochRequested).asJava

    //Stubs
    expect(replicaManager.getLeaderReplicaIfLocal(tp)).andThrow(new UnknownTopicOrPartitionException())
    replay(replicaManager)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH_OFFSET), response.get(tp))
  }
}