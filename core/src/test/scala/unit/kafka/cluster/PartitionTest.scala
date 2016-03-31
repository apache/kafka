/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.cluster

import kafka.cluster.Partition
import kafka.cluster.Replica
import kafka.log.Log
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.server.LogOffsetMetadata
import kafka.server.ReplicaManager
import kafka.utils.Logging
import kafka.utils.SystemTime
import org.apache.kafka.common.protocol.Errors
import org.easymock.EasyMock._
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConversions._

class PartitionTest extends Logging {

  @Test
  def testCheckEnoughReplicasReachOffset() {
    def createPartition(logEndOffsets: Long*): Partition = {
      // Precondition: min.insync.replicas=2
      val minInSyncReplicas = 2

      val replicas = logEndOffsets.zip(Stream.from(0)).map { case (logEndOffset, i) =>
        val replica = createMock(classOf[Replica])
        val isLeader = i == 0
        if (isLeader) {
          val logConfig = new LogConfig(Map(LogConfig.MinInSyncReplicasProp -> minInSyncReplicas))
          val log = createMock(classOf[Log])
          expect(log.config).andReturn(logConfig)
          replay(log)
          expect(replica.log).andReturn(Some(log))
          // High watermark should be set to the minimum value of LogEndOffset in ISR replicas
          // by its definition.
          expect(replica.highWatermark).andReturn(LogOffsetMetadata(logEndOffsets.min))
        }
        expect(replica.isLocal).andReturn(isLeader)
        expect(replica.logEndOffset).andReturn(LogOffsetMetadata(logEndOffset))
        replay(replica)
        replica
      }

      val config = createMock(classOf[KafkaConfig])
      expect(config.brokerId).andReturn(1)
      replay(config)

      val replicaManager = createMock(classOf[ReplicaManager])
      expect(replicaManager.config).andReturn(config)
      expect(replicaManager.logManager).andReturn(null) // Return value won't be used
      expect(replicaManager.zkUtils).andReturn(null)    // Return value won't be used
      replay(replicaManager)

      val partition = new Partition("whatever", 0, SystemTime, replicaManager) {
        override def leaderReplicaIfLocal(): Option[Replica] =
          Some(replicas.head)
      }
      partition.inSyncReplicas = replicas.toSet
      partition
    }

    // When more than two replicas(including leader) satisfies requiredOffset.
    assertEquals((true, Errors.NONE.code),
      createPartition(
        2, // Leader
        2, // Caught up follower
        1) // Delayed follower
        .checkEnoughReplicasReachOffset(2))

    // When only one replica(should be the leader) satisfies requiredOffset but there are still sufficient
    // number of followers in ISR.
    assertEquals((false, Errors.NONE.code),
      createPartition(
        2, // Leader
        1, // Delayed follower
        1) // Delayed follower
        .checkEnoughReplicasReachOffset(2))

    // When there are just two replicas(including leader) and all they satisfies requiredOffset.
    assertEquals((true, Errors.NONE.code),
      createPartition(
        2, // Leader
        2) // Caught up follower
        .checkEnoughReplicasReachOffset(2))

    // When there's not sufficient number of followers in ISR.
    assertEquals((false, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code),
      createPartition(
        2) // Leader
        .checkEnoughReplicasReachOffset(2))
  }
}
