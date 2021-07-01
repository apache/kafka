/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.raft.KafkaRaftManager;
import kafka.testkit.KafkaClusterTestKit
import kafka.testkit.TestKitNodes
import kafka.utils.TestUtils
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import scala.jdk.CollectionConverters._

@Timeout(120000)
class RaftClusterSnapshotTest {

  @Test
  def testContorllerSnapshotGenerated(): Unit = {
    val metadataSnapshotMaxNewRecordBytes = 1
    val cluster = new KafkaClusterTestKit
      .Builder(
        new TestKitNodes.Builder()
          .setNumBrokerNodes(3)
          .setNumControllerNodes(3)
          .build()
      )
      .setConfigProp(
        KafkaConfig.MetadataSnapshotMaxNewRecordBytesProp,
        metadataSnapshotMaxNewRecordBytes.toString
      )
      .build()

    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(
        () => controllerRaftManagers(cluster).forall(_.kafkaRaftClient.leaderAndEpoch.leaderId.isPresent),
        "Raft leader was never elected"
      )

      TestUtils.waitUntilTrue(
        () => controllerRaftManagers(cluster).forall(_.replicatedLog.latestSnapshotId.isPresent),
        s"Expected for every controller to generate a snapshot: ${
          controllerRaftManagers(cluster).map(_.replicatedLog.latestSnapshotId)
        }}"
      )

    } finally {
      cluster.close()
    }
  }

  private def controllerRaftManagers(cluster: KafkaClusterTestKit): List[KafkaRaftManager[ApiMessageAndVersion]] = {
    val controllerIds = cluster.controllers.keySet.asScala

    cluster.raftManagers().asScala.flatMap { case (replicaId, raftManager) =>
      if (controllerIds.contains(replicaId)) {
        Some(raftManager)
      } else {
        None
      }
    }.toList
  }
}
