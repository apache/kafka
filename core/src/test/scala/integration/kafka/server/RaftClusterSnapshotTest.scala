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

import kafka.testkit.KafkaClusterTestKit
import kafka.testkit.TestKitNodes
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.Admin
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.Assertions._

@Timeout(120000)
class RaftClusterSnapshotTest {

  @Test
  def testContorllerSnapshotGenerated(): Unit = {
    val metadataSnapshotMinNewRecordBytes = 1
    val cluster = new KafkaClusterTestKit
      .Builder(
        new TestKitNodes.Builder()
          .setNumBrokerNodes(0)
          .setNumControllerNodes(3)
          .build()
      )
      .setConfigProp(
        KafkaConfig.MetadataSnapshotMinNewRecordBytesProp,
        metadataSnapshotMinNewRecordBytes.toString
      )
      .build()

    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(
        () => {
          cluster
          .raftManagers()
          .values()
          .iterator()
          .next()
          .kafkaRaftClient
          .leaderAndEpoch()
          .leaderId
          .isPresent
        },
        "RaftManager was not initialized."
      )

      // TODO: check that a snapshot was generated

      val admin = Admin.create(cluster.controllerClientProperties())
      try {
        assertEquals(
          cluster.nodes().clusterId().toString,
          admin.describeCluster().clusterId().get()
        )
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }
}
