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

import org.apache.kafka.common.test.KafkaClusterTestKit
import org.apache.kafka.common.test.TestKitNodes
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.BufferSupplier
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.server.config.KRaftConfigs
import org.apache.kafka.snapshot.RecordsSnapshotReader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

import scala.jdk.CollectionConverters._
import scala.util.Using

@Timeout(120)
class RaftClusterSnapshotTest {

  @Test
  def testSnapshotsGenerated(): Unit = {
    val numberOfBrokers = 3
    val numberOfControllers = 3

    Using.resource(
      new KafkaClusterTestKit
        .Builder(
          new TestKitNodes.Builder()
            .setNumBrokerNodes(numberOfBrokers)
            .setNumControllerNodes(numberOfControllers)
            .build()
        )
        .setConfigProp(KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, "10")
        .setConfigProp(KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, "0")
        .build()
    ) { cluster =>
      cluster.format()
      cluster.startup()

      // Check that every controller and broker has a snapshot
      TestUtils.waitUntilTrue(
        () => {
          cluster.raftManagers().asScala.forall { case (_, raftManager) =>
            raftManager.replicatedLog.latestSnapshotId.isPresent
          }
        },
        s"Expected for every controller and broker to generate a snapshot: ${
          cluster.raftManagers().asScala.map { case (id, raftManager) =>
            (id, raftManager.replicatedLog.latestSnapshotId)
          }
        }"
      )

      assertEquals(numberOfControllers + numberOfBrokers, cluster.raftManagers.size())

      // For every controller and broker perform some sanity checks against the latest snapshot
      for ((_, raftManager) <- cluster.raftManagers().asScala) {
        Using.resource(
          RecordsSnapshotReader.of(
            raftManager.replicatedLog.latestSnapshot.get(),
            new MetadataRecordSerde(),
            BufferSupplier.create(),
            1,
            true
          )
        ) { snapshot =>
          // Check that the snapshot is non-empty
          assertTrue(snapshot.hasNext)

          // Check that we can read the entire snapshot
          while (snapshot.hasNext) {
            val batch = snapshot.next
            assertTrue(batch.sizeInBytes > 0)
            // A batch must have at least one control records or at least one data records, but not both
            assertNotEquals(
              batch.records.isEmpty,
              batch.controlRecords.isEmpty,
              s"data records = ${batch.records}; control records = ${batch.controlRecords}"
            )
          }
        }
      }
    }
  }
}
