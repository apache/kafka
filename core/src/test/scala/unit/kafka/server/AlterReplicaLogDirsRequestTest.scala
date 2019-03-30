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

package kafka.server

import kafka.network.SocketServer
import kafka.utils._
import java.io.File

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class AlterReplicaLogDirsRequestTest extends BaseRequestTest {
  override val logDirCount = 5
  override val brokerCount = 1

  val topic = "topic"

  @Test
  def testAlterReplicaLogDirsRequest() {
    val partitionNum = 5

    // Alter replica dir before topic creation
    val logDir1 = new File(servers.head.config.logDirs(Random.nextInt(logDirCount))).getAbsolutePath
    val partitionDirs1 = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir1).toMap
    val alterReplicaLogDirsResponse1 = sendAlterReplicaLogDirsRequest(partitionDirs1)

    // The response should show error UNKNOWN_TOPIC_OR_PARTITION for all partitions
    (0 until partitionNum).foreach { partition =>
      val tp = new TopicPartition(topic, partition)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, alterReplicaLogDirsResponse1.responses().get(tp))
      assertTrue(servers.head.logManager.getLog(tp).isEmpty)
    }

    createTopic(topic, partitionNum, 1)
    (0 until partitionNum).foreach { partition =>
      assertEquals(logDir1, servers.head.logManager.getLog(new TopicPartition(topic, partition)).get.dir.getParent)
    }

    // Alter replica dir again after topic creation
    val logDir2 = new File(servers.head.config.logDirs(Random.nextInt(logDirCount))).getAbsolutePath
    val partitionDirs2 = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir2).toMap
    val alterReplicaLogDirsResponse2 = sendAlterReplicaLogDirsRequest(partitionDirs2)
    // The response should succeed for all partitions
    (0 until partitionNum).foreach { partition =>
      val tp = new TopicPartition(topic, partition)
      assertEquals(Errors.NONE, alterReplicaLogDirsResponse2.responses().get(tp))
      TestUtils.waitUntilTrue(() => {
        logDir2 == servers.head.logManager.getLog(new TopicPartition(topic, partition)).get.dir.getParent
      }, "timed out waiting for replica movement")
    }
  }

  @Test
  def testAlterReplicaLogDirsRequestErrorCode(): Unit = {
    val offlineDir = new File(servers.head.config.logDirs.tail.head).getAbsolutePath
    val validDir1 = new File(servers.head.config.logDirs(1)).getAbsolutePath
    val validDir2 = new File(servers.head.config.logDirs(2)).getAbsolutePath
    val validDir3 = new File(servers.head.config.logDirs(3)).getAbsolutePath

    // Test AlterReplicaDirRequest before topic creation
    val partitionDirs1 = mutable.Map.empty[TopicPartition, String]
    partitionDirs1.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs1.put(new TopicPartition(topic, 1), validDir1)
    val alterReplicaDirResponse1 = sendAlterReplicaLogDirsRequest(partitionDirs1.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, alterReplicaDirResponse1.responses().get(new TopicPartition(topic, 0)))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, alterReplicaDirResponse1.responses().get(new TopicPartition(topic, 1)))

    createTopic(topic, 3, 1)

    // Test AlterReplicaDirRequest after topic creation
    val partitionDirs2 = mutable.Map.empty[TopicPartition, String]
    partitionDirs2.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs2.put(new TopicPartition(topic, 1), validDir2)
    val alterReplicaDirResponse2 = sendAlterReplicaLogDirsRequest(partitionDirs2.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, alterReplicaDirResponse2.responses().get(new TopicPartition(topic, 0)))
    assertEquals(Errors.NONE, alterReplicaDirResponse2.responses().get(new TopicPartition(topic, 1)))

    // Test AlterReplicaDirRequest after topic creation and log directory failure
    servers.head.logDirFailureChannel.maybeAddOfflineLogDir(offlineDir, "", new java.io.IOException())
    TestUtils.waitUntilTrue(() => !servers.head.logManager.isLogDirOnline(offlineDir), s"timed out waiting for $offlineDir to be offline", 3000)
    val partitionDirs3 = mutable.Map.empty[TopicPartition, String]
    partitionDirs3.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs3.put(new TopicPartition(topic, 1), validDir3)
    partitionDirs3.put(new TopicPartition(topic, 2), offlineDir)
    val alterReplicaDirResponse3 = sendAlterReplicaLogDirsRequest(partitionDirs3.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, alterReplicaDirResponse3.responses().get(new TopicPartition(topic, 0)))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, alterReplicaDirResponse3.responses().get(new TopicPartition(topic, 1)))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, alterReplicaDirResponse3.responses().get(new TopicPartition(topic, 2)))
  }

  private def sendAlterReplicaLogDirsRequest(partitionDirs: Map[TopicPartition, String], socketServer: SocketServer = controllerSocketServer): AlterReplicaLogDirsResponse = {
    val request = new AlterReplicaLogDirsRequest.Builder(partitionDirs.asJava).build()
    val response = connectAndSend(request, ApiKeys.ALTER_REPLICA_LOG_DIRS, socketServer)
    AlterReplicaLogDirsResponse.parse(response, request.version)
  }

}
