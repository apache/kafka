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
import org.apache.kafka.common.requests._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable

class AlterReplicaLogDirsRequestTest extends BaseRequestTest {

  override def numBrokers: Int = 1
  override def logDirCount: Int = 5

  val topic = "topic"

  @Test
  def testAlterReplicaLogDirsRequestBeforeTopicCreation() {
    val partitionNum = 5
    val logDir = new File(servers.head.config.logDirs.head).getAbsolutePath
    val partitionDirs = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir).toMap
    val alterReplicaDirResponse = sendAlterReplicaLogDirsRequest(partitionDirs)

    // The response should show error REPLICA_NOT_AVAILABLE for all partitions
    (0 until partitionNum).foreach { partition =>
      val tp = new TopicPartition(topic, partition)
      assertEquals(Errors.REPLICA_NOT_AVAILABLE, alterReplicaDirResponse.responses().get(tp))
      assertTrue(servers.head.logManager.getLog(tp).isEmpty)
    }

    TestUtils.createTopic(zkUtils, topic, partitionNum, 1, servers)
    (0 until partitionNum).foreach { partition =>
      assertEquals(logDir, servers.head.logManager.getLog(new TopicPartition(topic, partition)).get.dir.getParent)
    }
  }

  @Test
  def testAlterReplicaLogDirsRequestErrorCode(): Unit = {
    val validDir = new File(servers.head.config.logDirs.head).getAbsolutePath
    val offlineDir = new File(servers.head.config.logDirs.tail.head).getAbsolutePath
    servers.head.logDirFailureChannel.maybeAddOfflineLogDir(offlineDir, "", new java.io.IOException())
    TestUtils.createTopic(zkUtils, topic, 3, 1, servers)

    val partitionDirs = mutable.Map.empty[TopicPartition, String]
    partitionDirs.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs.put(new TopicPartition(topic, 1), validDir)
    partitionDirs.put(new TopicPartition(topic, 2), offlineDir)

    val alterReplicaDirResponse = sendAlterReplicaLogDirsRequest(partitionDirs.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, alterReplicaDirResponse.responses().get(new TopicPartition(topic, 0)))
    assertEquals(Errors.NONE, alterReplicaDirResponse.responses().get(new TopicPartition(topic, 1)))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, alterReplicaDirResponse.responses().get(new TopicPartition(topic, 2)))
  }

  private def sendAlterReplicaLogDirsRequest(partitionDirs: Map[TopicPartition, String], socketServer: SocketServer = controllerSocketServer): AlterReplicaLogDirsResponse = {
    val request = new AlterReplicaLogDirsRequest.Builder(partitionDirs.asJava).build()
    val response = connectAndSend(request, ApiKeys.ALTER_REPLICA_LOG_DIRS, socketServer)
    AlterReplicaLogDirsResponse.parse(response, request.version)
  }

}
