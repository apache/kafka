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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class DescribeDirsRequestTest extends BaseRequestTest {

  override def numBrokers: Int = 1
  override def logDirCount: Int = 2

  val topic = "topic"
  val partitionNum = 2
  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)

  @Test
  def testDescribeDirsRequestWithSpecificLogDirAndPartition() {
    val invalidDir = "invalidDir"
    val onlineDir = servers.head.config.logDirs.head
    val offlineDir = servers.head.config.logDirs.tail.head
    servers.head.logDirFailureChannel.maybeAddOfflineLogDir(offlineDir, "", new java.io.IOException())
    TestUtils.createTopic(zkUtils, topic, partitionNum, 1, servers)
    TestUtils.produceMessages(servers, topic, 10)

    val logDirInfos = sendDescribeDirsRequest(Set(invalidDir, onlineDir), Set(tp0)).logDirInfos()
    assertEquals(2, logDirInfos.size())

    assertEquals(Errors.DIR_NOT_AVAILABLE, logDirInfos.get(invalidDir).error)
    assertEquals(0, logDirInfos.get(invalidDir).replicaInfos.size())

    assertEquals(Errors.NONE, logDirInfos.get(onlineDir).error)
    val replicaInfo0 = logDirInfos.get(onlineDir).replicaInfos.get(tp0)
    assertFalse(logDirInfos.get(onlineDir).replicaInfos.containsKey(tp1))
    assertEquals(servers.head.logManager.getLog(tp0).get.size, replicaInfo0.size)
    assertTrue(servers.head.logManager.getLog(tp0).get.logEndOffset > 0)
    assertEquals(servers.head.logManager.getLog(tp0).get.logEndOffset, replicaInfo0.logEndOffset)
  }

  @Test
  def testDescribeDirsRequestWithWildcardLogDirAndPartition(): Unit = {
    val onlineDir = servers.head.config.logDirs.head
    val offlineDir = servers.head.config.logDirs.tail.head
    servers.head.logDirFailureChannel.maybeAddOfflineLogDir(offlineDir, "", new java.io.IOException())
    TestUtils.createTopic(zkUtils, topic, partitionNum, 1, servers)
    TestUtils.produceMessages(servers, topic, 10)

    val logDirInfos = sendDescribeDirsRequest(Set.empty[String], Set.empty[TopicPartition]).logDirInfos()
    assertEquals(logDirCount, logDirInfos.size())

    assertEquals(Errors.KAFKA_STORAGE_ERROR, logDirInfos.get(offlineDir).error)
    assertEquals(0, logDirInfos.get(offlineDir).replicaInfos.size())


    assertEquals(Errors.NONE, logDirInfos.get(onlineDir).error)
    val replicaInfo0 = logDirInfos.get(onlineDir).replicaInfos.get(tp0)
    val replicaInfo1 = logDirInfos.get(onlineDir).replicaInfos.get(tp1)
    assertEquals(servers.head.logManager.getLog(tp0).get.size, replicaInfo0.size)
    assertEquals(servers.head.logManager.getLog(tp1).get.size, replicaInfo1.size)
    assertTrue(servers.head.logManager.getLog(tp0).get.logEndOffset > 0)
    assertEquals(servers.head.logManager.getLog(tp0).get.logEndOffset, replicaInfo0.logEndOffset)
    assertEquals(servers.head.logManager.getLog(tp1).get.logEndOffset, replicaInfo1.logEndOffset)
  }

  private def sendDescribeDirsRequest(logDirs: Set[String],
                                      partitions: Set[TopicPartition],
                                      socketServer: SocketServer = controllerSocketServer): DescribeDirsResponse = {
    val request = new DescribeDirsRequest.Builder(logDirs.asJava, partitions.asJava).build()
    val response = connectAndSend(request, ApiKeys.DESCRIBE_DIRS, socketServer)
    DescribeDirsResponse.parse(response, request.version)
  }
}
