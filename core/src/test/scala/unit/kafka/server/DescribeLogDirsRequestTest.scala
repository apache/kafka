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

import java.io.File

import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DescribeLogDirsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class DescribeLogDirsRequestTest extends BaseRequestTest {
  override val logDirCount = 2
  override val brokerCount: Int = 1

  val topic = "topic"
  val partitionNum = 2
  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)

  @Test
  def testDescribeLogDirsRequest(): Unit = {
    val onlineDir = new File(servers.head.config.logDirs.head).getAbsolutePath
    val offlineDir = new File(servers.head.config.logDirs.tail.head).getAbsolutePath
    servers.head.replicaManager.handleLogDirFailure(offlineDir)
    createTopic(topic, partitionNum, 1)
    TestUtils.generateAndProduceMessages(servers, topic, 10)

    val request = new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(null)).build()
    val response = connectAndReceive[DescribeLogDirsResponse](request, destination = controllerSocketServer)
    case class ReplicaInfo(size: Long, offsetLag: Long, isFuture: Boolean)
    case class LogDirInfo(error: Errors, replicaInfos: Map[TopicPartition, ReplicaInfo])
    val logDirInfos = response.data.results.asScala.map{case result =>
      result.logDir() -> LogDirInfo(Errors.forCode(result.errorCode), result.topics.asScala.flatMap { topic =>
          topic.partitions.asScala.map { partition =>
            new TopicPartition(topic.name, partition.partitionIndex) -> ReplicaInfo(partition.partitionSize, partition.offsetLag, partition.isFutureKey)
          }.toMap
        }.toMap)
    }.toMap

    assertEquals(logDirCount, logDirInfos.size)
    assertEquals(Errors.KAFKA_STORAGE_ERROR, logDirInfos(offlineDir).error)
    assertEquals(0, logDirInfos(offlineDir).replicaInfos.size)

    assertEquals(Errors.NONE, logDirInfos(onlineDir).error)
    val replicaInfo0 = logDirInfos(onlineDir).replicaInfos(tp0)
    val replicaInfo1 = logDirInfos(onlineDir).replicaInfos(tp1)
    val log0 = servers.head.logManager.getLog(tp0).get
    val log1 = servers.head.logManager.getLog(tp1).get
    assertEquals(log0.size, replicaInfo0.size)
    assertEquals(log1.size, replicaInfo1.size)
    val logEndOffset = servers.head.logManager.getLog(tp0).get.logEndOffset
    assertTrue(s"LogEndOffset '$logEndOffset' should be > 0", logEndOffset > 0)
    assertEquals(servers.head.replicaManager.getLogEndOffsetLag(tp0, log0.logEndOffset, false), replicaInfo0.offsetLag)
    assertEquals(servers.head.replicaManager.getLogEndOffsetLag(tp1, log1.logEndOffset, false), replicaInfo1.offsetLag)
  }

}
