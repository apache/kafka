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

package kafka.server

import kafka.api.{ProducerResponseStatus, SerializationTestUtils, ProducerRequest}
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, MockScheduler, MockTime, TestUtils}

import java.util.concurrent.atomic.AtomicBoolean
import java.io.File

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.{MockTime => JMockTime}
import org.easymock.EasyMock
import org.I0Itec.zkclient.ZkClient
import org.junit.Test

import scala.collection.Map

class ReplicaManagerTest {

  val topic = "test-topic"

  @Test
  def testHighWaterMarkDirectoryMapping() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    val config = KafkaConfig.fromProps(props)
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    val zkUtils = ZkUtils(zkClient, false)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)).toArray)
    val time: MockTime = new MockTime()
    val jTime = new JMockTime
    val metrics = new Metrics
    val rm = new ReplicaManager(config, metrics, time, jTime, zkUtils, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false))
    try {
      val partition = rm.getOrCreatePartition(topic, 1)
      partition.getOrCreateReplica(1)
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(false)
      metrics.close()
    }
  }

  @Test
  def testHighwaterMarkRelativeDirectoryMapping() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    val zkUtils = ZkUtils(zkClient, false)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)).toArray)
    val time: MockTime = new MockTime()
    val jTime = new JMockTime
    val metrics = new Metrics
    val rm = new ReplicaManager(config, metrics, time, jTime, zkUtils, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false))
    try {
      val partition = rm.getOrCreatePartition(topic, 1)
      partition.getOrCreateReplica(1)
      rm.checkpointHighWatermarks()
    } finally {
      // shutdown the replica manager upon test completion
      rm.shutdown(false)
      metrics.close()
    }
  }

  @Test
  def testIllegalRequiredAcks() {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    val config = KafkaConfig.fromProps(props)
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    val zkUtils = ZkUtils(zkClient, false)
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)).toArray)
    val time: MockTime = new MockTime()
    val jTime = new JMockTime
    val metrics = new Metrics
    val rm = new ReplicaManager(config, metrics, time, jTime, zkUtils, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), Option(this.getClass.getName))
    try {
      val produceRequest = new ProducerRequest(1, "client 1", 3, 1000, SerializationTestUtils.topicDataProducerRequest)
      def callback(responseStatus: Map[TopicAndPartition, ProducerResponseStatus]) = {
        assert(responseStatus.values.head.error == Errors.INVALID_REQUIRED_ACKS.code)
      }
      rm.appendMessages(timeout = 0, requiredAcks = 3, internalTopicsAllowed = false, messagesPerPartition = produceRequest.data, responseCallback = callback)
    } finally {
      rm.shutdown(false)
      metrics.close()
    }

    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }
}
