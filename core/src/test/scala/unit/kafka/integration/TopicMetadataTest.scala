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

package kafka.integration

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.admin.CreateTopicCommand
import java.nio.ByteBuffer
import kafka.log.LogManager
import kafka.utils.TestUtils
import kafka.server.{KafkaApis, KafkaConfig}
import junit.framework.Assert._
import org.I0Itec.zkclient.ZkClient
import TestUtils._
import org.easymock.EasyMock
import kafka.network.BoundedByteBufferReceive
import kafka.api.{TopicMetadataSend, TopicMetadataRequest}
import kafka.cluster.Broker

class TopicMetadataTest extends JUnit3Suite with ZooKeeperTestHarness {
  val props = createBrokerConfigs(1)
  val configs = props.map(p => new KafkaConfig(p) { override val flushInterval = 1})
  var zkClient: ZkClient = null
  var brokers: Seq[Broker] = null

  override def setUp() {
    super.setUp()
    zkClient = zookeeper.client
    // create brokers in zookeeper
    brokers = TestUtils.createBrokersInZk(zkClient, configs.map(config => config.brokerId))
  }

  override def tearDown() {
    super.tearDown()
  }

  def testTopicMetadataRequest {
    // create topic
    val topic = "test"
    CreateTopicCommand.createTopic(zkClient, topic, 1)

    // create a topic metadata request
    val topicMetadataRequest = new TopicMetadataRequest(List(topic))

    val serializedMetadataRequest = ByteBuffer.allocate(topicMetadataRequest.sizeInBytes + 2)
    topicMetadataRequest.writeTo(serializedMetadataRequest)
    serializedMetadataRequest.rewind()
    val deserializedMetadataRequest = TopicMetadataRequest.readFrom(serializedMetadataRequest)

    assertEquals(topicMetadataRequest, deserializedMetadataRequest)
  }

  def testBasicTopicMetadata {
    // create topic
    val topic = "test"
    CreateTopicCommand.createTopic(zkClient, topic, 1)

    mockLogManagerAndTestTopic(topic)
  }

  def testAutoCreateTopic {
    // auto create topic
    val topic = "test"

    mockLogManagerAndTestTopic(topic)
  }

  private def mockLogManagerAndTestTopic(topic: String) = {
    // topic metadata request only requires 2 APIs from the log manager
    val logManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(logManager.getServerConfig).andReturn(configs.head)
    EasyMock.expect(logManager.getZookeeperClient).andReturn(zkClient)
    EasyMock.replay(logManager)

    // create a topic metadata request
    val topicMetadataRequest = new TopicMetadataRequest(List(topic))

    val serializedMetadataRequest = ByteBuffer.allocate(topicMetadataRequest.sizeInBytes + 2)
    topicMetadataRequest.writeTo(serializedMetadataRequest)
    serializedMetadataRequest.rewind()

    // create the kafka request handler
    val kafkaRequestHandler = new KafkaApis(logManager)

    // mock the receive API to return the request buffer as created above
    val receivedRequest = EasyMock.createMock(classOf[BoundedByteBufferReceive])
    EasyMock.expect(receivedRequest.buffer).andReturn(serializedMetadataRequest)
    EasyMock.replay(receivedRequest)

    // call the API (to be tested) to get metadata
    val metadataResponse = kafkaRequestHandler.handleTopicMetadataRequest(receivedRequest)

    // verify the topic metadata returned
    metadataResponse match {
      case Some(metadata) =>
        val responseBuffer = metadata.asInstanceOf[TopicMetadataSend].metadata
        val topicMetadata = TopicMetadataRequest.deserializeTopicsMetadataResponse(responseBuffer)
        assertEquals("Expecting metadata only for 1 topic", 1, topicMetadata.size)
        assertEquals("Expecting metadata for the test topic", "test", topicMetadata.head.topic)
        val partitionMetadata = topicMetadata.head.partitionsMetadata
        assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
        assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
        assertEquals(brokers, partitionMetadata.head.replicas)
        assertNull("Not expecting log metadata", partitionMetadata.head.logMetadata.getOrElse(null))
      case None =>
        fail("Metadata response expected")
    }

    // verify the expected calls to log manager occurred in the right order
    EasyMock.verify(logManager)
    EasyMock.verify(receivedRequest)
  }
}