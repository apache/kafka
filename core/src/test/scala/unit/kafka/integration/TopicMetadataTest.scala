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
import junit.framework.Assert._
import org.easymock.EasyMock
import kafka.network._
import kafka.cluster.Broker
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.server.{ReplicaManager, KafkaZooKeeper, KafkaApis, KafkaConfig}
import kafka.common.ErrorMapping
import kafka.api.{TopicMetadata, TopicMetaDataResponse, TopicMetadataRequest}


class TopicMetadataTest extends JUnit3Suite with ZooKeeperTestHarness {
  val props = createBrokerConfigs(1)
  val configs = props.map(p => new KafkaConfig(p) { override val flushInterval = 1})
  var brokers: Seq[Broker] = null

  override def setUp() {
    super.setUp()
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
    // set up leader for topic partition 0
    val leaderForPartitionMap = Map(
      0 -> configs.head.brokerId
    )
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap)
    val topicMetadata = mockLogManagerAndTestTopic(topic)
    assertEquals("Expecting metadata only for 1 topic", 1, topicMetadata.size)
    assertEquals("Expecting metadata for the test topic", "test", topicMetadata.head.topic)
    val partitionMetadata = topicMetadata.head.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
    assertNull("Not expecting log metadata", partitionMetadata.head.logMetadata.getOrElse(null))
    assertEquals(1, partitionMetadata.head.replicas.size)
  }

  def testAutoCreateTopic {
    // auto create topic
    val topic = "test"

    val topicMetadata = mockLogManagerAndTestTopic(topic)
    assertEquals("Expecting metadata only for 1 topic", 1, topicMetadata.size)
    assertEquals("Expecting metadata for the test topic", "test", topicMetadata.head.topic)
    val partitionMetadata = topicMetadata.head.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
    assertNull("Not expecting log metadata", partitionMetadata.head.logMetadata.getOrElse(null))
    assertEquals(0, partitionMetadata.head.replicas.size)
    assertEquals(None, partitionMetadata.head.leader)
    assertEquals(ErrorMapping.LeaderNotAvailableCode, partitionMetadata.head.errorCode)
  }

  private def mockLogManagerAndTestTopic(topic: String): Seq[TopicMetadata] = {
    // topic metadata request only requires 2 APIs from the log manager
    val logManager = EasyMock.createMock(classOf[LogManager])
    val kafkaZookeeper = EasyMock.createMock(classOf[KafkaZooKeeper])
    val replicaManager = EasyMock.createMock(classOf[ReplicaManager])
    EasyMock.expect(kafkaZookeeper.getZookeeperClient).andReturn(zkClient)
    EasyMock.expect(logManager.config).andReturn(configs.head)
    EasyMock.replay(logManager)
    EasyMock.replay(kafkaZookeeper)

    // create a topic metadata request
    val topicMetadataRequest = new TopicMetadataRequest(List(topic))

    val serializedMetadataRequest = ByteBuffer.allocate(topicMetadataRequest.sizeInBytes + 2)
    topicMetadataRequest.writeTo(serializedMetadataRequest)
    serializedMetadataRequest.rewind()

    // create the kafka request handler
    val requestChannel = new RequestChannel(2, 5)
    val apis = new KafkaApis(requestChannel, logManager, replicaManager, kafkaZookeeper)

    // mock the receive API to return the request buffer as created above
    val receivedRequest = EasyMock.createMock(classOf[BoundedByteBufferReceive])
    EasyMock.expect(receivedRequest.buffer).andReturn(serializedMetadataRequest)
    EasyMock.replay(receivedRequest)

    // call the API (to be tested) to get metadata
    apis.handleTopicMetadataRequest(new RequestChannel.Request(processor=0, requestKey=5, request=receivedRequest, start=1))
    val metadataResponse = requestChannel.receiveResponse(0).response.asInstanceOf[BoundedByteBufferSend].buffer
    
    // check assertions
    val topicMetadata = TopicMetaDataResponse.readFrom(metadataResponse).topicsMetadata

    // verify the expected calls to log manager occurred in the right order
    EasyMock.verify(kafkaZookeeper)
    EasyMock.verify(receivedRequest)

    topicMetadata
  }
}