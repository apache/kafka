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

package kafka.controller


import org.scalatest.junit.JUnit3Suite

import junit.framework.Assert._
import java.nio.ByteBuffer
import kafka.common.ErrorMapping
import kafka.api._
import collection.mutable.Map
import collection.mutable.Set
import kafka.integration.KafkaServerTestHarness
import kafka.utils.TestUtils
import kafka.server.KafkaConfig
import kafka.network.{Receive, BlockingChannel}


class ControllerToBrokerRequestTest extends JUnit3Suite with KafkaServerTestHarness  {

  val kafkaProps = TestUtils.createBrokerConfigs(1)
  val configs = List(new KafkaConfig(kafkaProps.head))
  var blockingChannel: BlockingChannel = null

  override def setUp() {
    super.setUp()
    blockingChannel = new BlockingChannel("localhost", configs.head.port, 1000000, 0, 64*1024)
    blockingChannel.connect
  }

  override def tearDown() {
    super.tearDown()
    blockingChannel.disconnect()
  }


  def createSampleLeaderAndISRRequest() : LeaderAndISRRequest = {
    val topic1 = "test1"
    val topic2 = "test2"

    val leader1 = 1;
    val ISR1 = List(1, 2, 3)

    val leader2 = 2;
    val ISR2 = List(2, 3, 4)

    val leaderAndISR1 = new LeaderAndISR(leader1, 1, ISR1, 1)
    val leaderAndISR2 = new LeaderAndISR(leader2, 1, ISR2, 2)
    val map = Map(((topic1, 1), leaderAndISR1), ((topic1, 2), leaderAndISR1),
                  ((topic2, 1), leaderAndISR2), ((topic2, 2), leaderAndISR2))

    new LeaderAndISRRequest(1, "client 1", 1, 4, map)
  }

  def createSampleLeaderAndISRResponse() : LeaderAndISRResponse = {
    val topic1 = "test1"
    val topic2 = "test2"
    val responseMap = Map(((topic1, 1), ErrorMapping.NoError), ((topic1, 2), ErrorMapping.NoError),
                          ((topic2, 1), ErrorMapping.NoError), ((topic2, 2), ErrorMapping.NoError))

    new LeaderAndISRResponse(1, responseMap)
  }


  def createSampleStopReplicaRequest() : StopReplicaRequest = {
    val topic1 = "test1"
    val topic2 = "test2"
    new StopReplicaRequest(1, "client 1", 1000, Set((topic1, 1), (topic1, 2),
                                           (topic2, 1), (topic2, 2)))
  }

  def createSampleStopReplicaResponse() : StopReplicaResponse = {
    val topic1 = "test1"
    val topic2 = "test2"
    val responseMap = Map(((topic1, 1), ErrorMapping.NoError), ((topic1, 2), ErrorMapping.NoError),
                          ((topic2, 1), ErrorMapping.NoError), ((topic2, 2), ErrorMapping.NoError))

    new StopReplicaResponse(1, responseMap)
  }


  def testLeaderAndISRRequest {
    val leaderAndISRRequest = createSampleLeaderAndISRRequest()

    val serializedLeaderAndISRRequest = ByteBuffer.allocate(leaderAndISRRequest.sizeInBytes)
    leaderAndISRRequest.writeTo(serializedLeaderAndISRRequest)
    serializedLeaderAndISRRequest.rewind()
    val deserializedLeaderAndISRRequest = LeaderAndISRRequest.readFrom(serializedLeaderAndISRRequest)

    assertEquals(leaderAndISRRequest, deserializedLeaderAndISRRequest)
  }

  def testLeaderAndISRResponse {
    val leaderAndISRResponse = createSampleLeaderAndISRResponse()

    val serializedLeaderAndISRResponse = ByteBuffer.allocate(leaderAndISRResponse.sizeInBytes)
    leaderAndISRResponse.writeTo(serializedLeaderAndISRResponse)
    serializedLeaderAndISRResponse.rewind()
    val deserializedLeaderAndISRResponse = LeaderAndISRResponse.readFrom(serializedLeaderAndISRResponse)
    assertEquals(leaderAndISRResponse, deserializedLeaderAndISRResponse)
  }


  def testStopReplicaRequest {
    val stopReplicaRequest = createSampleStopReplicaRequest()

    val serializedStopReplicaRequest = ByteBuffer.allocate(stopReplicaRequest.sizeInBytes)
    stopReplicaRequest.writeTo(serializedStopReplicaRequest)
    serializedStopReplicaRequest.rewind()
    val deserializedStopReplicaRequest = StopReplicaRequest.readFrom(serializedStopReplicaRequest)
    assertEquals(stopReplicaRequest, deserializedStopReplicaRequest)
  }


  def testStopReplicaResponse {
    val stopReplicaResponse = createSampleStopReplicaResponse()

    val serializedStopReplicaResponse = ByteBuffer.allocate(stopReplicaResponse.sizeInBytes)
    stopReplicaResponse.writeTo(serializedStopReplicaResponse)
    serializedStopReplicaResponse.rewind()
    val deserializedStopReplicaResponse = StopReplicaResponse.readFrom(serializedStopReplicaResponse)
    assertEquals(stopReplicaResponse, deserializedStopReplicaResponse)
  }



  def testEndToEndLeaderAndISRRequest {

    val leaderAndISRRequest = createSampleLeaderAndISRRequest()

    var response: Receive = null
    blockingChannel.send(leaderAndISRRequest)
    response = blockingChannel.receive()

    val leaderAndISRResponse = LeaderAndISRResponse.readFrom(response.buffer)
    val expectedLeaderAndISRResponse = createSampleLeaderAndISRResponse()

    assertEquals(leaderAndISRResponse, expectedLeaderAndISRResponse)

  }



  def testEndToEndStopReplicaRequest {
    val stopReplicaRequest = createSampleStopReplicaRequest()

    var response: Receive = null
    blockingChannel.send(stopReplicaRequest)
    response = blockingChannel.receive()

    val stopReplicaResponse = StopReplicaResponse.readFrom(response.buffer)
    val expectedStopReplicaResponse = createSampleStopReplicaResponse()
    assertEquals(stopReplicaResponse, expectedStopReplicaResponse)

  }

}