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
package kafka.server.epoch.util

import java.net.SocketTimeoutException
import java.util
import kafka.cluster.BrokerEndPoint
import kafka.server.BlockingSend
import org.apache.kafka.clients.{ClientRequest, ClientResponse, MockClient, NetworkClientUtils}
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochResponseData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.requests.{AbstractRequest, FetchResponse, OffsetsForLeaderEpochResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.{SystemTime, Time}
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}

import scala.collection.Map

/**
  * Stub network client used for testing the ReplicaFetcher, wraps the MockClient used for consumer testing
  *
  * The common case is that there is only one OFFSET_FOR_LEADER_EPOCH request/response. So, the
  * response to OFFSET_FOR_LEADER_EPOCH is 'offsets' map. If the test needs to set another round of
  * OFFSET_FOR_LEADER_EPOCH with different offsets in response, it should update offsets using
  * setOffsetsForNextResponse
  */
class ReplicaFetcherMockBlockingSend(offsets: java.util.Map[TopicPartition, EpochEndOffset],
                                     sourceBroker: BrokerEndPoint,
                                     time: Time)
  extends BlockingSend {

  private val client = new MockClient(new SystemTime)
  var fetchCount = 0
  var epochFetchCount = 0
  var lastUsedOffsetForLeaderEpochVersion = -1
  var callback: Option[() => Unit] = None
  var currentOffsets: util.Map[TopicPartition, EpochEndOffset] = offsets
  var fetchPartitionData: Map[TopicPartition, FetchResponseData.PartitionData] = Map.empty
  var topicIds: Map[String, Uuid] = Map.empty
  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

  def setEpochRequestCallback(postEpochFunction: () => Unit): Unit = {
    callback = Some(postEpochFunction)
  }

  def setOffsetsForNextResponse(newOffsets: util.Map[TopicPartition, EpochEndOffset]): Unit = {
    currentOffsets = newOffsets
  }

  def setFetchPartitionDataForNextResponse(partitionData: Map[TopicPartition, FetchResponseData.PartitionData]): Unit = {
    fetchPartitionData = partitionData
  }

  def setIdsForNextResponse(topicIds: Map[String, Uuid]): Unit = {
    this.topicIds = topicIds
  }

  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {
    if (!NetworkClientUtils.awaitReady(client, sourceNode, time, 500))
      throw new SocketTimeoutException(s"Failed to connect within 500 ms")

    //Send the request to the mock client
    val clientRequest = request(requestBuilder)
    client.send(clientRequest, time.milliseconds())

    //Create a suitable response based on the API key
    val response = requestBuilder.apiKey() match {
      case ApiKeys.OFFSET_FOR_LEADER_EPOCH =>
        callback.foreach(_.apply())
        epochFetchCount += 1
        lastUsedOffsetForLeaderEpochVersion = requestBuilder.latestAllowedVersion()

        val data = new OffsetForLeaderEpochResponseData()
        currentOffsets.forEach((tp, offsetForLeaderPartition) => {
          var topic = data.topics.find(tp.topic)
          if (topic == null) {
            topic = new OffsetForLeaderTopicResult()
              .setTopic(tp.topic)
            data.topics.add(topic)
          }
          topic.partitions.add(offsetForLeaderPartition)
        })

        new OffsetsForLeaderEpochResponse(data)

      case ApiKeys.FETCH =>
        fetchCount += 1
        val partitionData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
        fetchPartitionData.foreach { case (tp, data) => partitionData.put(new TopicIdPartition(topicIds.getOrElse(tp.topic(), Uuid.ZERO_UUID), tp), data) }
        fetchPartitionData = Map.empty
        topicIds = Map.empty
        FetchResponse.of(Errors.NONE, 0,
          if (partitionData.isEmpty) JFetchMetadata.INVALID_SESSION_ID else 1,
          partitionData)

      case _ =>
        throw new UnsupportedOperationException
    }

    //Use mock client to create the appropriate response object
    client.respondFrom(response, sourceNode)
    client.poll(30, time.milliseconds()).iterator().next()
  }

  private def request(requestBuilder: Builder[_ <: AbstractRequest]): ClientRequest = {
    client.newClientRequest(
      sourceBroker.id.toString,
      requestBuilder,
      time.milliseconds(),
      true)
  }

  override def initiateClose(): Unit = {}

  override def close(): Unit = {}
}
