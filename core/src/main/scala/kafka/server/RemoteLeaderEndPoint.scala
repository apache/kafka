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

import java.util.Collections
import kafka.api.{KAFKA_0_10_1_IV2, KAFKA_0_11_0_IV0, KAFKA_2_0_IV0, KAFKA_2_0_IV1, KAFKA_2_1_IV1, KAFKA_2_2_IV1, KAFKA_2_3_IV1, KAFKA_2_8_IV0, KAFKA_3_0_IV1}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.Logging
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}

import scala.jdk.CollectionConverters._
import scala.collection.Map

class RemoteLeaderEndPoint(val endpoint: BlockingSend,
                           val fetchSessionHandler: FetchSessionHandler,
                           val brokerConfig: KafkaConfig) extends LeaderEndPoint with Logging {

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short = {
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_3_0_IV1) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 6
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_2_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0
  }

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short = {
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0
  }

  override def initiateClose(): Unit = endpoint.initiateClose()

  override def close(): Unit = endpoint.close()

  override def fetch(fetchRequest: FetchRequest.Builder): collection.Map[TopicPartition, FetchData] = {
    val clientResponse = try {
      endpoint.sendRequest(fetchRequest)
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
    val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    if (!fetchSessionHandler.handleResponse(fetchResponse, clientResponse.requestHeader().apiVersion())) {
      // If we had a session topic ID related error, throw it, otherwise return an empty fetch data map.
      if (fetchResponse.error == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
        throw Errors.forCode(fetchResponse.error().code()).exception()
      } else {
        Map.empty
      }
    } else {
      fetchResponse.responseData(fetchSessionHandler.sessionTopicNames, clientResponse.requestHeader().apiVersion()).asScala
    }
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_TIMESTAMP)
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.LATEST_TIMESTAMP)
  }

  private def fetchOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int, earliestOrLatest: Long): Long = {
    val topic = new ListOffsetsTopic()
      .setName(topicPartition.topic)
      .setPartitions(Collections.singletonList(
        new ListOffsetsPartition()
          .setPartitionIndex(topicPartition.partition)
          .setCurrentLeaderEpoch(currentLeaderEpoch)
          .setTimestamp(earliestOrLatest)))
    val requestBuilder = ListOffsetsRequest.Builder.forReplica(listOffsetRequestVersion, brokerConfig.brokerId)
      .setTargetTimes(Collections.singletonList(topic))

    val clientResponse = endpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetsResponse]
    val responsePartition = response.topics.asScala.find(_.name == topicPartition.topic).get
      .partitions.asScala.find(_.partitionIndex == topicPartition.partition).get

    Errors.forCode(responsePartition.errorCode) match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          responsePartition.offset
        else
          responsePartition.oldStyleOffsets.get(0)
      case error => throw error.exception
    }
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    if (partitions.isEmpty) {
      debug("Skipping leaderEpoch request since all partitions do not have an epoch")
      return Map.empty
    }

    val topics = new OffsetForLeaderTopicCollection(partitions.size)
    partitions.forKeyValue { (topicPartition, epochData) =>
      var topic = topics.find(topicPartition.topic)
      if (topic == null) {
        topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic)
        topics.add(topic)
      }
      topic.partitions.add(epochData)
    }

    val epochRequest = OffsetsForLeaderEpochRequest.Builder.forFollower(
      offsetForLeaderEpochRequestVersion, topics, brokerConfig.brokerId)
    debug(s"Sending offset for leader epoch request $epochRequest")

    try {
      val response = endpoint.sendRequest(epochRequest)
      val responseBody = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse]
      debug(s"Received leaderEpoch response $response")
      responseBody.data.topics.asScala.flatMap { offsetForLeaderTopicResult =>
        offsetForLeaderTopicResult.partitions.asScala.map { offsetForLeaderPartitionResult =>
          val tp = new TopicPartition(offsetForLeaderTopicResult.topic, offsetForLeaderPartitionResult.partition)
          tp -> offsetForLeaderPartitionResult
        }
      }.toMap
    } catch {
      case t: Throwable =>
        warn(s"Error when sending leader epoch request for $partitions", t)

        // if we get any unexpected exception, mark all partitions with an error
        val error = Errors.forException(t)
        partitions.map { case (tp, _) =>
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(error.code)
        }
    }
  }

  override def toString: String = s"RemoteLeaderEndPoint with ReplicaFetcherBlockingSend $endpoint"
}

