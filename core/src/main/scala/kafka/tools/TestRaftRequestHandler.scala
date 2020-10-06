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

package kafka.tools

import java.util.Collections

import kafka.network.RequestChannel
import kafka.raft.KafkaNetworkChannel
import kafka.server.ApiRequestHandler
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ApiVersionsResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.{AckMode, RaftClient}

import scala.jdk.CollectionConverters._

/**
 * Simple request handler implementation for use by [[TestRaftServer]].
 */
class TestRaftRequestHandler(
  networkChannel: KafkaNetworkChannel,
  requestChannel: RequestChannel,
  time: Time,
  client: RaftClient,
  metadataPartition: TopicPartition
) extends ApiRequestHandler with Logging {

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")
      request.header.apiKey match {
        case ApiKeys.VOTE
             | ApiKeys.BEGIN_QUORUM_EPOCH
             | ApiKeys.END_QUORUM_EPOCH
             | ApiKeys.FETCH =>
          val requestBody = request.body[AbstractRequest]
          networkChannel.postInboundRequest(
            request.header,
            requestBody,
            response => sendResponse(request, Some(response)))

        case ApiKeys.API_VERSIONS =>
          sendResponse(request, Option(ApiVersionsResponse.apiVersionsResponse(0, 2,
            Features.emptySupportedFeatures())))

        case ApiKeys.METADATA =>
          val metadataRequest = request.body[MetadataRequest]
          val topics = new MetadataResponseData.MetadataResponseTopicCollection

          if (!metadataRequest.data.topics.isEmpty) {
            val leaderAndEpoch = client.currentLeaderAndEpoch()

            if (metadataRequest.data.topics.size != 1
              || !metadataRequest.data.topics.get(0).name().equals(metadataPartition.topic)) {
              throw new IllegalArgumentException(s"Should only handle metadata request querying for " +
                s"`${metadataPartition.topic}, but found ${metadataRequest.data.topics}")
            }

            topics.add(new MetadataResponseTopic()
              .setErrorCode(Errors.NONE.code)
              .setName(metadataPartition.topic)
              .setIsInternal(true)
              .setPartitions(Collections.singletonList(new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code)
                .setPartitionIndex(metadataPartition.partition)
                .setLeaderId(leaderAndEpoch.leaderId.orElse(-1)))))
          }

          val brokers = new MetadataResponseData.MetadataResponseBrokerCollection
          networkChannel.allConnections().foreach { connection =>
            brokers.add(new MetadataResponseData.MetadataResponseBroker()
              .setNodeId(connection.id)
              .setHost(connection.host)
              .setPort(connection.port))
          }

          sendResponse(request, Option(new MetadataResponse(
            new MetadataResponseData()
              .setTopics(topics)
              .setBrokers(brokers))))

        case ApiKeys.PRODUCE =>
          val produceRequest = request.body[ProduceRequest]
          val records = produceRequest.partitionRecordsOrFail().get(metadataPartition)

          val ackMode = produceRequest.acks match {
            case 1 => AckMode.LEADER
            case -1 => AckMode.QUORUM
            case _ => throw new IllegalArgumentException(s"Unsupported ack mode ${produceRequest.acks} " +
              s"in Produce request (the only supported modes are acks=1 and acks=-1)")
          }

          client.append(records, ackMode, produceRequest.timeout)
            .whenComplete { (_, exception) =>
              val error = if (exception == null)
                Errors.NONE
              else
                Errors.forException(exception)

              sendResponse(request, Option(new ProduceResponse(
                Collections.singletonMap(metadataPartition,
                  new ProduceResponse.PartitionResponse(error)))))
            }

        case _ => throw new IllegalArgumentException(s"Unsupported api key: ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(request, e)
    } finally {
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  private def handleError(request: RequestChannel.Request, err: Throwable): Unit = {
    error("Error when handling request: " +
      s"clientId=${request.header.clientId}, " +
      s"correlationId=${request.header.correlationId}, " +
      s"api=${request.header.apiKey}, " +
      s"version=${request.header.apiVersion}, " +
      s"body=${request.body[AbstractRequest]}", err)

    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(0, err)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(err))
    else
      sendResponse(request, Some(response))
  }

  private def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  private def sendResponse(request: RequestChannel.Request,
                           responseOpt: Option[AbstractResponse]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    val response = responseOpt match {
      case Some(response) =>
        val responseSend = request.context.buildResponse(response)
        val responseString =
          if (RequestChannel.isRequestLoggingEnabled) Some(response.toString(request.context.apiVersion))
          else None
        new RequestChannel.SendResponse(request, responseSend, responseString, None)
      case None =>
        new RequestChannel.NoOpResponse(request)
    }
    sendResponse(response)
  }

  private def sendResponse(response: RequestChannel.Response): Unit = {
    requestChannel.sendResponse(response)
  }

}
