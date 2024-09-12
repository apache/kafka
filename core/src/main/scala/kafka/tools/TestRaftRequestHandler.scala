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

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.{ApiRequestHandler, ApiVersionManager}
import kafka.utils.Logging
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.{BeginQuorumEpochResponseData, EndQuorumEpochResponseData, FetchResponseData, FetchSnapshotResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BeginQuorumEpochResponse, EndQuorumEpochResponse, FetchResponse, FetchSnapshotResponse, VoteResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.RequestLocal

/**
 * Simple request handler implementation for use by [[TestRaftServer]].
 */
class TestRaftRequestHandler(
  raftManager: RaftManager[_],
  requestChannel: RequestChannel,
  time: Time,
  apiVersionManager: ApiVersionManager
) extends ApiRequestHandler with Logging {

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      trace(s"Handling request:${request.requestDesc(true)} with context ${request.context}")
      request.header.apiKey match {
        case ApiKeys.API_VERSIONS => handleApiVersions(request)
        case ApiKeys.VOTE => handleVote(request)
        case ApiKeys.BEGIN_QUORUM_EPOCH => handleBeginQuorumEpoch(request)
        case ApiKeys.END_QUORUM_EPOCH => handleEndQuorumEpoch(request)
        case ApiKeys.FETCH => handleFetch(request)
        case ApiKeys.FETCH_SNAPSHOT => handleFetchSnapshot(request)
        case _ => throw new IllegalArgumentException(s"Unsupported api key: ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable =>
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", e)
        val errorResponse = request.body[AbstractRequest].getErrorResponse(e)
        requestChannel.sendResponse(request, errorResponse, None)
    } finally {
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  private def handleApiVersions(request: RequestChannel.Request): Unit = {
    requestChannel.sendResponse(request, apiVersionManager.apiVersionResponse(throttleTimeMs = 0, request.header.apiVersion() < 4), None)
  }

  private def handleVote(request: RequestChannel.Request): Unit = {
    handle(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  private def handleBeginQuorumEpoch(request: RequestChannel.Request): Unit = {
    handle(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  private def handleEndQuorumEpoch(request: RequestChannel.Request): Unit = {
    handle(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  private def handleFetch(request: RequestChannel.Request): Unit = {
    handle(request, response => new FetchResponse(response.asInstanceOf[FetchResponseData]))
  }

  private def handleFetchSnapshot(request: RequestChannel.Request): Unit = {
    handle(request, response => new FetchSnapshotResponse(response.asInstanceOf[FetchSnapshotResponseData]))
  }

  private def handle(
    request: RequestChannel.Request,
    buildResponse: ApiMessage => AbstractResponse
  ): Unit = {
    val requestBody = request.body[AbstractRequest]

    val future = raftManager.handleRequest(
      request.context,
      request.header,
      requestBody.data,
      time.milliseconds()
    )

    future.whenComplete((response, exception) => {
      val res = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(response)
      }
      requestChannel.sendResponse(request, res, None)
    })
  }

}
