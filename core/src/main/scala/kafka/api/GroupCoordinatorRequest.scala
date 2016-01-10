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

package kafka.api

import java.nio.ByteBuffer

import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

object GroupCoordinatorRequest {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer) = {
    // envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = ApiUtils.readShortString(buffer)

    // request
    val group = ApiUtils.readShortString(buffer)
    GroupCoordinatorRequest(group, versionId, correlationId, clientId)
  }

}

case class GroupCoordinatorRequest(group: String,
                                   versionId: Short = GroupCoordinatorRequest.CurrentVersion,
                                   correlationId: Int = 0,
                                   clientId: String = GroupCoordinatorRequest.DefaultClientId)
  extends RequestOrResponse(Some(ApiKeys.GROUP_COORDINATOR.id)) {

  def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    ApiUtils.shortStringLength(clientId) +
    ApiUtils.shortStringLength(group)

  def writeTo(buffer: ByteBuffer) {
    // envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    ApiUtils.writeShortString(buffer, clientId)

    // consumer metadata request
    ApiUtils.writeShortString(buffer, group)
  }

  override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    // return ConsumerCoordinatorNotAvailable for all uncaught errors
    val errorResponse = GroupCoordinatorResponse(None, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, correlationId)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)))
  }

  def describe(details: Boolean) = {
    val consumerMetadataRequest = new StringBuilder
    consumerMetadataRequest.append("Name: " + this.getClass.getSimpleName)
    consumerMetadataRequest.append("; Version: " + versionId)
    consumerMetadataRequest.append("; CorrelationId: " + correlationId)
    consumerMetadataRequest.append("; ClientId: " + clientId)
    consumerMetadataRequest.append("; Group: " + group)
    consumerMetadataRequest.toString()
  }
}