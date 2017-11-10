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

import org.apache.kafka.common.protocol.ApiKeys

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
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

@deprecated("This object has been deprecated and will be removed in a future release.", "1.0.0")
case class GroupCoordinatorRequest(group: String,
                                   versionId: Short = GroupCoordinatorRequest.CurrentVersion,
                                   correlationId: Int = 0,
                                   clientId: String = GroupCoordinatorRequest.DefaultClientId)
  extends RequestOrResponse(Some(ApiKeys.FIND_COORDINATOR.id)) {

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
