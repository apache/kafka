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
import kafka.api.ApiUtils._
import collection.mutable.ListBuffer
import kafka.network.{BoundedByteBufferSend, RequestChannel}
import kafka.common.{TopicAndPartition, ErrorMapping}
import kafka.network.RequestChannel.Response
import kafka.utils.Logging

object ControlledShutdownRequest extends Logging {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): ControlledShutdownRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val brokerId = buffer.getInt
    new ControlledShutdownRequest(versionId, correlationId, brokerId)
  }
}

case class ControlledShutdownRequest(val versionId: Short,
                                     val correlationId: Int,
                                     val brokerId: Int)
  extends RequestOrResponse(Some(RequestKeys.ControlledShutdownKey)){

  def this(correlationId: Int, brokerId: Int) =
    this(ControlledShutdownRequest.CurrentVersion, correlationId, brokerId)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    buffer.putInt(brokerId)
  }

  def sizeInBytes(): Int = {
    2 +  /* version id */
      4 + /* correlation id */
      4 /* broker id */
  }

  override def toString(): String = {
    describe(true)
  }

  override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val errorResponse = ControlledShutdownResponse(correlationId, ErrorMapping.codeFor(e.getCause.asInstanceOf[Class[Throwable]]), Set.empty[TopicAndPartition])
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
  }

  override def describe(details: Boolean = false): String = {
    val controlledShutdownRequest = new StringBuilder
    controlledShutdownRequest.append("Name: " + this.getClass.getSimpleName)
    controlledShutdownRequest.append("; Version: " + versionId)
    controlledShutdownRequest.append("; CorrelationId: " + correlationId)
    controlledShutdownRequest.append("; BrokerId: " + brokerId)
    controlledShutdownRequest.toString()
  }
}