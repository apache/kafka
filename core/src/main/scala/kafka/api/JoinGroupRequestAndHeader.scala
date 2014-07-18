/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.nio.ByteBuffer
import kafka.network.{BoundedByteBufferSend, RequestChannel}
import kafka.common.ErrorMapping
import org.apache.kafka.common.requests._
import kafka.network.RequestChannel.Response
import scala.Some

object JoinGroupRequestAndHeader {
  def readFrom(buffer: ByteBuffer): JoinGroupRequestAndHeader = {
    val header = RequestHeader.parse(buffer)
    val body = JoinGroupRequest.parse(buffer)
    new JoinGroupRequestAndHeader(header, body)
  }
}

case class JoinGroupRequestAndHeader(override val header: RequestHeader, override val body: JoinGroupRequest)
  extends GenericRequestOrResponseAndHeader(header, body, RequestKeys.nameForKey(RequestKeys.JoinGroupKey), Some(RequestKeys.JoinGroupKey)) {

  override def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
    val errorResponseHeader = new ResponseHeader(header.correlationId)
    val errorResponseBody = new JoinGroupResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    val errorHeartBeatResponseAndHeader = new JoinGroupResponseAndHeader(errorResponseHeader, errorResponseBody)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorHeartBeatResponseAndHeader)))
  }
}
