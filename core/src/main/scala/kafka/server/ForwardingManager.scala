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

import java.nio.ByteBuffer

import kafka.network.RequestChannel
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, EnvelopeRequest, EnvelopeResponse, RequestHeader}
import org.apache.kafka.common.utils.Time

import scala.compat.java8.OptionConverters._
import scala.concurrent.TimeoutException

class ForwardingManager(channelManager: BrokerToControllerChannelManager,
                        time: Time,
                        retryTimeoutMs: Long) extends Logging {

  def forwardRequest(request: RequestChannel.Request,
                     responseCallback: AbstractResponse => Unit): Unit = {
    val principalSerde = request.context.principalSerde.asScala.getOrElse(
      throw new IllegalArgumentException(s"Cannot deserialize principal from request $request " +
        "since there is no serde defined")
    )
    val serializedPrincipal = principalSerde.serialize(request.context.principal)
    val forwardRequestBuffer = request.buffer.duplicate()
    forwardRequestBuffer.flip()
    val envelopeRequest = new EnvelopeRequest.Builder(
      forwardRequestBuffer,
      serializedPrincipal,
      request.context.clientAddress.getAddress
    )

    class ForwardingResponseHandler extends ControllerRequestCompletionHandler {
      override def onComplete(clientResponse: ClientResponse): Unit = {
        val envelopeResponse = clientResponse.responseBody.asInstanceOf[EnvelopeResponse]
        val envelopeError = envelopeResponse.error()
        val requestBody = request.body[AbstractRequest]

        val response = if (envelopeError != Errors.NONE) {
          // An envelope error indicates broker misconfiguration (e.g. the principal serde
          // might not be defined on the receiving broker). In this case, we do not return
          // the error directly to the client since it would not be expected. Instead we
          // return `UNKNOWN_SERVER_ERROR` so that the user knows that there is a problem
          // on the broker.
          debug(s"Forwarded request $request failed with an error in the envelope response $envelopeError")
          requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)
        } else {
          parseResponse(envelopeResponse.responseData, requestBody, request.header)
        }
        responseCallback(response)
      }

      override def onTimeout(): Unit = {
        debug(s"Forwarding of the request $request failed due to timeout exception")
        val response = request.body[AbstractRequest].getErrorResponse(new TimeoutException)
        responseCallback(response)
      }
    }

    val currentTime = time.milliseconds()
    val deadlineMs =
      if (Long.MaxValue - currentTime < retryTimeoutMs)
        Long.MaxValue
      else
        currentTime + retryTimeoutMs

    channelManager.sendRequest(envelopeRequest, new ForwardingResponseHandler, deadlineMs)
  }

  private def parseResponse(
    buffer: ByteBuffer,
    request: AbstractRequest,
    header: RequestHeader
  ): AbstractResponse = {
    try {
      AbstractResponse.parseResponse(buffer, header)
    } catch {
      case e: Exception =>
        error(s"Failed to parse response from envelope for request with header $header", e)
        request.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)
    }
  }

}
