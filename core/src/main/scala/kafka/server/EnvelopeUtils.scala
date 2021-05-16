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

package kafka.server

import java.net.{InetAddress, UnknownHostException}
import java.nio.ByteBuffer

import kafka.network.RequestChannel
import org.apache.kafka.common.errors.{InvalidRequestException, PrincipalDeserializationException, UnsupportedVersionException}
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.requests.{EnvelopeRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.compat.java8.OptionConverters._

object EnvelopeUtils {
  def handleEnvelopeRequest(
    request: RequestChannel.Request,
    requestChannelMetrics: RequestChannel.Metrics,
    handler: RequestChannel.Request => Unit): Unit = {
    val envelope = request.body[EnvelopeRequest]
    val forwardedPrincipal = parseForwardedPrincipal(request.context, envelope.requestPrincipal)
    val forwardedClientAddress = parseForwardedClientAddress(envelope.clientAddress)

    val forwardedRequestBuffer = envelope.requestData.duplicate()
    val forwardedRequestHeader = parseForwardedRequestHeader(forwardedRequestBuffer)

    val forwardedApi = forwardedRequestHeader.apiKey
    if (!forwardedApi.forwardable) {
      throw new InvalidRequestException(s"API $forwardedApi is not enabled or is not eligible for forwarding")
    }

    val forwardedContext = new RequestContext(
      forwardedRequestHeader,
      request.context.connectionId,
      forwardedClientAddress,
      forwardedPrincipal,
      request.context.listenerName,
      request.context.securityProtocol,
      ClientInformation.EMPTY,
      request.context.fromPrivilegedListener
    )

    val forwardedRequest = parseForwardedRequest(
      request,
      forwardedContext,
      forwardedRequestBuffer,
      requestChannelMetrics
    )
    handler(forwardedRequest)
  }

  private def parseForwardedClientAddress(
    address: Array[Byte]
  ): InetAddress = {
    try {
      InetAddress.getByAddress(address)
    } catch {
      case e: UnknownHostException =>
        throw new InvalidRequestException("Failed to parse client address from envelope", e)
    }
  }

  private def parseForwardedRequest(
    envelope: RequestChannel.Request,
    forwardedContext: RequestContext,
    buffer: ByteBuffer,
    requestChannelMetrics: RequestChannel.Metrics
  ): RequestChannel.Request = {
    try {
      new RequestChannel.Request(
        processor = envelope.processor,
        context = forwardedContext,
        startTimeNanos = envelope.startTimeNanos,
        envelope.memoryPool,
        buffer,
        requestChannelMetrics,
        Some(envelope)
      )
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException(s"Failed to parse forwarded request " +
          s"with header ${forwardedContext.header}", e)
    }
  }

  private def parseForwardedRequestHeader(
    buffer: ByteBuffer
  ): RequestHeader = {
    try {
      RequestHeader.parse(buffer)
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException("Failed to parse request header from envelope", e)
    }
  }

  private def parseForwardedPrincipal(
    envelopeContext: RequestContext,
    principalBytes: Array[Byte]
  ): KafkaPrincipal = {
    envelopeContext.principalSerde.asScala match {
      case Some(serde) =>
        try {
          serde.deserialize(principalBytes)
        } catch {
          case e: Exception =>
            throw new PrincipalDeserializationException("Failed to deserialize client principal from envelope", e)
        }

      case None =>
        throw new PrincipalDeserializationException("Could not deserialize principal since " +
          "no `KafkaPrincipalSerde` has been defined")
    }
  }
}
