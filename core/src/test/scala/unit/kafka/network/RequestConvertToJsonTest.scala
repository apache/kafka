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

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import com.fasterxml.jackson.databind.node.{BooleanNode, DoubleNode, JsonNodeFactory, LongNode, ObjectNode, TextNode}
import kafka.network
import kafka.network.RequestConvertToJson.requestHeaderNode
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.{ClientInformation, ListenerName, NetworkSend}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

import java.util.Collections
import scala.collection.mutable.ArrayBuffer

class RequestConvertToJsonTest {

  @Test
  def testAllRequestTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach { key => {
      val version: Short = key.latestVersion()
      val message = key match {
        case ApiKeys.DESCRIBE_ACLS =>
          ApiMessageType.fromApiKey(key.id).newRequest().asInstanceOf[DescribeAclsRequestData]
            .setPatternTypeFilter(1).setResourceTypeFilter(1).setPermissionType(1).setOperation(1)
        case _ =>
          ApiMessageType.fromApiKey(key.id).newRequest()
      }

      val bytes = MessageUtil.toByteBuffer(message, version)
      val req = AbstractRequest.parseRequest(key, version, bytes).request
      try {
        RequestConvertToJson.request(req)
      } catch {
        case _ : IllegalStateException => unhandledKeys += key.toString
      }
    }}
    assertEquals(ArrayBuffer.empty, unhandledKeys, "Unhandled request keys")
  }

  @Test
  def testAllApiVersionsResponseHandled(): Unit = {

    ApiKeys.values().foreach { key => {
      val unhandledVersions = ArrayBuffer[java.lang.Short]()
      key.allVersions().forEach { version => {
        val message = key match {
          // Specify top-level error handling for verifying compatibility across versions
          case ApiKeys.DESCRIBE_LOG_DIRS =>
            ApiMessageType.fromApiKey(key.id).newResponse().asInstanceOf[DescribeLogDirsResponseData]
              .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
          case _ =>
            ApiMessageType.fromApiKey(key.id).newResponse()
        }

        val bytes = MessageUtil.toByteBuffer(message, version)
        val response = AbstractResponse.parseResponse(key, bytes, version)
        try {
          RequestConvertToJson.response(response, version)
        } catch {
          case _ : IllegalStateException => unhandledVersions += version
        }}
      }
      assertEquals(ArrayBuffer.empty, unhandledVersions, s"API: ${key.toString} - Unhandled request versions")
    }}
  }

  @Test
  def testAllResponseTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach { key => {
      val version: Short = key.latestVersion()
      val message = ApiMessageType.fromApiKey(key.id).newResponse()
      val bytes = MessageUtil.toByteBuffer(message, version)
      val res = AbstractResponse.parseResponse(key, bytes, version)
      try {
        RequestConvertToJson.response(res, version)
      } catch {
        case _ : IllegalStateException => unhandledKeys += key.toString
      }
    }}
    assertEquals(ArrayBuffer.empty, unhandledKeys, "Unhandled response keys")
  }

  @Test
  def testRequestHeaderNode(): Unit = {
    val alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0)
    val req = request(alterIsrRequest)
    val header = req.header

    val expectedNode = RequestHeaderDataJsonConverter.write(header.data, header.headerVersion, false).asInstanceOf[ObjectNode]
    expectedNode.set("requestApiKeyName", new TextNode(header.apiKey.toString))

    val actualNode = RequestConvertToJson.requestHeaderNode(header)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testRequestHeaderNodeWithDeprecatedApiVersion(): Unit = {
    val fetchRequest = FetchRequest.Builder.forConsumer(0, 0, 0, Collections.emptyMap()).build(0);
    val req = request(fetchRequest)
    val header = req.header

    val expectedNode = RequestHeaderDataJsonConverter.write(header.data, header.headerVersion, false).asInstanceOf[ObjectNode]
    expectedNode.set("requestApiKeyName", new TextNode(header.apiKey.toString))
    expectedNode.set("requestApiVersionDeprecated", BooleanNode.TRUE)

    val actualNode = RequestConvertToJson.requestHeaderNode(header)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testClientInfoNode(): Unit = {
    val clientInfo = new ClientInformation("name", "1")

    val expectedNode = new ObjectNode(JsonNodeFactory.instance)
    expectedNode.set("softwareName", new TextNode(clientInfo.softwareName))
    expectedNode.set("softwareVersion", new TextNode(clientInfo.softwareVersion))

    val actualNode = RequestConvertToJson.clientInfoNode(clientInfo)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testRequestDesc(): Unit = {
    val alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0)
    val req = request(alterIsrRequest)

    val expectedNode = new ObjectNode(JsonNodeFactory.instance)
    expectedNode.set("isForwarded", if (req.isForwarded) BooleanNode.TRUE else BooleanNode.FALSE)
    expectedNode.set("requestHeader", requestHeaderNode(req.header))
    expectedNode.set("request", req.requestLog.getOrElse(new TextNode("")))

    val actualNode = RequestConvertToJson.requestDesc(req.header, req.requestLog, req.isForwarded)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testRequestDescMetrics(): Unit = {
    val alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0)
    val req = request(alterIsrRequest)
    val send = new NetworkSend(req.context.connectionId, alterIsrRequest.toSend(req.header))
    val headerLog = RequestConvertToJson.requestHeaderNode(req.header)
    val res = new RequestChannel.SendResponse(req, send, Some(headerLog), None)

    val totalTimeMs = 1
    val requestQueueTimeMs = 2
    val apiLocalTimeMs = 3
    val apiRemoteTimeMs = 4
    val apiThrottleTimeMs = 5
    val responseQueueTimeMs = 6
    val responseSendTimeMs = 7
    val temporaryMemoryBytes = 8
    val messageConversionsTimeMs = 9

    val expectedNode = RequestConvertToJson.requestDesc(req.header, req.requestLog, req.isForwarded).asInstanceOf[ObjectNode]
    expectedNode.set("response", res.responseLog.getOrElse(new TextNode("")))
    expectedNode.set("connection", new TextNode(req.context.connectionId))
    expectedNode.set("totalTimeMs", new DoubleNode(totalTimeMs))
    expectedNode.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs))
    expectedNode.set("localTimeMs", new DoubleNode(apiLocalTimeMs))
    expectedNode.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs))
    expectedNode.set("throttleTimeMs", new LongNode(apiThrottleTimeMs))
    expectedNode.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs))
    expectedNode.set("sendTimeMs", new DoubleNode(responseSendTimeMs))
    expectedNode.set("securityProtocol", new TextNode(req.context.securityProtocol.toString))
    expectedNode.set("principal", new TextNode(req.session.principal.toString))
    expectedNode.set("listener", new TextNode(req.context.listenerName.value))
    expectedNode.set("clientInformation", RequestConvertToJson.clientInfoNode(req.context.clientInformation))
    expectedNode.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes))
    expectedNode.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs))

    val actualNode = RequestConvertToJson.requestDescMetrics(req.header, req.requestLog, res.responseLog, req.context, req.session, req.isForwarded,
      totalTimeMs, requestQueueTimeMs, apiLocalTimeMs, apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
      responseSendTimeMs, temporaryMemoryBytes, messageConversionsTimeMs).asInstanceOf[ObjectNode]

    assertEquals(expectedNode, actualNode)
  }

  def request(req: AbstractRequest): RequestChannel.Request = {
    val buffer = req.serializeWithHeader(new RequestHeader(req.apiKey, req.version, "client-id", 1))
    val requestContext = newRequestContext(buffer)
    new network.RequestChannel.Request(processor = 1,
      requestContext,
      startTimeNanos = 0,
      mock(classOf[MemoryPool]),
      buffer,
      mock(classOf[RequestChannel.Metrics])
    )
  }

  private def newRequestContext(buffer: ByteBuffer): RequestContext = {
    new RequestContext(
      RequestHeader.parse(buffer),
      "connection-id",
      InetAddress.getLoopbackAddress,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT,
      new ClientInformation("name", "version"),
      false)
  }
}
