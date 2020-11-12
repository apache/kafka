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
import java.util
import java.util.{Collections, Optional}

import com.fasterxml.jackson.databind.node.{DoubleNode, LongNode, ObjectNode, TextNode}
import kafka.network
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.{ClientInformation, ListenerName, NetworkSend}
import org.junit.Test
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.PartitionData
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.easymock.EasyMock.createNiceMock
import org.junit.Assert.assertEquals

import scala.collection.mutable.ArrayBuffer

class RequestConvertToJsonTest {

  @Test
  def testAllRequestTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach { key => {
      val version: Short = key.latestVersion()
      val req = if (key == ApiKeys.PRODUCE) {
        // There's inconsistency with the toStruct schema in ProduceRequest
        // and ProduceRequestDataJsonConverters where the field names don't
        // match so the struct does not have the correct field names. This is
        // a temporary workaround until ProduceRequest starts using ProduceRequestData
        ProduceRequest.Builder.forCurrentMagic(0.toShort, 10000, new util.HashMap[TopicPartition, MemoryRecords]).build()
      } else {
        val struct = ApiMessageType.fromApiKey(key.id).newRequest().toStruct(version)
        AbstractRequest.parseRequest(key, version, struct)
      }
      try {
        RequestConvertToJson.request(req)
      } catch {
        case _ : IllegalStateException => unhandledKeys += key.toString
      }
    }}
    assertEquals("Unhandled request keys", ArrayBuffer.empty, unhandledKeys)
  }

  @Test
  def testAllResponseTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach { key => {
      val version: Short = key.latestVersion()
      val struct = ApiMessageType.fromApiKey(key.id).newResponse().toStruct(version)
      val res = AbstractResponse.parseResponse(key, struct, version)
      try {
        RequestConvertToJson.response(res, version)
      } catch {
        case _ : IllegalStateException => unhandledKeys += key.toString
      }
    }}
    assertEquals("Unhandled response keys", ArrayBuffer.empty, unhandledKeys)
  }

  @Test
  def testFormatOfOffsetsForLeaderEpochRequestNode(): Unit = {
    val partitionDataMap = new util.HashMap[TopicPartition, PartitionData]
    partitionDataMap.put(new TopicPartition("topic1", 0), new PartitionData(Optional.of(0),  1))

    val version: Short = ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion
    val request = OffsetsForLeaderEpochRequest.Builder.forConsumer(partitionDataMap).build(version)
    val actualNode = RequestConvertToJson.request(request)

    val requestData = OffsetForLeaderEpochRequestDataJsonConverter.read(actualNode, version)
    val expectedNode = OffsetForLeaderEpochRequestDataJsonConverter.write(requestData, version, true)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfProduceRequestNode(): Unit = {
    val produceDataMap = new util.HashMap[TopicPartition, MemoryRecords]

    val version: Short = ApiKeys.PRODUCE.latestVersion
    val request = ProduceRequest.Builder.forMagic(2, 0.toShort, 0, produceDataMap, "").build()
    val actualNode = RequestConvertToJson.request(request)

    val requestData = new ProduceRequestData()
    val expectedNode = ProduceRequestDataJsonConverter.write(requestData, version)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfOffsetsForLeaderEpochResponseNode(): Unit = {
    val endOffsetMap = new util.HashMap[TopicPartition, EpochEndOffset]
    endOffsetMap.put(new TopicPartition("topic1", 0), new EpochEndOffset(1, 10L))

    val version: Short = ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion
    val response = new OffsetsForLeaderEpochResponse(endOffsetMap)
    val actualNode = RequestConvertToJson.response(response, version)

    val requestData = OffsetForLeaderEpochResponseDataJsonConverter.read(actualNode, version)
    val expectedNode = OffsetForLeaderEpochResponseDataJsonConverter.write(requestData, version, true)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfProduceResponseNode(): Unit = {
    val responseData = new util.HashMap[TopicPartition, ProduceResponse.PartitionResponse]
    val partResponse = new ProduceResponse.PartitionResponse(Errors.NONE, 10000, RecordBatch.NO_TIMESTAMP, 100, Collections.singletonList(new ProduceResponse.RecordError(3, "Record error")), "Produce failed")
    responseData.put(new TopicPartition("topic1", 0), partResponse)

    val version: Short = ApiKeys.PRODUCE.latestVersion
    val response = new ProduceResponse(responseData)
    val actualNode = RequestConvertToJson.response(response, version)

    val requestData = ProduceResponseDataJsonConverter.read(actualNode, version)
    val expectedNode = ProduceResponseDataJsonConverter.write(requestData, version, true)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testRequestDescMetrics(): Unit = {
    val req = request(new AlterIsrRequest(new AlterIsrRequestData(), 0))
    val byteBuffer = req.body[AbstractRequest].serialize(req.header)
    val send = new NetworkSend(req.context.connectionId, byteBuffer)
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

    val expectedNode = RequestConvertToJson.requestDesc(req.header, req.loggableRequest).asInstanceOf[ObjectNode]
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

    val actualNode = RequestConvertToJson.requestDescMetrics(req.header, res, req.loggableRequest, req.context, req.session,
      totalTimeMs, requestQueueTimeMs, apiLocalTimeMs, apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
      responseSendTimeMs, temporaryMemoryBytes, messageConversionsTimeMs).asInstanceOf[ObjectNode]

    assertEquals(expectedNode, actualNode)
  }

  private def request(req: AbstractRequest): RequestChannel.Request = {
    val buffer = req.serialize(new RequestHeader(req.api, req.version, "client-id", 1))
    val requestContext = newRequestContext(buffer)
    new network.RequestChannel.Request(processor = 1,
      requestContext,
      startTimeNanos = 0,
      createNiceMock(classOf[MemoryPool]),
      buffer,
      createNiceMock(classOf[RequestChannel.Metrics])
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
      new ClientInformation("name", "version"))
  }
}
