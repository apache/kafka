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

import java.nio.ByteBuffer
import java.util
import java.util.{Collections, Optional}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message._
import org.junit.Test
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, TimestampType}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.PartitionData
import org.apache.kafka.common.requests._
import org.junit.Assert.assertEquals

import scala.collection.mutable.ArrayBuffer

class RequestConvertToJsonTest {

  @Test
  def testAllRequestTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach(key => {
      val version: Short = 0
      var req: AbstractRequest = null
      if (key == ApiKeys.PRODUCE) {
        // There's inconsistency with the toStruct schema in ProduceRequest
        // and ProduceRequestDataJsonConverters where the field names don't
        // match so the struct does not have the correct field names. This is
        // a temporary workaround until ProduceRequest starts using ProduceRequestData
        req = ProduceRequest.Builder.forCurrentMagic(0.toShort, 10000, new util.HashMap[TopicPartition, MemoryRecords]()).build()
      } else {
        val struct = ApiMessageType.fromApiKey(key.id).newRequest().toStruct(version)
        req = AbstractRequest.parseRequest(key, version, struct)
      }
      try {
        RequestConvertToJson.request(req, false)
      } catch {
        case _ : AssertionError => unhandledKeys += key.toString
      }
    })
    assertEquals("Unhandled request keys", ArrayBuffer.empty, unhandledKeys)
  }

  @Test
  def testAllResponseTypesHandled(): Unit = {
    val unhandledKeys = ArrayBuffer[String]()
    ApiKeys.values().foreach(key => {
      val version: Short = 0
      val struct = ApiMessageType.fromApiKey(key.id).newResponse().toStruct(version)
      val res = AbstractResponse.parseResponse(key, struct, version)
      try {
        RequestConvertToJson.response(res, version)
      } catch {
        case _ : AssertionError => unhandledKeys += key.toString
      }
    })
    assertEquals("Unhandled response keys", ArrayBuffer.empty, unhandledKeys)
  }

  @Test
  def testFormatOfOffsetsForLeaderEpochRequestNode(): Unit = {
    val partitionDataMap = new util.HashMap[TopicPartition, PartitionData]
    partitionDataMap.put(new TopicPartition("topic1", 0), new PartitionData(Optional.of(0),  1))

    val version: Short = 3
    val request = OffsetsForLeaderEpochRequest.Builder.forConsumer(partitionDataMap).build(version)
    val actualNode = RequestConvertToJson.request(request, false)

    val requestData = OffsetForLeaderEpochRequestDataJsonConverter.read(actualNode, version)
    val expectedNode = OffsetForLeaderEpochRequestDataJsonConverter.write(requestData, version)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfProduceRequestNode(): Unit = {
    val buffer = ByteBuffer.allocate(256)
    val builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0L)
    builder.append(10L, null, "a".getBytes)
    val produceData = new util.HashMap[TopicPartition, MemoryRecords]
    produceData.put(new TopicPartition("test", 0), builder.build)

    val version: Short = 3
    val request = ProduceRequest.Builder.forCurrentMagic(1.toShort, 5000, produceData).build(version)
    val actualNode = RequestConvertToJson.request(request, false)

    val requestData = ProduceRequestDataJsonConverter.read(actualNode, version)
    val expectedNode = ProduceRequestDataJsonConverter.write(requestData, version)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfOffsetsForLeaderEpochResponseNode(): Unit = {
    val endOffsetMap = new util.HashMap[TopicPartition, EpochEndOffset]
    endOffsetMap.put(new TopicPartition("topic1", 0), new EpochEndOffset(1, 10L))

    val version: Short = 3
    val response = new OffsetsForLeaderEpochResponse(endOffsetMap)
    val actualNode = RequestConvertToJson.response(response, version)

    val requestData = OffsetForLeaderEpochResponseDataJsonConverter.read(actualNode, version)
    val expectedNode = OffsetForLeaderEpochResponseDataJsonConverter.write(requestData, version)

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def testFormatOfProduceResponseNode(): Unit = {
    val responseData = new util.HashMap[TopicPartition, ProduceResponse.PartitionResponse]
    val partResponse = new ProduceResponse.PartitionResponse(Errors.NONE, 10000, RecordBatch.NO_TIMESTAMP, 100, Collections.singletonList(new ProduceResponse.RecordError(3, "Record error")), "Produce failed")
    responseData.put(new TopicPartition("topic1", 0), partResponse)

    val version: Short = 3
    val response = new ProduceResponse(responseData)
    val actualNode = RequestConvertToJson.response(response, version)

    val requestData = ProduceResponseDataJsonConverter.read(actualNode, version)
    val expectedNode = ProduceResponseDataJsonConverter.write(requestData, version)

    assertEquals(expectedNode, actualNode)
  }
}
