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
package unit.kafka.server

import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel

import kafka.api.{PartitionFetchInfo, FetchRequest}
import kafka.common.TopicAndPartition
import kafka.network.RequestChannel.Session
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.server.KafkaApis
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.protocol.{SecurityProtocol, ApiKeys, ProtoUtils}
import org.apache.kafka.common.requests.{RequestSend, RequestHeader, HeartbeatRequest}
import org.junit.Test
import org.junit.Assert.{assertNull, assertNotNull}

/**
 * Unit test for KafkaApis
 */
class KafkaApisTest {
  //TODO: The API version test should use KafkaServerTestHarness with actual clients after we have protocol version control on clients.
  @Test(expected = classOf[UnsupportedVersionException])
  def testValidateRequestVersionForScalaRequests() {
    val fetchRequest = new FetchRequest((ProtoUtils.latestVersion(ApiKeys.FETCH.id) + 1).toShort, 0, "test", -1, 100, 1,
      Map.empty[TopicAndPartition, PartitionFetchInfo])
    val buffer = RequestOrResponseSend.serialize(fetchRequest)
    buffer.rewind()
    val request = RequestChannel.Request(0, "foo", new Session(null, "bar"), buffer, 0L, SecurityProtocol.PLAINTEXT)
    assertNull(request.header)
    assertNull(request.body)
    KafkaApis.validateRequestVersion(request, Option(request.requestObj.asInstanceOf[FetchRequest].versionId))
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def testValidateRequestVersionForJavaRequests() {
    val heartBeatRequest = new HeartbeatRequest("foo", 0, "bar")
    val requestHeader = new RequestHeader(ApiKeys.HEARTBEAT.id, "foo", 0)
    val requestSend = new RequestSend("foo", requestHeader, heartBeatRequest.toStruct)
    val buffer = ByteBuffer.allocate(requestSend.size().toInt)
    val simpleGatheringBytesChannel = new SimpleGatheringBytesChannel(buffer)
    requestSend.writeTo(simpleGatheringBytesChannel)
    // Skip first 4 bytes for the size delimited format and change the API version.
    buffer.putShort(6, (ProtoUtils.latestVersion(ApiKeys.HEARTBEAT.id) + 1).toShort)
    buffer.position(4)
    val request = RequestChannel.Request(0, "foo", new Session(null, "bar"), buffer.slice(), 0L, SecurityProtocol.PLAINTEXT)
    assertNotNull(request.header)
    assertNull(request.body)
    KafkaApis.validateRequestVersion(request, None)
  }

  // This class is written because the NetworkSend can only write to a GatheringByteChannel. This class should be
  // removed after the unit test start to use KafkaTestHarness.
  private class SimpleGatheringBytesChannel(buffer: ByteBuffer) extends GatheringByteChannel {
    override def write(buffers: Array[ByteBuffer], offset: Int, length: Int) : Long = 0L

    override def write(buffers: Array[ByteBuffer]): Long = {
      val positionBeforeWrite = buffer.position()
      buffers foreach buffer.put
      buffer.position() - positionBeforeWrite
    }

    override def write(byteBuffer: ByteBuffer): Int = {
      val positionBeforeWrite = buffer.position()
      buffer.put(byteBuffer)
      buffer.position() - positionBeforeWrite
    }

    override def isOpen: Boolean = true

    override def close() { }
  }
}
