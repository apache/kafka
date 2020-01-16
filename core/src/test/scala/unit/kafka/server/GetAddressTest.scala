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


import java.util.Properties


import kafka.server.{BaseRequestTest, KafkaServer}
import kafka.network.SocketServer
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests._
import org.junit.Test
import sun.nio.ch.DirectBuffer

import scala.collection.JavaConverters._

class GetAddressTest extends BaseRequestTest {

  private lazy val time = new MockTime

  protected override def numBrokers = 1

  protected override def brokerTime(brokerId: Int) = time

  protected override def propertyOverrides(props: Properties): Unit = {
    props.put("log.flush.interval.messages", "1")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.retention.check.interval.ms", (5 * 1000 * 60).toString)
    props.put("log.segment.bytes", "1073741824")
    props.put("log.preallocate","true")
  }


  @Test
  def testConsumerGetAddress() {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)

    createTopic(topic, 1, 1)

    val logManager = server.getLogManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
                  "Log for partition [topic,0] should be created")
    val log = logManager.getLog(topicPartition).get

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    log.onHighWatermarkIncremented(log.logEndOffsetMetadata)
    log.maybeIncrementLogStartOffset(3)

    val startoffset = log.logStartOffset
    val res = log.fetchAddressByOffset(startoffset)
    val res2 = log.fetchAddressByOffset(startoffset)

    assert(res.get.bytebuffer.asInstanceOf[DirectBuffer].address() == res2.get.bytebuffer.asInstanceOf[DirectBuffer].address())

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = RDMAConsumeAddressRequest.Builder.forConsumer()
      .setTargetOffsets(Map(topicPartition ->
        new RDMAConsumeAddressRequest.PartitionData(startoffset)).asJava).build()

    val clientAddress = sendGetAddressRequest(request).responseData.get(topicPartition)


    val requestToForget = RDMAConsumeAddressRequest.Builder.forConsumer()
      .setToForgetOffsets(Map(topicPartition -> new java.lang.Long(0L) ).asJava).build()

    sendGetAddressRequest(requestToForget).responseData.get(topicPartition)


    assert(res.get.endPosition == clientAddress.writtenPosition)
    assert(res.get.baseOffset == clientAddress.baseOffset)
    assert(res.get.bytebuffer.asInstanceOf[DirectBuffer].address() == clientAddress.address)
  }


  private def server: KafkaServer = servers.head

  private def sendGetAddressRequest(request: RDMAConsumeAddressRequest, destination: Option[SocketServer] = None): RDMAConsumeAddressResponse = {
    val response = connectAndSend(request, ApiKeys.CONSUMER_RDMA_REGISTER, destination = destination.getOrElse(anySocketServer))
    RDMAConsumeAddressResponse.parse(response, request.version)
  }

  private def sendProduceGetAddressRequest(request: RDMAProduceAddressRequest, destination: Option[SocketServer] = None): RDMAProduceAddressResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCER_RDMA_REGISTER, destination = destination.getOrElse(anySocketServer))
    RDMAProduceAddressResponse.parse(response, request.version)
  }

  private def sendProduceRequest(request: ProduceRequest, destination: Option[SocketServer] = None): ProduceResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCE, destination = destination.getOrElse(anySocketServer))
    ProduceResponse.parse(response, request.version)
  }

  @Test
  def testProduserGetAddress() {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)

    createTopic(topic, 1, 1)

    val logManager = server.getLogManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
      "Log for partition [topic,0] should be created")
    val log = logManager.getLog(topicPartition).get

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    log.onHighWatermarkIncremented(log.logEndOffsetMetadata)
    log.maybeIncrementLogStartOffset(3)

    val res = log.fetchAddress(false)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")

    val tplist = List(topicPartition).asJava
    val toUpdate: List[TopicPartition]  = List()
    //val targetTimes = Map(tp -> new RDMAProduceAddressRequest(startOffset))
    val request = new RDMAProduceAddressRequest.Builder(0,tplist,toUpdate.asJava,0).build()


    val clientAddress = sendProduceGetAddressRequest(request).responses().get(topicPartition)


    assert(res.bytebuffer.asInstanceOf[DirectBuffer].address()+res.startPosition == clientAddress.address)
    assert(res.offset == clientAddress.offset)


    val res2 = log.fetchAddress(true)

    val clientAddress2 = sendProduceGetAddressRequest(request).responses().get(topicPartition)

    assert(res2.bytebuffer.asInstanceOf[DirectBuffer].address()+res2.startPosition == clientAddress2.address)
    assert(res2.offset == clientAddress2.offset)

    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes, "value".getBytes))
    val partitionRecords = Map(topicPartition -> records)

    val size2= records.validBytes()

    val res1 = sendProduceRequest(new ProduceRequest.Builder(7, 7, -1, 3000, partitionRecords.asJava, null).build())
    val (tp1, partitionResponse1) = res1.responses.asScala.head

    val res3 = log.fetchAddress(false)
    assert(res3.startPosition == size2)

  }



}
