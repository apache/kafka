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

import java.nio.ByteBuffer
import java.util.Properties

import kafka.log.LogConfig
import kafka.message.ZStdCompressionCodec
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientRDMARequest, ExclusiveRdmaClient, ProduceRDMAWriteRequest}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse, RDMAProduceAddressRequest, RDMAProduceAddressResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  protected override def propertyOverrides(props: Properties): Unit = {
    props.put("log.segment.bytes", "1073741824")
    props.put("log.preallocate","true")
  }

  @Test
  def testSimpleProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> memoryRecords)
      val produceResponse = sendProduceRequest(leader,
          ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, partitionResponse.error)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.logAppendTime)
      partitionResponse
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    sendAndCheck(MemoryRecords.withRecords(CompressionType.GZIP,
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value1".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value2".getBytes)), 1)
  }

  @Test
  def testRdmaProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> memoryRecords)
      val produceResponse = sendProduceRequest(leader,
        ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, partitionResponse.error)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.logAppendTime)
      partitionResponse
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    val topicPartition = new TopicPartition("topic", partition)
    val tplist = List(topicPartition).asJava

    val toUpdate: List[TopicPartition]  = List()

    val request = new RDMAProduceAddressRequest.Builder(-1,tplist,toUpdate.asJava,3000).build()
    val addressResponse = sendProduceAddressRequest(leader,request)

    val clientAddress = addressResponse.responses().get(topicPartition)
    assert(clientAddress.error == Errors.NONE)

    val rdmabrokers = servers.map(
      b => b.config.brokerId.asInstanceOf[java.lang.Integer] -> b.rdmaserver.getPort().asInstanceOf[java.lang.Integer] ).toMap.asJava


    val rdmaclient = new ExclusiveRdmaClient("testProducer", new LogContext("[RDMA test client]"),
      null,100,100,1,1)

    val leaderRdmaPort = rdmabrokers.get(leader)

    val destination = new Node( leader,"10.152.8.96",leaderRdmaPort )
    rdmaclient.connect( destination)
    assert(clientAddress.error == Errors.NONE)


    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))

    val length = records.sizeInBytes()
    val buffer  = ByteBuffer.allocateDirect(length)

    buffer.put(records.buffer() )
    buffer.rewind()

    val mr = rdmaclient.MemReg(buffer)

    val lkey = mr.getLkey

    var builder = new ProduceRDMAWriteRequest(clientAddress.address,clientAddress.rkey,
      length,buffer,lkey,
      clientAddress.immdata)
    var req = new ClientRDMARequest(destination.idString(),1,builder,"testProducer",0,false,false,null)

    rdmaclient.send(req,0)
    rdmaclient.poll(0,0)

    /* val addressResponse2 = sendProduceAddressRequest(leader,request)

    val clientAddress2 = addressResponse2.responses().get(topicPartition)
    assert(clientAddress2.error == Errors.NONE)

    assert(clientAddress2.rkey == clientAddress.rkey )
    //assert(clientAddress2.length == clientAddress.length - length )
    //assert(clientAddress2.immdata == clientAddress.immdata )

    */

    builder = new ProduceRDMAWriteRequest(clientAddress.address+length,clientAddress.rkey,
      length,buffer,lkey,
      clientAddress.immdata)

    req = new ClientRDMARequest(destination.idString(),1,builder,"testProducer",0,false,false,null)

    rdmaclient.send(req,0)
    rdmaclient.poll(0,0)

  }

  @Test
  def testProduceToNonReplica() {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the produce request to the non-replica
    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceRequest = ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()

    val produceResponse = sendProduceRequest(nonReplicaId, produceRequest)
    assertEquals(1, produceResponse.responses.size)
    assertEquals(Errors.NOT_LEADER_FOR_PARTITION, produceResponse.responses.asScala.head._2.error)
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, leader) if leader != -1 => (partition, leader)
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val produceResponse = sendProduceRequest(leader, 
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
  }

  @Test
  def testZSTDProduceRequest(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic compressed with ZSTD
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CompressionTypeProp, ZStdCompressionCodec.name)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.ZSTD,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    // produce request with v7: works fine!
    val res1 = sendProduceRequest(leader,
      new ProduceRequest.Builder(7, 7, -1, 3000, partitionRecords.asJava, null).build())
    val (tp1, partitionResponse1) = res1.responses.asScala.head
    assertEquals(topicPartition, tp1)
    assertEquals(Errors.NONE, partitionResponse1.error)
    assertEquals(0, partitionResponse1.baseOffset)
    assertEquals(-1, partitionResponse1.logAppendTime)

    // produce request with v3: returns Errors.UNSUPPORTED_COMPRESSION_TYPE.
    val res2 = sendProduceRequest(leader,
      new ProduceRequest.Builder(3, 3, -1, 3000, partitionRecords.asJava, null)
        .buildUnsafe(3))
    val (tp2, partitionResponse2) = res2.responses.asScala.head
    assertEquals(topicPartition, tp2)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, partitionResponse2.error)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCE, destination = brokerSocketServer(leaderId))
    ProduceResponse.parse(response, request.version)
  }

  private def sendProduceAddressRequest(leaderId: Int, request: RDMAProduceAddressRequest): RDMAProduceAddressResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCER_RDMA_REGISTER, destination = brokerSocketServer(leaderId))
    RDMAProduceAddressResponse.parse(response, request.version)
  }

}
