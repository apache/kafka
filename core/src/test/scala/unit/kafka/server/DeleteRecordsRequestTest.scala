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

import kafka.network.SocketServer
import kafka.server.BaseRequestTest
import kafka.utils._
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{ApiError, DeleteRecordsRequest, DeleteRecordsResponse}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert.{assertEquals, _}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class DeleteRecordsRequestTest extends BaseRequestTest {

  val topicPartition = new TopicPartition("topic", 0)

  val topicPartition2 = new TopicPartition("topic2", 0)

  protected var consumer: Consumer[Array[Byte], Array[Byte]] = null

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.createTopic(zkUtils, topicPartition.topic, Map(0->Seq(1)), servers)
    TestUtils.createTopic(zkUtils, topicPartition2.topic, Map(0->Seq(1)), servers)
    // create producer
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5, keySerializer = new ByteArraySerializer, valueSerializer = new ByteArraySerializer)
    for (i <- 0 to 10) {
      producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition, new Array[Byte](i), new Array[Byte](i))).get
      producer.send(new ProducerRecord(topicPartition2.topic, topicPartition2.partition, new Array[Byte](i), new Array[Byte](i))).get
    }
    producer.close()

    consumer = TestUtils.createNewConsumer(brokerList = TestUtils.getBrokerListStrFromServers(servers), securityProtocol = SecurityProtocol.PLAINTEXT)

    TestUtils.waitUntilTrue(() => {
      val endOffsets = consumer.endOffsets(Set(topicPartition, topicPartition2).asJava)
      endOffsets.get(topicPartition) == 11 && endOffsets.get(topicPartition2) == 11
    }, "Expected to get end offset of 11 for each partition")

  }

  @After
  override def tearDown() = {
    consumer.close()
    super.tearDown()
  }

  @Test
  def testValidDeleteRecordsRequests() {
    val timeout = 10000

    // Single topic: validateOnly
    validateValidDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout, Map[TopicPartition, java.lang.Long](
      topicPartition -> 2L).asJava, true).build())
    // Multi topic: validateOnly
    validateValidDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout, Map[TopicPartition, java.lang.Long](
      topicPartition -> 1L, topicPartition2 -> 2L).asJava, true).build())

    // Single topic
    validateValidDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout, Map[TopicPartition, java.lang.Long](
      topicPartition -> 5L).asJava, false).build())
    // Multi topic
    validateValidDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout, Map[TopicPartition, java.lang.Long](
      topicPartition -> 7L, topicPartition2 -> 3L).asJava, false).build())

  }

  protected def validateValidDeleteRecordsRequests(request: DeleteRecordsRequest): Unit = {

    val priorOffset = consumer.beginningOffsets(Set(topicPartition).asJava).get(topicPartition)

    val response = sendDeleteRecordsRequest(request)
    request.partitionOffsets().asScala.foreach{ case (tp, offset) =>
      assertTrue(s"Error with $tp: ${response.responses.get(tp).error}", response.responses.get(tp).error.isSuccess)}

    if (!request.validateOnly) {
      request.partitionOffsets.asScala.foreach { case (tp, offset) =>
        assertEquals(request.partitionOffsets.get(tp), response.responses.get(tp).lowWatermark)
        validateRecordsWereDeleted(tp, offset)
      }
    } else {
      // validate partitions were not deleted
      val postOffset = consumer.beginningOffsets(Set(topicPartition).asJava).get(topicPartition)
      assertEquals("Expected that no records were deleted", priorOffset, postOffset)
    }
  }

  @Test
  def testErrorDeleteRecordsRequests() {
    val timeout = 30000
    val timeoutTopic = new TopicPartition("invalid-timeout", 0)
    val doesNotExist = new TopicPartition("does-not-exist", 0)
    // Basic
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](doesNotExist -> 101L).asJava, false).build(),
      Map(doesNotExist -> new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, null)))

    // Partial
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](
      topicPartition -> 5L,
      doesNotExist -> 101L).asJava, false).build(),
      Map(
        topicPartition -> ApiError.NONE,
        doesNotExist -> new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, null)
      )
    )

    // offset too large
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](topicPartition -> 101L).asJava, false).build(),
      Map(topicPartition -> new ApiError(Errors.OFFSET_OUT_OF_RANGE,
        "Cannot increment the log start offset to 101 of partition topic-0 since it is larger than the high watermark 11")))

    // offset invalid
    validateErrorDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](topicPartition -> -101L).asJava, false).build(),
      Map(topicPartition -> new ApiError(Errors.OFFSET_OUT_OF_RANGE,
        "The offset -101 for partition topic-0 is not valid")))

    // TODO this is valid, but should it be?
    /*validateValidDeleteRecordsRequests(new DeleteRecordsRequest.Builder(timeout, Map[TopicPartition, java.lang.Long](
      topicPartition -> 5L).asJava, false).build())
    validateErrorDeleteTopicRequests(new DeleteRecordsRequest.Builder(timeout,
      Map[TopicPartition, java.lang.Long](topicPartition -> 2L).asJava, false).build(),
      Map(topicPartition -> new ApiError(Errors.OFFSET_OUT_OF_RANGE,
        "The offset -101 for partition topic-0 is not valid")))*/

  }

  protected def validateErrorDeleteRecordsRequests(request: DeleteRecordsRequest, expectedResponse: Map[TopicPartition, ApiError]): Unit = {

    val expectSuccess = request.partitionOffsets().asScala.filter{ case (tp, offset) => expectedResponse(tp).isSuccess }
    // don't get prior offsets e.g. if topic not exists -> auto.create.topics.enable kicks in!
    val priorOffsets = consumer.beginningOffsets(expectSuccess.keySet.asJava)

    val response = sendDeleteRecordsRequest(request)
    val errors = response.responses.asScala
    assertEquals("The response size should match", expectedResponse.size, response.responses.size)

    expectedResponse.foreach { case (topicPartition, expectedError) =>
      assertEquals(s"The response error codes for $topicPartition should match " + errors(topicPartition).error, expectedResponse(topicPartition).error, errors(topicPartition).error.error)
      assertEquals(s"The response error messages for $topicPartition should match", expectedResponse(topicPartition).message, errors(topicPartition).error.message)
      // If no error validate the records were deleted
      if (expectedError.isSuccess) {
        if (request.validateOnly()) {
          val postOffsets = consumer.beginningOffsets(request.partitionOffsets().keySet())
          assertEquals(priorOffsets, postOffsets)
        } else {
          validateRecordsWereDeleted(topicPartition, request.partitionOffsets.get(topicPartition))
        }
      }
    }
  }

  @Test
  def testNotLeader() {
    TestUtils.waitUntilMetadataIsPropagated(servers, topicPartition.topic, topicPartition.partition)

    val request = new DeleteRecordsRequest.Builder(1000,
      Map[TopicPartition, java.lang.Long](topicPartition -> 5L).asJava, false).build()
    val response = sendDeleteRecordsRequest(request, brokerSocketServer(0))

    val error = response.responses.asScala.head._2.error
    assertEquals("Expected controller error when routed incorrectly",  Errors.UNKNOWN_TOPIC_OR_PARTITION, error.error)
  }

  private def validateRecordsWereDeleted(topicPartition: TopicPartition, expectedOffset: java.lang.Long): Unit = {
    val offset = consumer.beginningOffsets(Set(topicPartition).asJava).get(topicPartition)
    TestUtils.waitUntilTrue (() => offset == expectedOffset,
      s"Offset for $topicPartition should be $expectedOffset but was ${offset}")
  }

  private def sendDeleteRecordsRequest(request: DeleteRecordsRequest,
                                       socketServer: SocketServer = brokerSocketServer(1)): DeleteRecordsResponse = {
    val response = connectAndSend(request, ApiKeys.DELETE_RECORDS, socketServer)
    DeleteRecordsResponse.parse(response, request.version)
  }
}
