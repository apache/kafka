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
package integration.kafka.api

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.{ExecutionException}
import java.util.{ArrayList, Properties}

import kafka.api._
import kafka.cluster.{BrokerEndPoint, Broker, EndPoint}
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadata, TopicAndPartition}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import kafka.coordinator.ConsumerCoordinator
import kafka.integration.KafkaServerTestHarness
import kafka.message.ByteBufferMessageSet
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{TimeoutException, ApiException}
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

class AuthorizerIntegrationTest extends KafkaServerTestHarness {
  val topic = "topic"
  val part = 0
  val brokerId = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val topicAndPartition = new TopicAndPartition(topic, part)
  val group = "my-group"
  val topicResource = new Resource(Topic, topic)
  val groupResource = new Resource(ConsumerGroup, group)


  val authorizer = new SimpleAclAuthorizer

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  val numServers = 1
  val producerCount = 1
  val consumerCount = 2
  val producerConfig = new Properties
  val numRecords = 1

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
  overridingProps.put(KafkaConfig.BrokerIdProp, brokerId.toString)
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")

  val partitionStateInfo = new PartitionStateInfo(new LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId,
    LeaderAndIsr.initialLeaderEpoch, List(brokerId), LeaderAndIsr.initialZKVersion), KafkaController.InitialControllerEpoch), Set(brokerId))
  val endPoint = new EndPoint("localhost", 0, SecurityProtocol.PLAINTEXT)
  
  val RequestKeyToRequest = Map[Short, RequestOrResponse](
    RequestKeys.MetadataKey -> new TopicMetadataRequest(List(topic), correlationId),
    RequestKeys.ProduceKey -> ProducerRequest(ProducerRequest.CurrentVersion, correlationId, clientId, 1, 100,
      collection.mutable.Map(topicAndPartition -> new ByteBufferMessageSet(ByteBuffer.wrap("test".getBytes)))),
    RequestKeys.FetchKey -> new FetchRequest(requestInfo = Map(topicAndPartition -> new PartitionFetchInfo(0, 100))),
    RequestKeys.OffsetsKey -> new OffsetRequest(Map(topicAndPartition -> new PartitionOffsetRequestInfo(-1, 0))),
    RequestKeys.OffsetCommitKey -> new OffsetCommitRequest(groupId = group, requestInfo = Map(topicAndPartition ->
      new OffsetAndMetadata(new OffsetMetadata(0)))),
    RequestKeys.OffsetFetchKey -> new OffsetFetchRequest(groupId = group, requestInfo = Seq(topicAndPartition)),
    RequestKeys.ConsumerMetadataKey -> new ConsumerMetadataRequest(group = group)
// TODO:All the following methods require clusterAction acls, when we don't provide those it never sends a response back.
//    RequestKeys.UpdateMetadataKey -> UpdateMetadataRequest(UpdateMetadataRequest.CurrentVersion, correlationId, clientId, brokerId,
//      KafkaController.InitialControllerEpoch, Map(topicAndPartition -> partitionStateInfo), Set(new Broker(brokerId, Map(SecurityProtocol.PLAINTEXT -> endPoint)))),
//    RequestKeys.LeaderAndIsrKey -> new LeaderAndIsrRequest(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId,
//      brokerId, KafkaController.InitialControllerEpoch, Map((topic, part) -> partitionStateInfo), Set(new BrokerEndPoint(brokerId,"localhost", 0))),
//    RequestKeys.StopReplicaKey -> StopReplicaRequest(StopReplicaRequest.CurrentVersion, correlationId, clientId, brokerId,
//      KafkaController.InitialControllerEpoch, true, Set(topicAndPartition)),
//    RequestKeys.ControlledShutdownKey -> new ControlledShutdownRequest(ControlledShutdownRequest.CurrentVersion, correlationId, Some(clientId), brokerId)
  )

  val RequestKeyToResponseDeserializer: Map[Short, (ByteBuffer) => RequestOrResponse] =
    Map(RequestKeys.MetadataKey -> TopicMetadataResponse.readFrom,
      RequestKeys.ProduceKey -> ProducerResponse.readFrom,
      RequestKeys.FetchKey -> ((bytes: ByteBuffer) => FetchResponse.readFrom(bytes, FetchRequest.CurrentVersion)),
      RequestKeys.OffsetsKey -> OffsetResponse.readFrom,
      RequestKeys.OffsetCommitKey -> OffsetCommitResponse.readFrom,
      RequestKeys.OffsetFetchKey -> OffsetFetchResponse.readFrom,
      RequestKeys.ConsumerMetadataKey -> ConsumerMetadataResponse.readFrom,
      RequestKeys.UpdateMetadataKey -> UpdateMetadataResponse.readFrom,
      RequestKeys.LeaderAndIsrKey -> LeaderAndIsrResponse.readFrom,
      RequestKeys.StopReplicaKey -> StopReplicaResponse.readFrom,
      RequestKeys.ControlledShutdownKey -> ControlledShutdownResponse.readFrom
    )

  val RequestKeyToErrorCode = Map[Short, (Nothing) => Short](
    RequestKeys.MetadataKey -> ((resp: TopicMetadataResponse) => resp.topicsMetadata.find(_.topic == topic).get.errorCode),
    RequestKeys.ProduceKey -> ((resp: ProducerResponse) => resp.status.find(_._1 == topicAndPartition).get._2.error),
    RequestKeys.FetchKey -> ((resp: FetchResponse) => resp.data.find(_._1 == topicAndPartition).get._2.error),
    RequestKeys.OffsetsKey -> ((resp: OffsetResponse) => resp.partitionErrorAndOffsets.find(_._1 == topicAndPartition).get._2.error),
    RequestKeys.OffsetCommitKey -> ((resp: OffsetCommitResponse) => resp.commitStatus.find(_._1 == topicAndPartition).get._2),
    RequestKeys.OffsetFetchKey -> ((resp: OffsetFetchResponse) => resp.requestInfo.find(_._1 == topicAndPartition).get._2.error),
    RequestKeys.ConsumerMetadataKey -> ((resp: ConsumerMetadataResponse) => resp.errorCode),
    RequestKeys.UpdateMetadataKey -> ((resp: UpdateMetadataResponse) => resp.errorCode),
    RequestKeys.LeaderAndIsrKey -> ((resp: LeaderAndIsrResponse) => resp.errorCode),
    RequestKeys.StopReplicaKey -> ((resp: StopReplicaResponse) => resp.errorCode),
    RequestKeys.ControlledShutdownKey -> ((resp: ControlledShutdownResponse) => resp.errorCode)
  )

  val RequestKeysToAcls = Map[Short, Map[Resource, Set[Acl]]](
    RequestKeys.MetadataKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe))),
    RequestKeys.ProduceKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write))),
    RequestKeys.FetchKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read))),
    RequestKeys.OffsetsKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe))),
    RequestKeys.OffsetCommitKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)),
      groupResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read))),
    RequestKeys.OffsetFetchKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)),
      groupResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read))),
    RequestKeys.ConsumerMetadataKey -> Map(topicResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)),
      groupResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read))),
    RequestKeys.UpdateMetadataKey -> Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction))),
    RequestKeys.LeaderAndIsrKey -> Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction))),
    RequestKeys.StopReplicaKey -> Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction))),
    RequestKeys.ControlledShutdownKey -> Map(Resource.ClusterResource -> Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow,Acl.WildCardHost, ClusterAction)))
  )

  // configure the servers and clients
  override def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()

    authorizer.configure(this.servers.head.config.originals())
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, ClusterAction)), Resource.ClusterResource)

    for (i <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
        acks = 1)
    for (i <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
        groupId = group,
        partitionAssignmentStrategy = "range")

    // create the consumer offset topic
    TestUtils.createTopic(zkClient, ConsumerCoordinator.OffsetsTopicName,
      1,
      1,
      servers,
      servers.head.consumerCoordinator.offsetsTopicConfigs)
    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(this.zkClient, topic, 1, 1, this.servers)
  }

  @After
  override def tearDown() = {
    removeAllAcls
    super.tearDown()
  }

  @Test
  def testAuthorization() {
    val socket = new Socket("localhost", servers.head.boundPort())

    for ((key, request) <- RequestKeyToRequest) {

      removeAllAcls

      val buffer = ByteBuffer.allocate(request.sizeInBytes)
      request.writeTo(buffer)
      buffer.rewind()
      val requestBytes = buffer.array()

      sendRequestAndVerifyResponseErrorCode(socket, key, requestBytes, ErrorMapping.AuthorizationCode)

      for ((resource, acls) <- RequestKeysToAcls(key))
        addAndVerifyAcls(acls, resource)

      sendRequestAndVerifyResponseErrorCode(socket, key, requestBytes, ErrorMapping.NoError)
    }
  }

    @Test
    def testProduceNeedsAuthorization() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
      try {
        sendRecords(numRecords, tp)
        Assert.fail("should have thrown exception")
      } catch {
        case e: ApiException => Assert.assertEquals(Errors.AUTHORIZATION_FAILED.exception().getMessage, e.getMessage)
      }
    }

    @Test
    def testOnlyWritePermissionAllowsWritingToProducer() {
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
      try {
        sendRecords(numRecords, tp)
      } catch {
        case e: Exception => Assert.fail("Should not fail")
      }
    }

    @Test
    def testCreatePermissionNeededForWritingToNonExistentTopic() {
      val newTopic = "newTopic"
      val topicPartition = new TopicPartition(newTopic, 0)
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), new Resource(Topic, newTopic))
      try {
        sendRecords(numRecords, topicPartition)
        Assert.fail("should have thrown exception")
      } catch {
        case e: TimeoutException =>
        //TODO Need to update the producer so it actually throws the server side of exception.
        case e: Exception => Assert.fail("Only timeout exception should be thrown.")
      }

      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)
      try {
        sendRecords(numRecords, topicPartition)
      } catch {
        case e: Exception => Assert.fail("No exception should be thrown.")
      }
    }

    @Test
    def testConsumerNeedsAuthorization() {
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
      //TODO: Ideally we would want to test that when consumerGroup permission is not present we still get an AuthorizationException
      //but the consumer fetcher currently waits forever for the consumer metadata to become available.
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
      sendRecords(1, tp)
      try {
        this.consumers.head.assign(List(tp).asJava)
        consumeRecords(this.consumers.head)
        Assert.fail("should have thrown exception")
      } catch {
        case e: ApiException => Assert.assertEquals("Not authorized to read from topic-0", e.getMessage)
      }
    }

    @Test
    def testAllowingReadOnTopicAndGroupAllowsReading() {
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
      sendRecords(1, tp)
      this.consumers.head.assign(List(tp).asJava)
      consumeRecords(this.consumers.head)
    }
  
//    TODO: The following test goes into an infinite loop as consumer waits for consumer metadata to be propogated for ever.
//    @Test
//    def testCreatePermissionNeededToReadFromNonExistentTopic() {
//      val newTopic = "newTopic"
//      val topicPartition = new TopicPartition(newTopic, 0)
//      val newTopicResource = new Resource(Topic, newTopic)
//      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), newTopicResource)
//      try {
//        this.consumers(0).assign(List(topicPartition).asJava)
//        consumeRecords(this.consumers(0))
//        Assert.fail("should have thrown exception")
//      } catch {
//        case e: TimeoutException =>
//        //TODO need to update the consumer so if no metadata is propagated it throws server side exception instead of trying forever.
//        case e: Exception => Assert.fail("Only timeout exception should be thrown.")
//      }
//
//      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), newTopicResource)
//      addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)
//      try {
//        sendRecords(numRecords, topicPartition)
//        consumeRecords(this.consumers(0))
//      } catch {
//        case e: Exception => Assert.fail("No exception should be thrown.")
//      }
//    }

  def removeAllAcls() = {
    authorizer.getAcls().keys.foreach { resource =>
      authorizer.removeAcls(resource)
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], authorizer, resource)
    }
  }

  def sendRequestAndVerifyResponseErrorCode(socket: Socket, key: Short, requestBytes: Array[Byte], expectedErrorCode: Short): Unit = {
    sendRequest(socket, key, requestBytes)
    val response = RequestKeyToResponseDeserializer(key)(receiveResponse(socket))
    Assert.assertEquals(s"$key failed", expectedErrorCode, RequestKeyToErrorCode(key).asInstanceOf[(RequestOrResponse) => Short](response))
  }

  private def sendRequest(socket: Socket, id: Short, request: Array[Byte]) {
    println(request.length)
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length + 2)
    outgoing.writeShort(id)
    outgoing.write(request)
    outgoing.flush()
  }

  private def receiveResponse(socket: Socket): ByteBuffer = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    ByteBuffer.wrap(response)
  }

  private def sendRecords(numRecords: Int, tp: TopicPartition) {
    val futures = (0 until numRecords).map { i =>
      this.producers.head.send(new ProducerRecord(tp.topic(), tp.partition(), i.toString.getBytes, i.toString.getBytes))
    }
    try {
      futures.map(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def addAndVerifyAcls(acls: Set[Acl], resource: Resource) = {
    authorizer.addAcls(acls, resource)
    TestUtils.waitAndVerifyAcls(authorizer.getAcls(resource) ++ acls, authorizer, resource)
  }


  private def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]], numRecords: Int = 1, startingOffset: Int =
  0) {
    val records = new ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    val maxIters = numRecords * 50
    var iters = 0
    while (records.size < numRecords) {
      for (record <- consumer.poll(50).asScala) {
        records.add(record)
      }
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    for (i <- 0 until numRecords) {
      val record = records.get(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic())
      assertEquals(part, record.partition())
      assertEquals(offset.toLong, record.offset())
    }
  }
}
