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
package kafka.api

import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.lang.{Long => JLong}
import java.time.{Duration => JDuration}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}
import java.util.{Collections, Optional, Properties}
import java.{time, util}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils._
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.HostResolver
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer
import org.apache.kafka.clients.consumer.{CommitFailedException, Consumer, ConsumerConfig, ConsumerRecords, GroupProtocol, KafkaConsumer, OffsetAndMetadata, ShareConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig, SslConfigs, TopicConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.requests.DeleteRecordsRequest
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{ConsumerGroupState, ElectionType, GroupState, GroupType, IsolationLevel, TopicCollection, TopicPartition, TopicPartitionInfo, TopicPartitionReplica, Uuid}
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinatorConfig}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.common.{EligibleLeaderReplicasVersion, MetadataVersion}
import org.apache.kafka.server.config.{QuotaConfig, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.logger.LoggingController
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, LogFileUtils}
import org.apache.kafka.test.TestUtils.{DEFAULT_MAX_WAIT_MS, assertFutureThrows}
import org.apache.logging.log4j.core.config.Configurator
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory

import java.util.AbstractMap.SimpleImmutableEntry
import scala.collection.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Random, Using}

/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see [[org.apache.kafka.clients.admin.KafkaAdminClientTest]] for unit tests of the admin client.
 */
class PlaintextAdminIntegrationTest extends BaseAdminIntegrationTest {
  import PlaintextAdminIntegrationTest._

  val topic = "topic"
  val partition = 0
  val topicPartition = new TopicPartition(topic, partition)

  private var brokerLoggerConfigResource: ConfigResource = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    Configurator.reconfigure()
    brokerLoggerConfigResource = new ConfigResource(
      ConfigResource.Type.BROKER_LOGGER, brokers.head.config.brokerId.toString)
  }

  @Test
  @Timeout(30)
  def testDescribeConfigWithOptionTimeoutMs(): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    val brokenClient = Admin.create(config)

    try {
      // Describe and broker
      val brokerResource1 = new ConfigResource(ConfigResource.Type.BROKER, brokers(1).config.brokerId.toString)
      val brokerResource2 = new ConfigResource(ConfigResource.Type.BROKER, brokers(2).config.brokerId.toString)
      val configResources = util.List.of(brokerResource1, brokerResource2)

      val exception = assertThrows(classOf[ExecutionException], () => {
        brokenClient.describeConfigs(configResources,new DescribeConfigsOptions().timeoutMs(0)).all().get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally brokenClient.close(time.Duration.ZERO)
  }

  @Test
  def testCreatePartitionWithOptionRetryOnQuotaViolation(): Unit = {
    // Since it's hard to stably reach quota limit in integration test, we only verify quota configs are set correctly
    val config = createConfig
    val clientId = "test-client-id"

    config.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId)
    client = Admin.create(config)

    val entity = new ClientQuotaEntity(util.Map.of(ClientQuotaEntity.CLIENT_ID, clientId))
    val configEntries = Map(QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG -> 1.0, QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 3.0)
    client.alterClientQuotas(util.List.of(new ClientQuotaAlteration(entity, configEntries.map { case (k, v) =>
      new ClientQuotaAlteration.Op(k, v)
    }.asJavaCollection))).all.get

    TestUtils.waitUntilTrue(() => {
      // wait for our ClientQuotaEntity to be set
      client.describeClientQuotas(ClientQuotaFilter.all()).entities().get().size == 1
    }, "Timed out waiting for quota config to be propagated to all servers")

    val quotaEntities = client.describeClientQuotas(ClientQuotaFilter.all()).entities().get()

    assertEquals(configEntries, quotaEntities.get(entity).asScala)
  }

  @Test
  def testDefaultNameQuotaIsNotEqualToDefaultQuota(): Unit = {
    val config = createConfig
    val defaultQuota = "<default>"
    client = Admin.create(config)

    //"<default>" can not create default quota
    val userEntity = new ClientQuotaEntity(util.Map.of(ClientQuotaEntity.USER, defaultQuota))
    val clientEntity = new ClientQuotaEntity(util.Map.of(ClientQuotaEntity.CLIENT_ID, defaultQuota))
    val userAlterations = new ClientQuotaAlteration(userEntity,
      util.Set.of(new ClientQuotaAlteration.Op("consumer_byte_rate", 10000D)))
    val clientAlterations = new ClientQuotaAlteration(clientEntity,
      util.Set.of(new ClientQuotaAlteration.Op("producer_byte_rate", 10000D)))
    val alterations = util.List.of(userAlterations, clientAlterations)
    client.alterClientQuotas(alterations).all().get()

    TestUtils.waitUntilTrue(() => {
      try {
        //check "<default>" as a default quota use
        val userDefaultQuotas = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)))).entities().get()
        val clientDefaultQuotas = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID)))).entities().get()

        //check "<default>" as a normal quota use
        val userNormalQuota = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER,defaultQuota)))).entities().get()
        val clientNormalQuota = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID,defaultQuota)))).entities().get()

        userDefaultQuotas.size() == 0 && clientDefaultQuotas.size() == 0 && userNormalQuota.size() == 1 && clientNormalQuota.size() == 1
      } catch {
        case _: Exception => false
      }
    }, "Timed out waiting for quota config to be propagated to all servers")

    //null can create default quota
    val userDefaultEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> Option.empty[String].orNull).asJava)
    client.alterClientQuotas(util.List.of(new ClientQuotaAlteration(userDefaultEntity, util.Set.of(
            new ClientQuotaAlteration.Op("consumer_byte_rate", 100D))))).all().get()
    val clientDefaultEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.CLIENT_ID -> Option.empty[String].orNull).asJava)
    client.alterClientQuotas(util.List.of(new ClientQuotaAlteration(clientDefaultEntity, util.Set.of(
      new ClientQuotaAlteration.Op("producer_byte_rate", 100D))))).all().get()

    TestUtils.waitUntilTrue(() => {
      try {
        val userDefaultQuota = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)))).entities().get()
        val clientDefaultQuota = client.describeClientQuotas(ClientQuotaFilter.containsOnly(util.List.of(
          ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID)))).entities().get()
        userDefaultQuota.size() == 1 && clientDefaultQuota.size() == 1
      } catch {
        case _: Exception => false
      }
    }, "Timed out waiting for quota config to be propagated to all servers")
  }

  @Test
  def testDescribeUserScramCredentials(): Unit = {
    client = createAdminClient

    // add a new user
    val targetUserName = "tom"
    client.alterUserScramCredentials(util.List.of(
      new UserScramCredentialUpsertion(targetUserName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456")
    )).all.get
    TestUtils.waitUntilTrue(() => client.describeUserScramCredentials().all().get().size() == 1,
      "Add one user scram credential timeout")

    val result = client.describeUserScramCredentials().all().get()
    result.forEach((userName, scramDescription) => {
      assertEquals(targetUserName, userName)
      assertEquals(targetUserName, scramDescription.name())
      val credentialInfos = scramDescription.credentialInfos()
      assertEquals(1, credentialInfos.size())
      assertEquals(ScramMechanism.SCRAM_SHA_256, credentialInfos.get(0).mechanism())
      assertEquals(4096, credentialInfos.get(0).iterations())
    })

    // add other users
    client.alterUserScramCredentials(util.List.of(
      new UserScramCredentialUpsertion("tom2", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456"),
      new UserScramCredentialUpsertion("tom3", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "123456")
    )).all().get
    TestUtils.waitUntilTrue(() => client.describeUserScramCredentials().all().get().size() == 3,
      "Add user scram credential timeout")

    // alter user info
    client.alterUserScramCredentials(util.List.of(
      new UserScramCredentialUpsertion(targetUserName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), "123456")
    )).all.get
    TestUtils.waitUntilTrue(() => {
      client.describeUserScramCredentials().all().get().get(targetUserName).credentialInfos().size() == 2
    }, "Alter user scram credential timeout")

    val userTomResult = client.describeUserScramCredentials().all().get()
    assertEquals(3, userTomResult.size())
    val userScramCredential = userTomResult.get(targetUserName)
    assertEquals(targetUserName, userScramCredential.name())
    val credentialInfos = userScramCredential.credentialInfos()
    assertEquals(2, credentialInfos.size())
    val credentialList = credentialInfos.asScala.sortBy(s => s.mechanism().`type`())
    assertEquals(ScramMechanism.SCRAM_SHA_256, credentialList.head.mechanism())
    assertEquals(4096, credentialList.head.iterations())
    assertEquals(ScramMechanism.SCRAM_SHA_512, credentialList(1).mechanism())
    assertEquals(8192, credentialList(1).iterations())

    // test describeUserScramCredentials(List<String> users)
    val userAndScramMap = client.describeUserScramCredentials(util.List.of("tom2")).all().get()
    assertEquals(1, userAndScramMap.size())
    val scram = userAndScramMap.get("tom2")
    assertNotNull(scram)
    val credentialInfo = scram.credentialInfos().get(0)
    assertEquals(ScramMechanism.SCRAM_SHA_256, credentialInfo.mechanism())
    assertEquals(4096, credentialInfo.iterations())
  }

  private def createInvalidAdminClient(): Admin = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    Admin.create(config)
  }

  @Timeout(10)
  @Test
  def testDescribeUserScramCredentialsTimeout(): Unit = {
    client = createInvalidAdminClient()
    try {
      // test describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options)
      val exception = assertThrows(classOf[ExecutionException], () => {
        client.describeUserScramCredentials(util.List.of("tom4"),
          new DescribeUserScramCredentialsOptions().timeoutMs(0)).all().get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  private def consumeToExpectedNumber = (expectedNumber: Int, groupProtocol: String) => {
    val configs = new util.HashMap[String, Object]()
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
    configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString)
    configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
    val consumer = new KafkaConsumer(configs, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try {
      consumer.assign(util.Set.of(topicPartition))
      consumer.seekToBeginning(util.Set.of(topicPartition))
      var consumeNum = 0
      TestUtils.waitUntilTrue(() => {
        val records = consumer.poll(time.Duration.ofMillis(100))
        consumeNum += records.count()
        consumeNum >= expectedNumber
      }, "consumeToExpectedNumber timeout")
    } finally consumer.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDescribeProducers(groupProtocol: String): Unit = {
    client = createAdminClient
    client.createTopics(util.List.of(new NewTopic(topic, 1, 1.toShort))).all().get()

    def appendCommonRecords = (records: Int) => {
      val producer = new KafkaProducer(util.Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        plaintextBootstrapServers(brokers).asInstanceOf[Object]), new ByteArraySerializer, new ByteArraySerializer)
      try {
        (0 until records).foreach(i =>
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
            topic, partition, i.toString.getBytes, i.toString.getBytes())))
      } finally producer.close()
    }

    def appendTransactionRecords(transactionId: String, records: Int, commit: Boolean): KafkaProducer[Array[Byte], Array[Byte]] = {
      val producer = TestUtils.createTransactionalProducer(transactionId, brokers)
      producer.initTransactions()
      producer.beginTransaction()
      (0 until records).foreach(i =>
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
          topic, partition, i.toString.getBytes, i.toString.getBytes())))
      producer.flush()
      if (commit) {
        producer.commitTransaction()
        producer.close()
      }

      producer
    }

    def queryProducerDetail() = client
      .describeProducers(util.List.of(topicPartition))
      .partitionResult(topicPartition).get().activeProducers().asScala

    // send common msg
    appendCommonRecords(1)
    val producerIterator = queryProducerDetail()
    assertEquals(1, producerIterator.size)
    val producerState = producerIterator.last
    assertEquals(0, producerState.producerEpoch())
    assertFalse(producerState.coordinatorEpoch().isPresent)
    assertFalse(producerState.currentTransactionStartOffset().isPresent)


    // send committed transaction msg
    appendTransactionRecords("foo", 2, commit = true)
    // consume 3 records to ensure transaction finished
    consumeToExpectedNumber(3, groupProtocol)
    val transactionProducerIterator = queryProducerDetail()
    assertEquals(2, transactionProducerIterator.size)
    val containsCoordinatorEpochIterator = transactionProducerIterator
      .filter(producer => producer.coordinatorEpoch().isPresent)
    assertEquals(1, containsCoordinatorEpochIterator.size)
    val transactionProducerState = containsCoordinatorEpochIterator.last
    assertFalse(transactionProducerState.currentTransactionStartOffset().isPresent)


    // send ongoing transaction msg
    val ongoingProducer = appendTransactionRecords("foo3", 3, commit = false)
    try {
      val transactionNoneCommitProducerIterator = queryProducerDetail()
      assertEquals(3, transactionNoneCommitProducerIterator.size)
      val containsOngoingIterator = transactionNoneCommitProducerIterator
        .filter(producer => producer.currentTransactionStartOffset().isPresent)
      assertEquals(1, containsOngoingIterator.size)
      val ongoingTransactionProducerState = containsOngoingIterator.last
      // we send (1 common msg) + (2 transaction msg) + (1 transaction marker msg), so transactionStartOffset is 4
      assertEquals(4, ongoingTransactionProducerState.currentTransactionStartOffset().getAsLong)
    } finally ongoingProducer.close()
  }

  @Test
  def testDescribeTransactions(): Unit = {
    client = createAdminClient
    client.createTopics(util.List.of(new NewTopic(topic, 1, 1.toShort))).all().get()

    var transactionId = "foo"
    val stateAbnormalMsg = "The transaction state is abnormal"

    def describeTransactions(): TransactionDescription = {
      client.describeTransactions(util.Set.of(transactionId)).description(transactionId).get()
    }
    def transactionState(): TransactionState = {
      describeTransactions().state()
    }

    def findCoordinatorIdByTransactionId(transactionId: String): Int = {
      // calculate the transaction partition id
      val transactionPartitionId = Utils.abs(transactionId.hashCode) %
        brokers.head.metadataCache.numPartitions(Topic.TRANSACTION_STATE_TOPIC_NAME).get
      val transactionTopic = client.describeTopics(util.Set.of(Topic.TRANSACTION_STATE_TOPIC_NAME))
      val partitionList = transactionTopic.allTopicNames().get().get(Topic.TRANSACTION_STATE_TOPIC_NAME).partitions()
      partitionList.asScala.filter(tp => tp.partition() == transactionPartitionId).head.leader().id()
    }

    // normal commit case
    val producer = TestUtils.createTransactionalProducer(transactionId, brokers)
    try {
      // init, the transaction is not begin, so TransactionalIdNotFoundException is expected
      val exception = assertThrows(classOf[ExecutionException], () => transactionState())
      assertInstanceOf(classOf[TransactionalIdNotFoundException], exception.getCause)

      producer.initTransactions()
      assertEquals(TransactionState.EMPTY, transactionState())
      producer.beginTransaction()
      assertEquals(TransactionState.EMPTY, transactionState())

      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
      producer.flush()
      TestUtils.waitUntilTrue(() => transactionState() == TransactionState.ONGOING, stateAbnormalMsg)

      TestUtils.waitUntilTrue(() => describeTransactions().topicPartitions().size() == 1, "Describe transactions timeout")
      val transactionResult = describeTransactions()
      assertEquals(findCoordinatorIdByTransactionId(transactionId), transactionResult.coordinatorId())
      assertEquals(0, transactionResult.producerId())
      assertEquals(0, transactionResult.producerEpoch())
      assertEquals(util.Set.of(topicPartition), transactionResult.topicPartitions())

      producer.commitTransaction()
      TestUtils.waitUntilTrue(() => transactionState() == TransactionState.COMPLETE_COMMIT, stateAbnormalMsg)
    } finally producer.close()

    // abort case
    transactionId = "foo2"
    val abortProducer = TestUtils.createTransactionalProducer(transactionId, brokers)
    try {
      // init, the transaction is not begin, so TransactionalIdNotFoundException is expected
      val exception = assertThrows(classOf[ExecutionException], () => transactionState())
      assertTrue(exception.getCause.isInstanceOf[TransactionalIdNotFoundException])

      abortProducer.initTransactions()
      assertEquals(TransactionState.EMPTY, transactionState())
      abortProducer.beginTransaction()
      assertEquals(TransactionState.EMPTY, transactionState())

      val transactionResult = describeTransactions()
      assertEquals(findCoordinatorIdByTransactionId(transactionId), transactionResult.coordinatorId())
      assertEquals(0, transactionResult.topicPartitions().size())

      abortProducer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
      abortProducer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k2".getBytes, "v2".getBytes()))
      abortProducer.flush()

      val transactionSendMsgResult = describeTransactions()
      assertEquals(findCoordinatorIdByTransactionId(transactionId), transactionSendMsgResult.coordinatorId())
      assertEquals(util.Set.of(topicPartition), transactionSendMsgResult.topicPartitions())
      assertEquals(topicPartition, transactionSendMsgResult.topicPartitions().asScala.head)

      TestUtils.waitUntilTrue(() => transactionState() == TransactionState.ONGOING, stateAbnormalMsg)

      abortProducer.abortTransaction()
      TestUtils.waitUntilTrue(() => transactionState() == TransactionState.COMPLETE_ABORT, stateAbnormalMsg)
    } finally abortProducer.close()
  }

  @Test
  @Timeout(10)
  def testDescribeTransactionsTimeout(): Unit = {
    client = createInvalidAdminClient()
    try {
      val transactionId = "foo"
      val exception = assertThrows(classOf[ExecutionException], () => {
        client.describeTransactions(util.Set.of(transactionId),
          new DescribeTransactionsOptions().timeoutMs(0)).description(transactionId).get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  @Timeout(10)
  def testAbortTransactionTimeout(): Unit = {
    client = createInvalidAdminClient()
    try {
      val exception = assertThrows(classOf[ExecutionException], () => {
        client.abortTransaction(
          new AbortTransactionSpec(topicPartition, 1, 1, 1),
          new AbortTransactionOptions().timeoutMs(0)).all().get()
      })
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  def testListTransactions(): Unit = {
    def createTransactionList(): Unit = {
      client = createAdminClient
      client.createTopics(util.List.of(new NewTopic(topic, 1, 1.toShort))).all().get()

      val stateAbnormalMsg = "The transaction state is abnormal"
      def transactionState(transactionId: String): TransactionState = {
        client.describeTransactions(util.Set.of(transactionId)).description(transactionId).get().state()
      }

      val transactionId1 = "foo"
      val producer = TestUtils.createTransactionalProducer(transactionId1, brokers)
      try {
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
        producer.flush()
        producer.commitTransaction()
      } finally producer.close()
      TestUtils.waitUntilTrue(() => transactionState(transactionId1) == TransactionState.COMPLETE_COMMIT, stateAbnormalMsg)

      val transactionId2 = "foo2"
      val producer2 = TestUtils.createTransactionalProducer(transactionId2, brokers)
      try {
        producer2.initTransactions()
        producer2.beginTransaction()
        producer2.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
        producer2.flush()
        producer2.abortTransaction()
      } finally producer2.close()
      TestUtils.waitUntilTrue(() => transactionState(transactionId2) == TransactionState.COMPLETE_ABORT, stateAbnormalMsg)

      val transactionId3 = "foo3"
      val producer3 = TestUtils.createTransactionalProducer(transactionId3, brokers)
      try {
        producer3.initTransactions()
        producer3.beginTransaction()
        producer3.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
        producer3.flush()
        producer3.commitTransaction()
      } finally producer3.close()
      TestUtils.waitUntilTrue(() => transactionState(transactionId3) == TransactionState.COMPLETE_COMMIT, stateAbnormalMsg)
    }

    createTransactionList()

    assertEquals(3, client.listTransactions().all().get().size())
    assertEquals(2, client.listTransactions(new ListTransactionsOptions()
        .filterStates(util.List.of(TransactionState.COMPLETE_COMMIT))).all().get().size())
    assertEquals(1, client.listTransactions(new ListTransactionsOptions()
      .filterStates(util.List.of(TransactionState.COMPLETE_ABORT))).all().get().size())
    assertEquals(1, client.listTransactions(new ListTransactionsOptions()
      .filterProducerIds(util.List.of(0L))).all().get().size())

    // ensure all transaction's txnStartTimestamp >= 500
    Thread.sleep(501)
    assertEquals(3, client.listTransactions(new ListTransactionsOptions().filterOnDuration(500)).all().get().size())

    val producerNew = TestUtils.createTransactionalProducer("foo4", brokers)
    try {
      producerNew.initTransactions()
      producerNew.beginTransaction()
      producerNew.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "k1".getBytes, "v1".getBytes()))
      producerNew.flush()
      val transactionList = client.listTransactions(new ListTransactionsOptions().filterOnDuration(500)).all().get()
      // current transaction start time is now, so transactionList size is still 3
      assertEquals(3, transactionList.size())
      // transactionList not contains 'foo4'
      assertEquals(0, transactionList.asScala.count(t => t.transactionalId().equals("foo4")))
    } finally producerNew.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAbortTransaction(groupProtocol: String): Unit = {
    client = createAdminClient
    val tp = new TopicPartition("topic1", 0)
    client.createTopics(util.List.of(new NewTopic(tp.topic(), 1, 1.toShort))).all().get()

    def checkConsumer = (tp: TopicPartition, expectedNumber: Int) => {
      val configs = new util.HashMap[String, Object]()
      configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
      configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString)
      configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
      val consumer = new KafkaConsumer(configs, new ByteArrayDeserializer, new ByteArrayDeserializer)
      try {
        consumer.assign(util.Set.of(tp))
        consumer.seekToBeginning(util.Set.of(tp))
        def verifyRecordCount(records: ConsumerRecords[Array[Byte], Array[Byte]]): Boolean = {
          expectedNumber == records.count()
        }
        TestUtils.pollRecordsUntilTrue(
          consumer,
          verifyRecordCount,
          s"Consumer.poll() did not return the expected number of records ($expectedNumber) within the timeout",
          pollTimeoutMs = 3000
        )
      } finally consumer.close()
    }

    def appendRecord = (records: Int) => {
      val producer = new KafkaProducer(util.Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        plaintextBootstrapServers(brokers).asInstanceOf[Object]), new ByteArraySerializer, new ByteArraySerializer)
      try {
        (0 until records).foreach(i =>
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
            tp.topic(), tp.partition(), i.toString.getBytes, i.toString.getBytes())))
      } finally producer.close()
    }

    val producer = TestUtils.createTransactionalProducer("foo", brokers)
    try {
      producer.initTransactions()
      producer.beginTransaction()

      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
        tp.topic(), tp.partition(), "k1".getBytes, "v1".getBytes()))
      producer.flush()
      producer.commitTransaction()

      checkConsumer(tp, 1)

      producer.beginTransaction()
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
        tp.topic(), tp.partition(), "k2".getBytes, "v2".getBytes()))
      producer.flush()

      appendRecord(1)
      checkConsumer(tp, 1)

      val transactionalProducer = client.describeProducers(util.List.of(tp))
        .partitionResult(tp).get().activeProducers().asScala.minBy(_.producerId())

      assertDoesNotThrow(() => client.abortTransaction(
        new AbortTransactionSpec(tp,
          transactionalProducer.producerId(),
          transactionalProducer.producerEpoch().toShort,
          transactionalProducer.coordinatorEpoch().getAsInt)).all().get())

      checkConsumer(tp, 2)
    } finally producer.close()
  }

  @Test
  def testClose(): Unit = {
    val client = createAdminClient
    client.close()
    client.close() // double close has no effect
  }

  @Test
  def testListNodes(): Unit = {
    client = createAdminClient
    val brokerStrs = bootstrapServers().split(",").toList.sorted
    var nodeStrs: List[String] = null
    do {
      val nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map(node => s"${node.host}:${node.port}").toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
  }

  @Test
  def testListNodesWithFencedBroker(): Unit = {
    client = createAdminClient
    val fencedBrokerId = brokers.last.config.brokerId
    killBroker(fencedBrokerId, JDuration.ofMillis(0))
    // It takes a few seconds for a broker to get fenced after being killed
    // So we retry until only 2 of 3 brokers returned in the result or the max wait is reached
    TestUtils.retry(20000) {
      assertTrue(client.describeCluster().nodes().get().asScala.size.equals(brokers.size - 1))
    }

    // List nodes again but this time include the fenced broker
    val nodes = client.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true)).nodes().get().asScala
    assertTrue(nodes.size.equals(brokers.size))
    nodes.foreach(node => {
      if (node.id().equals(fencedBrokerId)) {
        assertTrue(node.isFenced)
      } else {
        assertFalse(node.isFenced)
      }
    })
  }

  @Test
  def testAdminClientHandlingBadIPWithoutTimeout(): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "1000")
    val returnBadAddressFirst = new HostResolver {
      override def resolve(host: String): Array[InetAddress] = {
        Array[InetAddress](InetAddress.getByName("10.200.20.100"), InetAddress.getByName(host))
      }
    }
    client = AdminClientTestUtils.create(config, returnBadAddressFirst)
    // simply check that a call, e.g. describeCluster, returns normally
    client.describeCluster().nodes().get()
  }

  @Test
  def testCreateExistingTopicsThrowTopicExistsException(): Unit = {
    client = createAdminClient
    val topic = "mytopic"
    val topics = Seq(topic)
    val newTopics = util.List.of(new NewTopic(topic, 1, 1.toShort))

    client.createTopics(newTopics).all.get()
    waitForTopics(client, topics, List())

    val newTopicsWithInvalidRF = util.List.of(new NewTopic(topic, 1, (brokers.size + 1).toShort))
    val e = assertThrows(classOf[ExecutionException],
      () => client.createTopics(newTopicsWithInvalidRF, new CreateTopicsOptions().validateOnly(true)).all.get())
    assertTrue(e.getCause.isInstanceOf[TopicExistsException])
  }

  @Test
  def testDeleteTopicsWithIds(): Unit = {
    client = createAdminClient
    val topics = Seq("mytopic", "mytopic2", "mytopic3")
    val newTopics = util.List.of(
      new NewTopic("mytopic", util.Map.of(0: Integer, util.List.of[Integer](1, 2), 1: Integer, util.List.of[Integer](2, 0))),
      new NewTopic("mytopic2", 3, 3.toShort),
      new NewTopic("mytopic3", Optional.empty[Integer], Optional.empty[java.lang.Short])
    )
    val createResult = client.createTopics(newTopics)
    createResult.all.get()
    waitForTopics(client, topics, List())
    val topicIds = getTopicIds().values.toSet

    client.deleteTopics(TopicCollection.ofTopicIds(topicIds.asJava)).all.get()
    waitForTopics(client, List(), topics)
  }

  @Test
  def testDeleteTopicsWithOptionTimeoutMs(): Unit = {
    client = createInvalidAdminClient()

    try {
      val timeoutOption = new DeleteTopicsOptions().timeoutMs(0)
      val exception = assertThrows(classOf[ExecutionException], () =>
        client.deleteTopics(util.List.of("test-topic"), timeoutOption).all().get())
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  def testListTopicsWithOptionTimeoutMs(): Unit = {
    client = createInvalidAdminClient()

    try {
      val timeoutOption = new ListTopicsOptions().timeoutMs(0)
      val exception = assertThrows(classOf[ExecutionException], () =>
        client.listTopics(timeoutOption).names().get())
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  def testListTopicsWithOptionListInternal(): Unit = {
    client = createAdminClient

    val topicNames = client.listTopics(new ListTopicsOptions().listInternal(true)).names().get()
    assertFalse(topicNames.isEmpty, "Expected to see internal topics")
  }

  @Test
  def testDescribeTopicsWithOptionPartitionSizeLimitPerResponse(): Unit = {
    client = createAdminClient

    val testTopics = Seq("test-topic")
    client.createTopics(testTopics.map(new NewTopic(_, 3, 1.toShort)).asJava).all.get()
    waitForTopics(client, testTopics, List())

    val topics = client.describeTopics(testTopics.asJava, new DescribeTopicsOptions().partitionSizeLimitPerResponse(1)).allTopicNames().get()
    assertEquals(1, topics.size())
    assertEquals(3, topics.get("test-topic").partitions().size())

    client.deleteTopics(testTopics.asJava).all().get()
    waitForTopics(client, List(), testTopics)
  }

  @Test
  def testDescribeTopicsWithOptionTimeoutMs(): Unit = {
    client = createInvalidAdminClient()

    try {
      val timeoutOption = new DescribeTopicsOptions().timeoutMs(0)
      val exception = assertThrows(classOf[ExecutionException], () =>
        client.describeTopics(util.List.of("test-topic"), timeoutOption).allTopicNames().get())
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  /**
    * describe should not auto create topics
    */
  @Test
  def testDescribeNonExistingTopic(): Unit = {
    client = createAdminClient

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())

    val nonExistingTopic = "non-existing"
    val results = client.describeTopics(util.List.of(nonExistingTopic, existingTopic)).topicNameValues()
    assertEquals(existingTopic, results.get(existingTopic).get.name)
    assertFutureThrows(classOf[UnknownTopicOrPartitionException], results.get(nonExistingTopic))
  }

  @Test
  def testDescribeTopicsWithIds(): Unit = {
    client = createAdminClient

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())
    ensureConsistentKRaftMetadata()

    val existingTopicId = brokers.head.metadataCache.getTopicId(existingTopic)

    val nonExistingTopicId = Uuid.randomUuid()

    val results = client.describeTopics(TopicCollection.ofTopicIds(util.List.of(existingTopicId, nonExistingTopicId))).topicIdValues()
    assertEquals(existingTopicId, results.get(existingTopicId).get.topicId())
    assertFutureThrows(classOf[UnknownTopicIdException], results.get(nonExistingTopicId))
  }

  @Test
  def testDescribeTopicsWithNames(): Unit = {
    client = createAdminClient

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())
    ensureConsistentKRaftMetadata()

    val existingTopicId = brokers.head.metadataCache.getTopicId(existingTopic)
    val results = client.describeTopics(TopicCollection.ofTopicNames(util.List.of(existingTopic))).topicNameValues()
    assertEquals(existingTopicId, results.get(existingTopic).get.topicId())
  }

  @Test
  def testDescribeCluster(): Unit = {
    client = createAdminClient
    val result = client.describeCluster
    val nodes = result.nodes.get()
    val clusterId = result.clusterId().get()
    assertEquals(brokers.head.dataPlaneRequestProcessor.clusterId, clusterId)
    val controller = result.controller().get()

    // In KRaft, we return a random brokerId as the current controller.
    val brokerIds = brokers.map(_.config.brokerId).toSet
    assertTrue(brokerIds.contains(controller.id))

    val brokerEndpoints = bootstrapServers().split(",")
    assertEquals(brokerEndpoints.size, nodes.size)
    for (node <- nodes.asScala) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(brokerEndpoints.contains(hostStr), s"Unknown host:port pair $hostStr in brokerVersionInfos")
    }
  }

  @Test
  def testDescribeLogDirs(): Unit = {
    client = createAdminClient
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10)
    val partitionsByBroker = leaderByPartition.groupBy { case (_, leaderId) => leaderId }.map { case (k, v) =>
      k -> v.keys.toSeq
    }
    ensureConsistentKRaftMetadata()
    val brokerIds = (0 until brokerCount).map(Integer.valueOf)
    val logDirInfosByBroker = client.describeLogDirs(brokerIds.asJava).allDescriptions.get

    (0 until brokerCount).foreach { brokerId =>
      val server = brokers.find(_.config.brokerId == brokerId).get
      val expectedPartitions = partitionsByBroker(brokerId)
      val logDirInfos = logDirInfosByBroker.get(brokerId)
      val replicaInfos = logDirInfos.asScala.flatMap { case (_, logDirInfo) =>
        logDirInfo.replicaInfos.asScala
      }.filter { case (k, _) => k.topic == topic }

      assertEquals(expectedPartitions.toSet, replicaInfos.keys.map(_.partition).toSet)
      logDirInfos.forEach { (logDir, logDirInfo) =>
        assertTrue(logDirInfo.totalBytes.isPresent)
        assertTrue(logDirInfo.usableBytes.isPresent)
        logDirInfo.replicaInfos.asScala.keys.foreach(tp =>
          assertEquals(server.logManager.getLog(tp).get.dir.getParent, logDir)
        )
      }
    }
  }

  @Test
  def testDescribeReplicaLogDirs(): Unit = {
    client = createAdminClient
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10)
    val replicas = leaderByPartition.map { case (partition, brokerId) =>
      new TopicPartitionReplica(topic, partition, brokerId)
    }.toSeq
    ensureConsistentKRaftMetadata()

    val replicaDirInfos = client.describeReplicaLogDirs(replicas.asJavaCollection).all.get
    replicaDirInfos.forEach { (topicPartitionReplica, replicaDirInfo) =>
      val server = brokers.find(_.config.brokerId == topicPartitionReplica.brokerId()).get
      val tp = new TopicPartition(topicPartitionReplica.topic(), topicPartitionReplica.partition())
      assertEquals(server.logManager.getLog(tp).get.dir.getParent, replicaDirInfo.getCurrentReplicaLogDir)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAlterReplicaLogDirs(groupProtocol: String): Unit = {
    client = createAdminClient
    val topic = "topic"
    val tp = new TopicPartition(topic, 0)
    val randomNums = brokers.map(server => server -> Random.nextInt(2)).toMap

    // Generate two mutually exclusive replicaAssignment
    val firstReplicaAssignment = brokers.map { server =>
      val logDir = new File(server.config.logDirs.get(randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap
    val secondReplicaAssignment = brokers.map { server =>
      val logDir = new File(server.config.logDirs.get(1 - randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap

    // Verify that replica can be created in the specified log directory
    val futures = client.alterReplicaLogDirs(firstReplicaAssignment.asJava,
      new AlterReplicaLogDirsOptions).values.asScala.values
    futures.foreach { future =>
      val exception = assertThrows(classOf[ExecutionException], () => future.get)
      assertTrue(exception.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    }

    createTopic(topic, replicationFactor = brokerCount)
    ensureConsistentKRaftMetadata()
    brokers.foreach { server =>
      val logDir = server.logManager.getLog(tp).get.dir.getParent
      assertEquals(firstReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)), logDir)
    }

    // Verify that replica can be moved to the specified log directory after the topic has been created
    client.alterReplicaLogDirs(secondReplicaAssignment.asJava, new AlterReplicaLogDirsOptions).all.get
    brokers.foreach { server =>
      TestUtils.waitUntilTrue(() => {
        val logDir = server.logManager.getLog(tp).get.dir.getParent
        secondReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)) == logDir
      }, "timed out waiting for replica movement")
    }

    // Verify that replica can be moved to the specified log directory while the producer is sending messages
    val running = new AtomicBoolean(true)
    val numMessages = new AtomicInteger
    import scala.concurrent.ExecutionContext.Implicits._
    val producerFuture = Future {
      val producer = TestUtils.createProducer(
        bootstrapServers(),
        securityProtocol = securityProtocol,
        trustStoreFile = trustStoreFile,
        retries = 0, // Producer should not have to retry when broker is moving replica between log directories.
        requestTimeoutMs = 10000,
        acks = -1
      )
      try {
        while (running.get) {
          val future = producer.send(new ProducerRecord(topic, s"xxxxxxxxxxxxxxxxxxxx-$numMessages".getBytes))
          numMessages.incrementAndGet()
          future.get(10, TimeUnit.SECONDS)
        }
        numMessages.get
      } finally producer.close()
    }

    try {
      TestUtils.waitUntilTrue(() => numMessages.get > 10, s"only $numMessages messages are produced before timeout. Producer future ${producerFuture.value}")
      client.alterReplicaLogDirs(firstReplicaAssignment.asJava, new AlterReplicaLogDirsOptions).all.get
      brokers.foreach { server =>
        TestUtils.waitUntilTrue(() => {
          val logDir = server.logManager.getLog(tp).get.dir.getParent
          firstReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)) == logDir
        }, s"timed out waiting for replica movement. Producer future ${producerFuture.value}")
      }

      val currentMessagesNum = numMessages.get
      TestUtils.waitUntilTrue(() => numMessages.get - currentMessagesNum > 10,
        s"only ${numMessages.get - currentMessagesNum} messages are produced within timeout after replica movement. Producer future ${producerFuture.value}")
    } finally running.set(false)

    val finalNumMessages = Await.result(producerFuture, Duration(20, TimeUnit.SECONDS))

    // Verify that all messages that are produced can be consumed
    val consumerRecords = TestUtils.consumeTopicRecords(brokers, topic, finalNumMessages,
      GroupProtocol.of(groupProtocol), securityProtocol = securityProtocol, trustStoreFile = trustStoreFile)
    consumerRecords.zipWithIndex.foreach { case (consumerRecord, index) =>
      assertEquals(s"xxxxxxxxxxxxxxxxxxxx-$index", new String(consumerRecord.value))
    }
  }

  @Test
  def testDescribeConfigsNonexistent(): Unit = {
    client = createAdminClient

    val brokerException = assertThrows(classOf[ExecutionException], () => {
      client.describeConfigs(util.List.of(new ConfigResource(ConfigResource.Type.BROKER, "-1"))).all().get()
    })
    assertInstanceOf(classOf[TimeoutException], brokerException.getCause)

    val topicException = assertThrows(classOf[ExecutionException], () => {
      client.describeConfigs(util.List.of(new ConfigResource(ConfigResource.Type.TOPIC, "none_topic"))).all().get()
    })
    assertInstanceOf(classOf[UnknownTopicOrPartitionException], topicException.getCause)

    val brokerLoggerException = assertThrows(classOf[ExecutionException], () => {
      client.describeConfigs(util.List.of(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "-1"))).all().get()
    })
    assertInstanceOf(classOf[TimeoutException], brokerLoggerException.getCause)
  }

  @Test
  def testDescribeConfigsNonexistentForKraft(): Unit = {
    client = createAdminClient

    val groupResource = new ConfigResource(ConfigResource.Type.GROUP, "none_group")
    val groupResult = client.describeConfigs(util.List.of(groupResource)).all().get().get(groupResource)
    assertNotEquals(0, groupResult.entries().size())
  }

  @Test
  def testDescribeAndAlterConfigs(): Unit = {
    client = createAdminClient

    // Create topics
    val topic1 = "describe-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    val maxMessageBytes = "500000"
    val retentionMs = "60000000"
    topicConfig1.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes)
    topicConfig1.setProperty(TopicConfig.RETENTION_MS_CONFIG, retentionMs)
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Describe topics and broker
    val brokerResource1 = new ConfigResource(ConfigResource.Type.BROKER, brokers(1).config.brokerId.toString)
    val brokerResource2 = new ConfigResource(ConfigResource.Type.BROKER, brokers(2).config.brokerId.toString)
    val configResources = util.List.of(topicResource1, topicResource2, brokerResource1, brokerResource2)
    val describeResult = client.describeConfigs(configResources)
    val configs = describeResult.all.get

    assertEquals(4, configs.size)

    val maxMessageBytes1 = configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
    assertEquals(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes1.name)
    assertEquals(topicConfig1.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG), maxMessageBytes1.value)
    assertFalse(maxMessageBytes1.isDefault)
    assertFalse(maxMessageBytes1.isSensitive)
    assertFalse(maxMessageBytes1.isReadOnly)

    assertEquals(topicConfig1.get(TopicConfig.RETENTION_MS_CONFIG),
      configs.get(topicResource1).get(TopicConfig.RETENTION_MS_CONFIG).value)

    val maxMessageBytes2 = configs.get(topicResource2).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
    assertEquals(ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT.toString, maxMessageBytes2.value)
    assertEquals(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes2.name)
    assertTrue(maxMessageBytes2.isDefault)
    assertFalse(maxMessageBytes2.isSensitive)
    assertFalse(maxMessageBytes2.isReadOnly)

    // Find the number of internal configs that we have explicitly set in the broker config.
    // These will appear when we describe the broker configuration. Other internal configs,
    // that we have not set, will not appear there.
    val numInternalConfigsSet = brokers.head.config.originals.keySet().asScala.count(k => {
      Option(KafkaConfig.configDef.configKeys().get(k)) match {
        case None => false
        case Some(configDef) => configDef.internalConfig
      }
    })
    assertEquals(brokers(1).config.nonInternalValues.size + numInternalConfigsSet,
      configs.get(brokerResource1).entries.size)
    assertEquals(brokers(1).config.brokerId.toString, configs.get(brokerResource1).get(ServerConfigs.BROKER_ID_CONFIG).value)
    val listenerSecurityProtocolMap = configs.get(brokerResource1).get(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)
    assertEquals(brokers(1).config.getString(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG), listenerSecurityProtocolMap.value)
    assertEquals(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, listenerSecurityProtocolMap.name)
    assertFalse(listenerSecurityProtocolMap.isDefault)
    assertFalse(listenerSecurityProtocolMap.isSensitive)
    assertFalse(listenerSecurityProtocolMap.isReadOnly)
    val truststorePassword = configs.get(brokerResource1).get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    assertEquals(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword.name)
    assertNull(truststorePassword.value)
    assertFalse(truststorePassword.isDefault)
    assertTrue(truststorePassword.isSensitive)
    assertFalse(truststorePassword.isReadOnly)
    val compressionType = configs.get(brokerResource1).get(ServerConfigs.COMPRESSION_TYPE_CONFIG)
    assertEquals(brokers(1).config.compressionType, compressionType.value)
    assertEquals(ServerConfigs.COMPRESSION_TYPE_CONFIG, compressionType.name)
    assertTrue(compressionType.isDefault)
    assertFalse(compressionType.isSensitive)
    assertFalse(compressionType.isReadOnly)

    assertEquals(brokers(2).config.nonInternalValues.size + numInternalConfigsSet,
      configs.get(brokerResource2).entries.size)
    assertEquals(brokers(2).config.brokerId.toString, configs.get(brokerResource2).get(ServerConfigs.BROKER_ID_CONFIG).value)
    assertEquals(brokers(2).config.logCleanerThreads.toString,
      configs.get(brokerResource2).get(CleanerConfig.LOG_CLEANER_THREADS_PROP).value)

    checkValidAlterConfigs(client, this, topicResource1, topicResource2, maxMessageBytes, retentionMs)
  }

  @Test
  def testIncrementalAlterAndDescribeGroupConfigs(): Unit = {
    client = createAdminClient
    val group = "describe-alter-configs-group"
    val groupResource = new ConfigResource(ConfigResource.Type.GROUP, group)

    // Alter group configs
    var groupAlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, ""), AlterConfigOp.OpType.DELETE)
    )

    var alterResult = client.incrementalAlterConfigs(util.Map.of(
      groupResource, groupAlterConfigs
    ))

    assertEquals(util.Set.of(groupResource), alterResult.values.keySet)
    alterResult.all.get(15, TimeUnit.SECONDS)

    ensureConsistentKRaftMetadata()

    // Describe group config, verify that group config was updated correctly
    var describeResult = client.describeConfigs(util.List.of(groupResource))
    var configs = describeResult.all.get(15, TimeUnit.SECONDS)

    assertEquals(1, configs.size)

    assertEquals("50000", configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).value)
    assertEquals(ConfigSource.DYNAMIC_GROUP_CONFIG, configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).source)
    assertEquals(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString, configs.get(groupResource).get(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG).value)
    assertEquals(ConfigSource.DEFAULT_CONFIG, configs.get(groupResource).get(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG).source)

    // Alter group with validateOnly=true
    groupAlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "60000"), AlterConfigOp.OpType.SET)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      groupResource, groupAlterConfigs
    ), new AlterConfigsOptions().validateOnly(true))
    alterResult.all.get(15, TimeUnit.SECONDS)

    // Verify that group config was not updated due to validateOnly = true
    describeResult = client.describeConfigs(util.List.of(groupResource))
    configs = describeResult.all.get(15, TimeUnit.SECONDS)

    assertEquals("50000", configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).value)

    // Alter group with validateOnly=true with invalid configs
    groupAlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "5"), AlterConfigOp.OpType.SET)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      groupResource, groupAlterConfigs
    ), new AlterConfigsOptions().validateOnly(true))

    assertFutureThrows(classOf[InvalidConfigurationException],
      alterResult.values.get(groupResource),
      "consumer.session.timeout.ms must be greater than or equal to group.consumer.min.session.timeout.ms")
  }

  @Test
  def testCreatePartitions(): Unit = {
    client = createAdminClient

    // Create topics
    val topic1 = "create-partitions-topic-1"
    createTopic(topic1)

    val topic2 = "create-partitions-topic-2"
    createTopic(topic2, replicationFactor = 2)

    // assert that both the topics have 1 partition
    val topic1_metadata = getTopicMetadata(client, topic1)
    val topic2_metadata = getTopicMetadata(client, topic2)
    assertEquals(1, topic1_metadata.partitions.size)
    assertEquals(1, topic2_metadata.partitions.size)

    val validateOnly = new CreatePartitionsOptions().validateOnly(true)
    val actuallyDoIt = new CreatePartitionsOptions().validateOnly(false)

    def partitions(topic: String, expectedNumPartitionsOpt: Option[Int]): util.List[TopicPartitionInfo] = {
      getTopicMetadata(client, topic, expectedNumPartitionsOpt = expectedNumPartitionsOpt).partitions
    }

    def numPartitions(topic: String, expectedNumPartitionsOpt: Option[Int]): Int = partitions(topic, expectedNumPartitionsOpt).size

    // validateOnly: try creating a new partition (no assignments), to bring the total to 3 partitions
    var alterResult = client.createPartitions(util.Map.of(topic1,
      NewPartitions.increaseTo(3)), validateOnly)
    var altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 1)

    // try creating a new partition (no assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(util.Map.of(topic1,
      NewPartitions.increaseTo(3)), actuallyDoIt)
    altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 3)

    // validateOnly: now try creating a new partition (with assignments), to bring the total to 3 partitions
    val newPartition2Assignments = util.List.of[util.List[Integer]](util.List.of[Integer](0, 1), util.List.of[Integer](1, 2))
    alterResult = client.createPartitions(util.Map.of(topic2,
      NewPartitions.increaseTo(3, newPartition2Assignments)), validateOnly)
    altered = alterResult.values.get(topic2).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic2, expectedNumPartitions = 1)

    // now try creating a new partition (with assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(util.Map.of(topic2,
      NewPartitions.increaseTo(3, newPartition2Assignments)), actuallyDoIt)
    altered = alterResult.values.get(topic2).get
    val actualPartitions2 = partitions(topic2, expectedNumPartitionsOpt = Some(3))
    assertEquals(3, actualPartitions2.size)
    assertEquals(Seq(0, 1), actualPartitions2.get(1).replicas.asScala.map(_.id).toList)
    assertEquals(Seq(1, 2), actualPartitions2.get(2).replicas.asScala.map(_.id).toList)

    // loop over error cases calling with+without validate-only
    for (option <- Seq(validateOnly, actuallyDoIt)) {
      val desc = if (option.validateOnly()) "validateOnly" else "validateOnly=false"

      // try a newCount which would be a decrease
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(1)), option)

      var e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when newCount is a decrease")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      var exceptionMsgStr = "The topic create-partitions-topic-1 currently has 3 partition(s); 1 would not be an increase."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (without assignment)
      alterResult = client.createPartitions(util.Map.of(topic2,
        NewPartitions.increaseTo(3)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get,
        () => s"$desc: Expect InvalidPartitionsException when requesting a noop")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      exceptionMsgStr = "Topic already has 3 partition(s)."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (where the assignment matches current state)
      alterResult = client.createPartitions(util.Map.of(topic2,
        NewPartitions.increaseTo(3, newPartition2Assignments)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (where the assignment doesn't match current state)
      alterResult = client.createPartitions(util.Map.of(topic2,
        NewPartitions.increaseTo(3, newPartition2Assignments.asScala.reverse.toList.asJava)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a bad topic name
      val unknownTopic = "an-unknown-topic"
      alterResult = client.createPartitions(util.Map.of(unknownTopic,
        NewPartitions.increaseTo(2)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(unknownTopic).get,
        () => s"$desc: Expect InvalidTopicException when using an unknown topic")
      assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException], desc)
      exceptionMsgStr = "This server does not host this topic-partition."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)

      // try an invalid newCount
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(-22)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when newCount is invalid")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      exceptionMsgStr = "The topic create-partitions-topic-1 currently has 3 partition(s); -22 would not be an increase."
      assertEquals(exceptionMsgStr, e.getCause.getMessage,
        desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments where the number of brokers != replication factor
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(4, util.List.of(util.List.of[Integer](1, 2)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when #brokers != replication factor")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "The manual partition assignment includes a partition with 2 replica(s), but this is not " +
          "consistent with previous partitions, which have 1 replica(s)."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try #assignments < with the increase
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(6, util.List.of(util.List.of[Integer](1)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "Attempted to add 3 additional partition(s), but only 1 assignment(s) were specified."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try #assignments > with the increase
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(4, util.List.of(util.List.of[Integer](1), util.List.of[Integer](2)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      exceptionMsgStr = "Attempted to add 1 additional partition(s), but only 2 assignment(s) were specified."
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try with duplicate brokers in assignments
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(4, util.List.of(util.List.of[Integer](1, 1)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments has duplicate brokers")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "The manual partition assignment includes the broker 1 more than once."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments with differently sized inner lists
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(5, util.List.of(util.List.of[Integer](1), util.List.of[Integer](1, 0)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments have differently sized inner lists")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "The manual partition assignment includes a partition with 2 replica(s), but this is not " +
          "consistent with previous partitions, which have 1 replica(s)."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments with unknown brokers
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(4, util.List.of(util.List.of[Integer](12)))), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments contains an unknown broker")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "The manual partition assignment includes broker 12, but no such broker is registered."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try with empty assignments
      alterResult = client.createPartitions(util.Map.of(topic1,
        NewPartitions.increaseTo(4, util.List.of)), option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments is empty")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = "Attempted to add 1 additional partition(s), but only 0 assignment(s) were specified."
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)
    }

    // a mixed success, failure response
    alterResult = client.createPartitions(util.Map.of(
      topic1, NewPartitions.increaseTo(4),
      topic2, NewPartitions.increaseTo(2)), actuallyDoIt)
    // assert that the topic1 now has 4 partitions
    altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 4)
    var e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
    assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException])
    val exceptionMsgStr = "The topic create-partitions-topic-2 currently has 3 partition(s); 2 would not be an increase."
    assertEquals(exceptionMsgStr, e.getCause.getMessage)
    TestUtils.waitForAllPartitionsMetadata(brokers, topic2, expectedNumPartitions = 3)

    // Delete the topic. Verify addition of partitions to deleted topic is not possible.
    // In KRaft, the deletion occurs immediately and hence we have a different Exception thrown in the response.
    val deleteResult = client.deleteTopics(util.List.of(topic1))
    deleteResult.topicNameValues.get(topic1).get
    alterResult = client.createPartitions(util.Map.of(topic1,
      NewPartitions.increaseTo(4)), validateOnly)
    e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
      () => "Expect InvalidTopicException or UnknownTopicOrPartitionException when the topic is queued for deletion")
    assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException], e.toString)
    assertEquals("This server does not host this topic-partition.", e.getCause.getMessage)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSeekAfterDeleteRecords(groupProtocol: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = createAdminClient

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    consumer.seekToBeginning(util.Set.of(topicPartition))
    assertEquals(0L, consumer.position(topicPartition))

    val result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(5L)))
    val lowWatermark = result.lowWatermarks().get(topicPartition).get.lowWatermark
    assertEquals(5L, lowWatermark)

    consumer.seekToBeginning(util.List.of(topicPartition))
    assertEquals(5L, consumer.position(topicPartition))

    consumer.seek(topicPartition, 7L)
    assertEquals(7L, consumer.position(topicPartition))

    client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(DeleteRecordsRequest.HIGH_WATERMARK))).all.get
    consumer.seekToBeginning(util.List.of(topicPartition))
    assertEquals(10L, consumer.position(topicPartition))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testLogStartOffsetCheckpoint(groupProtocol: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = createAdminClient

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    var result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(5L)))
    var lowWatermark: Option[Long] = Some(result.lowWatermarks.get(topicPartition).get.lowWatermark)
    assertEquals(Some(5), lowWatermark)

    for (i <- 0 until brokerCount) {
      killBroker(i)
    }
    restartDeadBrokers()

    client.close()
    client = createAdminClient

    TestUtils.waitUntilTrue(() => {
      // Need to retry if leader is not available for the partition
      result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(0L)))

      lowWatermark = None
      val future = result.lowWatermarks().get(topicPartition)
      try {
        lowWatermark = Some(future.get.lowWatermark)
        lowWatermark.contains(5L)
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[LeaderNotAvailableException] ||
          e.getCause.isInstanceOf[NotLeaderOrFollowerException] => false
        }
    }, s"Expected low watermark of the partition to be 5 but got ${lowWatermark.getOrElse("no response within the timeout")}")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testLogStartOffsetAfterDeleteRecords(groupProtocol: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = createAdminClient

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    val result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(3L)))
    val lowWatermark = result.lowWatermarks.get(topicPartition).get.lowWatermark
    assertEquals(3L, lowWatermark)

    for (i <- 0 until brokerCount)
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
  }

  @Test
  def testReplicaCanFetchFromLogStartOffsetAfterDeleteRecords(): Unit = {
    val leaders = createTopic(topic, replicationFactor = brokerCount)
    val followerIndex = if (leaders(0) != brokers.head.config.brokerId) 0 else 1

    def waitForFollowerLog(expectedStartOffset: Long, expectedEndOffset: Long): Unit = {
      TestUtils.waitUntilTrue(() => brokers(followerIndex).replicaManager.localLog(topicPartition).isDefined,
                              "Expected follower to create replica for partition")

      // wait until the follower discovers that log start offset moved beyond its HW
      TestUtils.waitUntilTrue(() => {
        brokers(followerIndex).replicaManager.localLog(topicPartition).get.logStartOffset == expectedStartOffset
      }, s"Expected follower to discover new log start offset $expectedStartOffset")

      TestUtils.waitUntilTrue(() => {
        brokers(followerIndex).replicaManager.localLog(topicPartition).get.logEndOffset == expectedEndOffset
      }, s"Expected follower to catch up to log end offset $expectedEndOffset")
    }

    // we will produce to topic and delete records while one follower is down
    killBroker(followerIndex)

    client = createAdminClient
    val producer = createProducer()
    sendRecords(producer, 100, topicPartition)

    val result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(3L)))
    result.all().get()

    // start the stopped broker to verify that it will be able to fetch from new log start offset
    restartDeadBrokers()

    waitForFollowerLog(expectedStartOffset=3L, expectedEndOffset=100L)

    // after the new replica caught up, all replicas should have same log start offset
    for (i <- 0 until brokerCount)
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)

    // kill the same follower again, produce more records, and delete records beyond follower's LOE
    killBroker(followerIndex)
    sendRecords(producer, 100, topicPartition)
    val result1 = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(117L)))
    result1.all().get()
    restartDeadBrokers()
    TestUtils.waitForBrokersInIsr(client, topicPartition, Set(followerIndex))
    waitForFollowerLog(expectedStartOffset=117L, expectedEndOffset=200L)
  }

  @Test
  def testAlterLogDirsAfterDeleteRecords(): Unit = {
    client = createAdminClient
    createTopic(topic, replicationFactor = brokerCount)
    val expectedLEO = 100
    val producer = createProducer()
    sendRecords(producer, expectedLEO, topicPartition)

    // delete records to move log start offset
    val result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(3L)))
    result.all().get()
    // make sure we are in the expected state after delete records
    for (i <- 0 until brokerCount) {
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
      assertEquals(expectedLEO, brokers(i).replicaManager.localLog(topicPartition).get.logEndOffset)
    }

    // we will create another dir just for one server
    val futureLogDir = brokers(0).config.logDirs.get(1)
    val futureReplica = new TopicPartitionReplica(topic, 0, brokers(0).config.brokerId)

    // Verify that replica can be moved to the specified log directory
    client.alterReplicaLogDirs(util.Map.of(futureReplica, futureLogDir)).all.get
    TestUtils.waitUntilTrue(() => {
      futureLogDir == brokers(0).logManager.getLog(topicPartition).get.dir.getParent
    }, "timed out waiting for replica movement")

    // once replica moved, its LSO and LEO should match other replicas
    assertEquals(3, brokers.head.replicaManager.localLog(topicPartition).get.logStartOffset)
    assertEquals(expectedLEO, brokers.head.replicaManager.localLog(topicPartition).get.logEndOffset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetsForTimesAfterDeleteRecords(groupProtocol: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = createAdminClient

    val props = new Properties()
    val consumer = createConsumer(configOverrides = props)
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    var returnedOffsets = consumer.offsetsForTimes(util.Map.of(topicPartition, JLong.valueOf(0L)))
    assertTrue(returnedOffsets.containsKey(topicPartition))
    assertEquals(0L, returnedOffsets.get(topicPartition).offset())

    var result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(5L)))
    result.all.get
    returnedOffsets = consumer.offsetsForTimes(util.Map.of(topicPartition, JLong.valueOf(0L)))
    assertTrue(returnedOffsets.containsKey(topicPartition))
    assertEquals(5L, returnedOffsets.get(topicPartition).offset())

    result = client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(DeleteRecordsRequest.HIGH_WATERMARK)))
    result.all.get
    returnedOffsets = consumer.offsetsForTimes(util.Map.of(topicPartition, JLong.valueOf(0L)))
    assertTrue(returnedOffsets.containsKey(topicPartition))
    assertNull(returnedOffsets.get(topicPartition))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteRecordsAfterCorruptRecords(groupProtocol: String): Unit = {
    val config = new Properties()
    config.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, "200")
    createTopic(topic, numPartitions = 1, replicationFactor = 1, config)

    client = createAdminClient

    val producer = createProducer()
    def sendRecords(begin: Int, end: Int) = {
      val futures = (begin until end).map( i => {
        val record = new ProducerRecord(topic, partition, s"$i".getBytes, s"$i".getBytes)
        producer.send(record)
      })
      futures.foreach(_.get)
    }
    sendRecords(0, 10)
    sendRecords(10, 20)

    val topicDesc = client.describeTopics(util.List.of(topic)).allTopicNames().get().get(topic)
    assertEquals(1, topicDesc.partitions().size())
    val partitionLeaderId = topicDesc.partitions().get(0).leader().id()
    val logDirMap = client.describeLogDirs(util.List.of(partitionLeaderId))
      .allDescriptions().get().get(partitionLeaderId)
    val logDir = logDirMap.entrySet.stream
      .filter(entry => entry.getValue.replicaInfos.containsKey(topicPartition)).findAny().get().getKey
    // retrieve the path of the first segment
    val logFilePath = LogFileUtils.logFile(Paths.get(logDir).resolve(topicPartition.toString).toFile, 0).toPath
    val firstSegmentRecordsSize = FileRecords.open(logFilePath.toFile).records().asScala.iterator.size
    assertTrue(firstSegmentRecordsSize > 0)

    // manually load the inactive segment file to corrupt the data
    val originalContent = Files.readAllBytes(logFilePath)
    val newContent = ByteBuffer.allocate(JLong.BYTES + Integer.BYTES + originalContent.length)
    newContent.putLong(0) // offset
    newContent.putInt(0) // size -> this will make FileLogInputStream throw "Found record size 0 smaller than minimum record..."
    newContent.put(Files.readAllBytes(logFilePath))
    newContent.flip()
    Files.write(logFilePath, newContent.array(), StandardOpenOption.TRUNCATE_EXISTING)

    val overrideConfig = new Properties
    overrideConfig.setProperty("auto.offset.reset", "earliest")
    val consumer = createConsumer(configOverrides = overrideConfig)
    consumer.subscribe(util.List.of(topic))
    assertEquals("Encountered corrupt message when fetching offset 0 for topic-partition topic-0",
      assertThrows(classOf[KafkaException], () => consumer.poll(JDuration.ofMillis(DEFAULT_MAX_WAIT_MS))).getMessage)

    val partitionFollowerId = brokers.map(b => b.config.nodeId).filter(id => id != partitionLeaderId).head
    val newAssignment = util.Map.of(topicPartition, Optional.of(new NewPartitionReassignment(
      util.List.of(Integer.valueOf(partitionLeaderId), Integer.valueOf(partitionFollowerId)))))

    // add follower to topic partition
    client.alterPartitionReassignments(newAssignment).all().get()
    // delete records in corrupt segment (the first segment)
    client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(firstSegmentRecordsSize))).all.get
    // verify reassignment is finished after delete records
    TestUtils.waitForBrokersInIsr(client, topicPartition, Set(partitionLeaderId, partitionFollowerId))
    // seek to beginning and make sure we can consume all records
    consumer.seekToBeginning(util.List.of(topicPartition))
    assertEquals(19, TestUtils.consumeRecords(consumer, 20 - firstSegmentRecordsSize).last.offset())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumeAfterDeleteRecords(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = createAdminClient

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    var messageCount = 0
    TestUtils.consumeRecords(consumer, 10)

    client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(3L))).all.get
    consumer.seek(topicPartition, 1)
    messageCount = 0
    TestUtils.consumeRecords(consumer, 7)

    client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(8L))).all.get
    consumer.seek(topicPartition, 1)
    messageCount = 0
    TestUtils.consumeRecords(consumer, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteRecordsWithException(groupProtocol: String): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = createAdminClient

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    assertEquals(5L, client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(5L)))
      .lowWatermarks.get(topicPartition).get.lowWatermark)

    // OffsetOutOfRangeException if offset > high_watermark
    val cause = assertThrows(classOf[ExecutionException],
      () => client.deleteRecords(util.Map.of(topicPartition, RecordsToDelete.beforeOffset(20L))).lowWatermarks.get(topicPartition).get).getCause
    assertEquals(classOf[OffsetOutOfRangeException], cause.getClass)
  }

  @Test
  def testDescribeConfigsForTopic(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)
    client = createAdminClient

    val existingTopic = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    client.describeConfigs(util.List.of(existingTopic)).values.get(existingTopic).get()

    val defaultTopic = new ConfigResource(ConfigResource.Type.TOPIC, "")
    var describeResult = client.describeConfigs(util.List.of(defaultTopic))
    assertFutureThrows(classOf[InvalidTopicException], describeResult.all())

    val nonExistentTopic = new ConfigResource(ConfigResource.Type.TOPIC, "unknown")
    describeResult = client.describeConfigs(util.List.of(nonExistentTopic))
    assertFutureThrows(classOf[UnknownTopicOrPartitionException], describeResult.all())

    val invalidTopic = new ConfigResource(ConfigResource.Type.TOPIC, "(invalid topic)")
    describeResult = client.describeConfigs(util.List.of(invalidTopic))
    assertFutureThrows(classOf[InvalidTopicException], describeResult.all())
  }

  @Test
  def testIncludeDocumentation(): Unit = {
    createTopic(topic)
    client = createAdminClient

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val resources = util.List.of(resource)
    val includeDocumentation = new DescribeConfigsOptions().includeDocumentation(true)
    var describeConfigs = client.describeConfigs(resources, includeDocumentation)
    var configEntries = describeConfigs.values().get(resource).get().entries()
    configEntries.forEach(e => assertNotNull(e.documentation()))

    val excludeDocumentation = new DescribeConfigsOptions().includeDocumentation(false)
    describeConfigs = client.describeConfigs(resources, excludeDocumentation)
    configEntries = describeConfigs.values().get(resource).get().entries()
    configEntries.forEach(e => assertNull(e.documentation()))
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    consumer.subscribe(util.List.of(topic))
    TestUtils.pollUntilTrue(consumer, () => !consumer.assignment.isEmpty, "Expected non-empty assignment")
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          topicPartition: TopicPartition): Unit = {
    val futures = (0 until numRecords).map( i => {
      val record = new ProducerRecord(topicPartition.topic, topicPartition.partition, s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    })

    futures.foreach(_.get)
  }

  @Test
  def testInvalidAlterConfigs(): Unit = {
    client = createAdminClient
    checkInvalidAlterConfigs(this, client)
  }

  /**
   * Test that ACL operations are not possible when the authorizer is disabled.
   * Also see [[kafka.api.SaslSslAdminIntegrationTest.testAclOperations()]] for tests of ACL operations
   * when the authorizer is enabled.
   */
  @Test
  def testAclOperations(): Unit = {
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    client = createAdminClient
    assertFutureThrows(classOf[SecurityDisabledException], client.describeAcls(AclBindingFilter.ANY).values())
    assertFutureThrows(classOf[SecurityDisabledException], client.createAcls(util.Set.of(acl)).all())
    assertFutureThrows(classOf[SecurityDisabledException], client.deleteAcls(util.Set.of(acl.toFilter)).all())
  }

  /**
    * Test closing the AdminClient with a generous timeout.  Calls in progress should be completed,
    * since they can be done within the timeout.  New calls should receive exceptions.
    */
  @Test
  def testDelayedClose(): Unit = {
    client = createAdminClient
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = topics.map(new NewTopic(_, 1, 1.toShort))
    val future = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    client.close(time.Duration.ofHours(2))
    val future2 = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureThrows(classOf[IllegalStateException], future2)
    future.get
    client.close(time.Duration.ofMinutes(30)) // multiple close-with-timeout should have no effect
  }

  /**
    * Test closing the AdminClient with a timeout of 0, when there are calls with extremely long
    * timeouts in progress.  The calls should be aborted after the hard shutdown timeout elapses.
    */
  @Test
  def testForceClose(): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    client = Admin.create(config)
    // Because the bootstrap servers are set up incorrectly, this call will not complete, but must be
    // cancelled by the close operation.
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(900000)).all()
    client.close(time.Duration.ZERO)
    assertFutureThrows(classOf[TimeoutException], future)
  }

  /**
    * Check that a call with a timeout does not complete before the minimum timeout has elapsed,
    * even when the default request timeout is shorter.
    */
  @Test
  def testMinimumRequestTimeouts(): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "0")
    client = Admin.create(config)
    val startTimeMs = Time.SYSTEM.milliseconds()
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(2)).all()
    assertFutureThrows(classOf[TimeoutException], future)
    val endTimeMs = Time.SYSTEM.milliseconds()
    assertTrue(endTimeMs > startTimeMs, "Expected the timeout to take at least one millisecond.")
  }

  /**
    * Test injecting timeouts for calls that are in flight.
    */
  @Test
  def testCallInFlightTimeouts(): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000000")
    config.put(AdminClientConfig.RETRIES_CONFIG, "0")
    val factory = new KafkaAdminClientTest.FailureInjectingTimeoutProcessorFactory()
    client = KafkaAdminClientTest.createInternal(new AdminClientConfig(config), factory)
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
        new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureThrows(classOf[TimeoutException], future)
    val future2 = client.createTopics(Seq("mytopic3", "mytopic4").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().validateOnly(true)).all()
    future2.get
    assertEquals(1, factory.failuresInjected)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testListConsumerGroupOffsets(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      assertConsumerGroupsIsClean()

      val testTopicName = "test_topic"
      prepareTopics(List(testTopicName), 2)
      prepareRecords(testTopicName)

      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val groupInstances = Set("")
      val topics = Set(testTopicName)

      // We need to disable the auto commit because after the members got removed from group, the offset commit
      // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
      val defaultConsumerConfig = new Properties(consumerConfig)
      defaultConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      defaultConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      defaultConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      val backgroundConsumers = prepareConsumers(groupInstances, topics, defaultConsumerConfig)

      try {
        // Start consumer polling threads in the background
        backgroundConsumers.start()
        val topicPartition = new TopicPartition(testTopicName, 0)

        // Test listConsumerGroupOffsets
        TestUtils.waitUntilTrue(() => {
          val parts = client.listConsumerGroupOffsets(testGroupId).partitionsToOffsetAndMetadata().get()
          parts.containsKey(topicPartition) && (parts.get(topicPartition).offset() == 1)
        }, "Expected the offset for partition 0 to eventually become 1.")

        // Test listConsumerGroupOffsets with requireStable true
        val options = new ListConsumerGroupOffsetsOptions().requireStable(true)
        var parts = client.listConsumerGroupOffsets(testGroupId, options)
          .partitionsToOffsetAndMetadata()
          .get()
        assertTrue(parts.containsKey(topicPartition))
        assertEquals(1, parts.get(topicPartition).offset())

        // Test listConsumerGroupOffsets with listConsumerGroupOffsetsSpec
        val groupSpecs = util.Map.of(
          testGroupId,
          new ListConsumerGroupOffsetsSpec().topicPartitions(util.List.of(new TopicPartition(testTopicName, 0)))
        )
        parts = client.listConsumerGroupOffsets(groupSpecs)
          .partitionsToOffsetAndMetadata()
          .get()
        assertTrue(parts.containsKey(topicPartition))
        assertEquals(1, parts.get(topicPartition).offset())

        // Test listConsumerGroupOffsets with listConsumerGroupOffsetsSpec and requireStable option
        parts = client.listConsumerGroupOffsets(groupSpecs, options)
          .partitionsToOffsetAndMetadata()
          .get()
        assertTrue(parts.containsKey(topicPartition))
        assertEquals(1, parts.get(topicPartition).offset())
      } finally {
        backgroundConsumers.close()
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testListConsumerGroups(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      assertConsumerGroupsIsClean()

      val testTopicName = "test_topic"
      prepareTopics(List(testTopicName), 2)

      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val groupInstances = Set("")
      val topics = Set(testTopicName)

      // We need to disable the auto commit because after the members got removed from group, the offset commit
      // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
      val defaultConsumerConfig = new Properties(consumerConfig)
      defaultConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      defaultConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      defaultConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      val backgroundConsumers = prepareConsumers(groupInstances, topics, defaultConsumerConfig)

      try {
        val groupType = if (groupProtocol.equalsIgnoreCase(GroupProtocol.CONSUMER.name)) GroupType.CONSUMER else GroupType.CLASSIC
        // Start consumer polling threads in the background
        backgroundConsumers.start()

        // Test that we can list the new group.
        TestUtils.waitUntilTrue(() => {
          val matching = client.listConsumerGroups.all.get.asScala.filter(group =>
            group.groupId == testGroupId && group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().withTypes(util.Set.of(groupType))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in group type $groupType")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().withTypes(util.Set.of(groupType))
            .inGroupStates(util.Set.of(GroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId && group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in group type $groupType and state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inGroupStates(util.Set.of(GroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId && group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inGroupStates(util.Set.of(GroupState.EMPTY))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(_.groupId == testGroupId)
          matching.isEmpty
        }, "Expected to find zero groups")
      } finally {
        backgroundConsumers.close()
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDescribeGroups(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      assertConsumerGroupsIsClean()

      val testTopicName = "test_topic"
      val testTopicName1 = testTopicName + "1"
      val testTopicName2 = testTopicName + "2"
      val testNumPartitions = 2
      prepareTopics(List(testTopicName, testTopicName1, testTopicName2), testNumPartitions)

      val producer = createProducer()
      try {
        producer.send(new ProducerRecord(testTopicName, 0, null, null)).get()
      } finally {
        Utils.closeQuietly(producer, "producer")
      }

      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val testInstanceId1 = "test_instance_id_1"
      val testInstanceId2 = "test_instance_id_2"
      val fakeGroupId = "fake_group_id"

      // contains two static members and one dynamic member
      val groupInstances = Set(testInstanceId1, testInstanceId2, "")
      val topics = Set(testTopicName, testTopicName1, testTopicName2)

      // We need to disable the auto commit because after the members got removed from group, the offset commit
      // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
      val defaultConsumerConfig = new Properties(consumerConfig)
      defaultConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      defaultConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      defaultConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      val backgroundConsumers = prepareConsumers(groupInstances, topics, defaultConsumerConfig)

      try {
        val groupType = if (groupProtocol.equalsIgnoreCase(GroupProtocol.CONSUMER.name)) GroupType.CONSUMER else GroupType.CLASSIC
        // Start consumer polling threads in the background
        backgroundConsumers.start()

        val describeWithFakeGroupResult = client.describeConsumerGroups(util.List.of(testGroupId, fakeGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
        assertEquals(2, describeWithFakeGroupResult.describedGroups().size())

        // Test that we can get information about the test consumer group.
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(testGroupId))
        val testGroupDescription = describeWithFakeGroupResult.describedGroups().get(testGroupId).get()
        if (groupType == GroupType.CLASSIC) {
          assertTrue(testGroupDescription.groupEpoch.isEmpty)
          assertTrue(testGroupDescription.targetAssignmentEpoch.isEmpty)
        } else {
          assertEquals(Optional.of(3), testGroupDescription.groupEpoch)
          assertEquals(Optional.of(3), testGroupDescription.targetAssignmentEpoch)
        }

        assertEquals(testGroupId, testGroupDescription.groupId())
        assertFalse(testGroupDescription.isSimpleConsumerGroup)
        assertEquals(groupInstances.size, testGroupDescription.members().size())
        val members = testGroupDescription.members()
        members.asScala.foreach { member =>
          assertEquals(testClientId, member.clientId)
          assertEquals(if (groupType == GroupType.CLASSIC) Optional.empty else Optional.of(true), member.upgraded)
        }
        val topicPartitionsByTopic = members.asScala.flatMap(_.assignment().topicPartitions().asScala).groupBy(_.topic())
        topics.foreach(topic => assertEquals(testNumPartitions, topicPartitionsByTopic.getOrElse(topic, List.empty).size))

        val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP)
        assertEquals(expectedOperations, testGroupDescription.authorizedOperations())

        // Test that the fake group throws GroupIdNotFoundException
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(fakeGroupId))
        assertFutureThrows(classOf[GroupIdNotFoundException], describeWithFakeGroupResult.describedGroups().get(fakeGroupId),
          s"Group $fakeGroupId not found.")

        // Test that all() also throws GroupIdNotFoundException
        assertFutureThrows(classOf[GroupIdNotFoundException], describeWithFakeGroupResult.all(),
          s"Group $fakeGroupId not found.")
      } finally {
        backgroundConsumers.close()
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  /**
   * Test the consumer group APIs.
   */
  @Test
  def testConsumerGroupWithMemberMigration(): Unit = {
    val config = createConfig
    client = Admin.create(config)
    var classicConsumer: Consumer[Array[Byte], Array[Byte]] = null
    var consumerConsumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      // Verify that initially there are no consumer groups to list.
      val list1 = client.listConsumerGroups
      assertEquals(0, list1.all.get.size)
      assertEquals(0, list1.errors.get.size)
      assertEquals(0, list1.valid.get.size)
      val testTopicName = "test_topic"
      val testNumPartitions = 2

      client.createTopics(util.List.of(
        new NewTopic(testTopicName, testNumPartitions, 1.toShort),
      )).all.get
      waitForTopics(client, List(testTopicName), List())

      val producer = createProducer()
      try {
        producer.send(new ProducerRecord(testTopicName, 0, null, null))
        producer.send(new ProducerRecord(testTopicName, 1, null, null))
        producer.flush()
      } finally {
        Utils.closeQuietly(producer, "producer")
      }

      val testGroupId = "test_group_id"
      val testClassicClientId = "test_classic_client_id"
      val testConsumerClientId = "test_consumer_client_id"

      val newConsumerConfig = new Properties(consumerConfig)
      newConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newConsumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, testClassicClientId)
      consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name)

      classicConsumer = createConsumer(configOverrides = newConsumerConfig)
      classicConsumer.subscribe(util.List.of(testTopicName))
      classicConsumer.poll(JDuration.ofMillis(1000))

      newConsumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, testConsumerClientId)
      consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name)
      consumerConsumer = createConsumer(configOverrides = newConsumerConfig)
      consumerConsumer.subscribe(util.List.of(testTopicName))
      consumerConsumer.poll(JDuration.ofMillis(1000))

      TestUtils.waitUntilTrue(() => {
        classicConsumer.poll(JDuration.ofMillis(100))
        consumerConsumer.poll(JDuration.ofMillis(100))
        val describeConsumerGroupResult = client.describeConsumerGroups(util.List.of(testGroupId)).all.get
        describeConsumerGroupResult.containsKey(testGroupId) &&
          describeConsumerGroupResult.get(testGroupId).groupState == GroupState.STABLE &&
          describeConsumerGroupResult.get(testGroupId).members.size == 2
      }, s"Expected to find 2 members in a stable group $testGroupId")

      val describeConsumerGroupResult = client.describeConsumerGroups(util.List.of(testGroupId)).all.get
      val group = describeConsumerGroupResult.get(testGroupId)
      assertNotNull(group)
      assertEquals(Optional.of(2), group.groupEpoch)
      assertEquals(Optional.of(2), group.targetAssignmentEpoch)

      val classicMember = group.members.asScala.find(_.clientId == testClassicClientId)
      assertTrue(classicMember.isDefined)
      assertEquals(Optional.of(2), classicMember.get.memberEpoch)
      assertEquals(Optional.of(false), classicMember.get.upgraded)

      val consumerMember = group.members.asScala.find(_.clientId == testConsumerClientId)
      assertTrue(consumerMember.isDefined)
      assertEquals(Optional.of(2), consumerMember.get.memberEpoch)
      assertEquals(Optional.of(true), consumerMember.get.upgraded)
    } finally {
      Utils.closeQuietly(classicConsumer, "classicConsumer")
      Utils.closeQuietly(consumerConsumer, "consumerConsumer")
      Utils.closeQuietly(client, "adminClient")
    }
  }

  /**
   * Test the consumer group APIs.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumerGroupsDeprecatedConsumerGroupState(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      assertConsumerGroupsIsClean()

      val testTopicName = "test_topic"
      val testTopicName1 = testTopicName + "1"
      val testTopicName2 = testTopicName + "2"
      val testNumPartitions = 2

      prepareTopics(List(testTopicName, testTopicName1, testTopicName2), testNumPartitions)
      prepareRecords(testTopicName)

      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val testInstanceId1 = "test_instance_id_1"
      val testInstanceId2 = "test_instance_id_2"
      val fakeGroupId = "fake_group_id"

      // contains two static members and one dynamic member
      val groupInstanceSet = Set(testInstanceId1, testInstanceId2, "")
      val topicSet = Set(testTopicName, testTopicName1, testTopicName2)

      // We need to disable the auto commit because after the members got removed from group, the offset commit
      // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
      val defaultConsumerConfig = new Properties(consumerConfig)
      defaultConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      defaultConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      defaultConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)

      val backgroundConsumerSet = new BackgroundConsumerSet(defaultConsumerConfig)
      groupInstanceSet.zip(topicSet).foreach { case (groupInstanceId, topic) =>
        val configOverrides = new Properties()
        if (groupInstanceId != "") {
          // static member
          configOverrides.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)
        }
        backgroundConsumerSet.addConsumer(topic, configOverrides)
      }

      try {
        val groupType = if (groupProtocol.equalsIgnoreCase(GroupProtocol.CONSUMER.name)) GroupType.CONSUMER else GroupType.CLASSIC
        // Start consumer polling threads in the background
        backgroundConsumerSet.start()

        // Test that we can list the new group.
        TestUtils.waitUntilTrue(() => {
          val matching = client.listConsumerGroups.all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().withTypes(util.Set.of(groupType))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in group type $groupType")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().withTypes(util.Set.of(groupType))
            .inStates(util.Set.of(ConsumerGroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in group type $groupType and state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().withTypes(util.Set.of(groupType))
            .inGroupStates(util.Set.of(GroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in group type $groupType and state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inStates(util.Set.of(ConsumerGroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inGroupStates(util.Set.of(GroupState.STABLE))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
            group.groupId == testGroupId &&
              group.state.get == ConsumerGroupState.STABLE &&
              group.groupState.get == GroupState.STABLE)
          matching.size == 1
        }, s"Expected to be able to list $testGroupId in state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inStates(util.Set.of(ConsumerGroupState.EMPTY))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(
            _.groupId == testGroupId)
          matching.isEmpty
        }, s"Expected to find zero groups")

        TestUtils.waitUntilTrue(() => {
          val options = new ListConsumerGroupsOptions().inGroupStates(util.Set.of(GroupState.EMPTY))
          val matching = client.listConsumerGroups(options).all.get.asScala.filter(
            _.groupId == testGroupId)
          matching.isEmpty
        }, s"Expected to find zero groups")

        val describeWithFakeGroupResult = client.describeConsumerGroups(util.List.of(testGroupId, fakeGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
        assertEquals(2, describeWithFakeGroupResult.describedGroups().size())

        // Test that we can get information about the test consumer group.
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(testGroupId))
        val testGroupDescription = describeWithFakeGroupResult.describedGroups().get(testGroupId).get()

        assertEquals(testGroupId, testGroupDescription.groupId())
        assertFalse(testGroupDescription.isSimpleConsumerGroup)
        assertEquals(groupInstanceSet.size, testGroupDescription.members().size())
        val members = testGroupDescription.members()
        members.asScala.foreach(member => assertEquals(testClientId, member.clientId()))
        val topicPartitionsByTopic = members.asScala.flatMap(_.assignment().topicPartitions().asScala).groupBy(_.topic())
        topicSet.foreach { topic =>
          val topicPartitions = topicPartitionsByTopic.getOrElse(topic, List.empty)
          assertEquals(testNumPartitions, topicPartitions.size)
        }

        val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP)
        assertEquals(expectedOperations, testGroupDescription.authorizedOperations())

        // Test that the fake group throws GroupIdNotFoundException
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(fakeGroupId))
        assertFutureThrows(classOf[GroupIdNotFoundException],
          describeWithFakeGroupResult.describedGroups().get(fakeGroupId), s"Group $fakeGroupId not found.")

        // Test that all() also throws GroupIdNotFoundException
        assertFutureThrows(classOf[GroupIdNotFoundException],
          describeWithFakeGroupResult.all(), s"Group $fakeGroupId not found.")

        val testTopicPart0 = new TopicPartition(testTopicName, 0)

        // Test listConsumerGroupOffsets
        TestUtils.waitUntilTrue(() => {
          val parts = client.listConsumerGroupOffsets(testGroupId).partitionsToOffsetAndMetadata().get()
          parts.containsKey(testTopicPart0) && (parts.get(testTopicPart0).offset() == 1)
        }, s"Expected the offset for partition 0 to eventually become 1.")

        // Test listConsumerGroupOffsets with requireStable true
        val options = new ListConsumerGroupOffsetsOptions().requireStable(true)
        var parts = client.listConsumerGroupOffsets(testGroupId, options)
          .partitionsToOffsetAndMetadata().get()
        assertTrue(parts.containsKey(testTopicPart0))
        assertEquals(1, parts.get(testTopicPart0).offset())

        // Test listConsumerGroupOffsets with listConsumerGroupOffsetsSpec
        val groupSpecs = util.Map.of(testGroupId,
          new ListConsumerGroupOffsetsSpec().topicPartitions(util.Set.of(new TopicPartition(testTopicName, 0))))
        parts = client.listConsumerGroupOffsets(groupSpecs).partitionsToOffsetAndMetadata().get()
        assertTrue(parts.containsKey(testTopicPart0))
        assertEquals(1, parts.get(testTopicPart0).offset())

        // Test listConsumerGroupOffsets with listConsumerGroupOffsetsSpec and requireStable option
        parts = client.listConsumerGroupOffsets(groupSpecs, options).partitionsToOffsetAndMetadata().get()
        assertTrue(parts.containsKey(testTopicPart0))
        assertEquals(1, parts.get(testTopicPart0).offset())
      } finally {
        backgroundConsumerSet.close()
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  /**
   * Test the consumer group APIs for member removal.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumerGroupWithMemberRemoval(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      // Verify that initially there are no consumer groups to list.
      assertConsumerGroupsIsClean()
      val testTopicName = "test_topic"
      val testTopicName1 = testTopicName + "1"
      val testTopicName2 = testTopicName + "2"
      val testNumPartitions = 2

      prepareTopics(List(testTopicName, testTopicName1, testTopicName2), testNumPartitions)

      prepareRecords(testTopicName)

      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val testInstanceId1 = "test_instance_id_1"
      val testInstanceId2 = "test_instance_id_2"
      val fakeGroupId = "fake_group_id"

      // contains two static members and one dynamic member
      val groupInstanceSet = Set(testInstanceId1, testInstanceId2, "")
      val topicSet = Set(testTopicName, testTopicName1, testTopicName2)

      // We need to disable the auto commit because after the members got removed from group, the offset commit
      // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
      val defaultConsumerConfig = new Properties(consumerConfig)
      defaultConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      defaultConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      defaultConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)

      val backgroundConsumerSet = new BackgroundConsumerSet(defaultConsumerConfig)
      groupInstanceSet.zip(topicSet).foreach { case (groupInstanceId, topic) =>
        val configOverrides = new Properties()
        if (groupInstanceId != "") {
          // static member
          configOverrides.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)
        }
        backgroundConsumerSet.addConsumer(topic, configOverrides)
      }

      try {
        // Start consumer polling threads in the background
        backgroundConsumerSet.start()

        // Test delete non-exist consumer instance
        val invalidInstanceId = "invalid-instance-id"
        var removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId, new RemoveMembersFromConsumerGroupOptions(
          util.Set.of(new MemberToRemove(invalidInstanceId))
        ))

        assertFutureThrows(classOf[UnknownMemberIdException], removeMembersResult.all)
        val firstMemberFuture = removeMembersResult.memberResult(new MemberToRemove(invalidInstanceId))
        assertFutureThrows(classOf[UnknownMemberIdException], firstMemberFuture)

        // Test consumer group deletion
        var deleteResult = client.deleteConsumerGroups(util.List.of(testGroupId, fakeGroupId))
        assertEquals(2, deleteResult.deletedGroups().size())

        // Deleting the fake group ID should get GroupIdNotFoundException.
        assertTrue(deleteResult.deletedGroups().containsKey(fakeGroupId))
        assertFutureThrows(classOf[GroupIdNotFoundException], deleteResult.deletedGroups().get(fakeGroupId))

        // Deleting the real group ID should get GroupNotEmptyException
        assertTrue(deleteResult.deletedGroups().containsKey(testGroupId))
        assertFutureThrows(classOf[GroupNotEmptyException], deleteResult.deletedGroups().get(testGroupId))

        // Stop the consumer threads and close consumers to prevent member rejoining.
        backgroundConsumerSet.stop()

        // Check the members in the group after consumers have stopped
        var describeTestGroupResult = client.describeConsumerGroups(util.List.of(testGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
        assertEquals(1, describeTestGroupResult.describedGroups().size())

        var testGroupDescription = describeTestGroupResult.describedGroups().get(testGroupId).get()
        assertEquals(testGroupId, testGroupDescription.groupId)
        assertFalse(testGroupDescription.isSimpleConsumerGroup)
        assertEquals(2, testGroupDescription.members().size())

        // Test delete one static member
        removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId,
          new RemoveMembersFromConsumerGroupOptions(util.Set.of(new MemberToRemove(testInstanceId1))))

        assertNull(removeMembersResult.all().get())
        assertNull(removeMembersResult.memberResult(new MemberToRemove(testInstanceId1)).get())

        describeTestGroupResult = client.describeConsumerGroups(util.List.of(testGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
        testGroupDescription = describeTestGroupResult.describedGroups().get(testGroupId).get()

        assertEquals(1, testGroupDescription.members().size())

        // Delete all active members remaining
        removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId, new RemoveMembersFromConsumerGroupOptions())
        assertNull(removeMembersResult.all().get())

        // The group should contain no members now.
        testGroupDescription = client.describeConsumerGroups(util.List.of(testGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true)).describedGroups().get(testGroupId).get()
        assertTrue(testGroupDescription.members().isEmpty)

        // Consumer group deletion on empty group should succeed
        deleteResult = client.deleteConsumerGroups(util.List.of(testGroupId))
        assertEquals(1, deleteResult.deletedGroups().size())

        assertTrue(deleteResult.deletedGroups().containsKey(testGroupId))
        assertNull(deleteResult.deletedGroups().get(testGroupId).get())

        // Test alterConsumerGroupOffsets when group is empty
        val testTopicPart0 = new TopicPartition(testTopicName, 0)
        val alterConsumerGroupOffsetsResult = client.alterConsumerGroupOffsets(testGroupId,
          util.Map.of(testTopicPart0, new OffsetAndMetadata(0L)))
        assertNull(alterConsumerGroupOffsetsResult.all().get())
        assertNull(alterConsumerGroupOffsetsResult.partitionResult(testTopicPart0).get())

        // Verify alterConsumerGroupOffsets success
        TestUtils.waitUntilTrue(() => {
          val parts = client.listConsumerGroupOffsets(testGroupId).partitionsToOffsetAndMetadata().get()
          parts.containsKey(testTopicPart0) && (parts.get(testTopicPart0).offset() == 0)
        }, s"Expected the offset for partition 0 to eventually become 0.")
      } finally {
        backgroundConsumerSet.close()
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDeleteConsumerGroupOffsets(groupProtocol: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      val testTopicName = "test_topic"
      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val fakeGroupId = "fake_group_id"

      val tp1 = new TopicPartition(testTopicName, 0)
      val tp2 = new TopicPartition("foo", 0)

      client.createTopics(util.Set.of(
        new NewTopic(testTopicName, 1, 1.toShort))).all().get()
      waitForTopics(client, List(testTopicName), List())

      prepareRecords(testTopicName)

      val newConsumerConfig = new Properties(consumerConfig)
      newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      // Increase timeouts to avoid having a rebalance during the test
      newConsumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE.toString)
      if (GroupProtocol.CLASSIC.name.equalsIgnoreCase(groupProtocol)) {
        newConsumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT.toString)
      }

      Using.resource(createConsumer(configOverrides = newConsumerConfig)) { consumer =>
        consumer.subscribe(util.List.of(testTopicName))
        val records = consumer.poll(JDuration.ofMillis(DEFAULT_MAX_WAIT_MS))
        assertNotEquals(0, records.count)
        consumer.commitSync()

        // Test offset deletion while consuming
        val partitions = new util.LinkedHashSet[TopicPartition](util.List.of(tp1, tp2))
        val offsetDeleteResult = client.deleteConsumerGroupOffsets(testGroupId, partitions)

        // Top level error will equal to the first partition level error
        assertFutureThrows(classOf[GroupSubscribedToTopicException], offsetDeleteResult.all())
        assertFutureThrows(classOf[GroupSubscribedToTopicException], offsetDeleteResult.partitionResult(tp1))
        assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetDeleteResult.partitionResult(tp2))

        // Test the fake group ID
        val fakeDeleteResult = client.deleteConsumerGroupOffsets(fakeGroupId, util.Set.of(tp1, tp2))

        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.all())
        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.partitionResult(tp1))
        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.partitionResult(tp2))
      }

      // Test offset deletion when group is empty
      val offsetDeleteResult = client.deleteConsumerGroupOffsets(testGroupId, util.Set.of(tp1, tp2))

      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetDeleteResult.all())
      assertNull(offsetDeleteResult.partitionResult(tp1).get())
      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetDeleteResult.partitionResult(tp2))
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  private def prepareTopics(topics: List[String], numberOfPartitions: Int): Unit = {
    client.createTopics(topics.map(topic => new NewTopic(topic, numberOfPartitions, 1.toShort)).asJava).all().get()
    waitForTopics(client, topics, List())
  }

  private def prepareRecords(testTopicName: String) = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord(testTopicName, 0, null, null)).get()
    } finally {
      Utils.closeQuietly(producer, "producer")
    }
  }

  private def prepareConsumers(groupInstanceSet: Set[String], topicSet: Set[String], defaultConsumerConfig: Properties) = {
    val backgroundConsumerSet = new BackgroundConsumerSet(defaultConsumerConfig)
    groupInstanceSet.zip(topicSet).foreach { case (groupInstanceId, topic) =>
      val configOverrides = new Properties()
      if (groupInstanceId != "") {
        // static member
        configOverrides.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)
      }
      backgroundConsumerSet.addConsumer(topic, configOverrides)
    }
    backgroundConsumerSet
  }

  /**
   * Verify that initially there are no consumer groups to list.
   */
  private def assertConsumerGroupsIsClean(): Unit = {
    val listResult = client.listConsumerGroups()
    assertEquals(0, listResult.all().get().size())
    assertEquals(0, listResult.errors().get().size())
    assertEquals(0, listResult.valid().get().size())
  }

  @Test
  def testListGroups(): Unit = {
    val classicGroupId = "classic_group_id"
    val consumerGroupId = "consumer_group_id"
    val shareGroupId = "share_group_id"
    val simpleGroupId = "simple_group_id"
    val streamsGroupId = "streams_group_id"
    val testTopicName = "test_topic"

    val config = createConfig
    client = Admin.create(config)

    client.createTopics(util.Set.of(
      new NewTopic(testTopicName, 1, 1.toShort)
    )).all().get()
    waitForTopics(client, List(testTopicName), List())
    val topicPartition = new TopicPartition(testTopicName, 0)

    consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name)
    val classicGroupConfig = new Properties(consumerConfig)
    classicGroupConfig.put(ConsumerConfig.GROUP_ID_CONFIG, classicGroupId)
    val classicGroup = createConsumer(configOverrides = classicGroupConfig)

    consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name)
    val consumerGroupConfig = new Properties(consumerConfig)
    consumerGroupConfig.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
    val consumerGroup = createConsumer(configOverrides = consumerGroupConfig)

    val shareGroupConfig = new Properties(consumerConfig)
    shareGroupConfig.put(ConsumerConfig.GROUP_ID_CONFIG, shareGroupId)
    val shareGroup = createShareConsumer(configOverrides = shareGroupConfig)

    val streamsGroup = createStreamsGroup(
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId
    )

    try {
      classicGroup.subscribe(util.Set.of(testTopicName))
      classicGroup.poll(JDuration.ofMillis(1000))
      consumerGroup.subscribe(util.Set.of(testTopicName))
      consumerGroup.poll(JDuration.ofMillis(1000))
      shareGroup.subscribe(util.Set.of(testTopicName))
      shareGroup.poll(JDuration.ofMillis(1000))
      streamsGroup.poll(JDuration.ofMillis(1000))

      val alterConsumerGroupOffsetsResult = client.alterConsumerGroupOffsets(simpleGroupId,
        util.Map.of(topicPartition, new OffsetAndMetadata(0L)))
      assertNull(alterConsumerGroupOffsetsResult.all().get())
      assertNull(alterConsumerGroupOffsetsResult.partitionResult(topicPartition).get())

      TestUtils.waitUntilTrue(() => {
        val groups = client.listGroups().all().get()
        groups.size() == 5
      }, "Expected to find all groups")

      val classicGroupListing = new GroupListing(classicGroupId, Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE))
      val consumerGroupListing = new GroupListing(consumerGroupId, Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE))
      val shareGroupListing = new GroupListing(shareGroupId, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE))
      val simpleGroupListing = new GroupListing(simpleGroupId, Optional.of(GroupType.CLASSIC), "", Optional.of(GroupState.EMPTY))
      val streamsGroupListing = new GroupListing(streamsGroupId, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))

      var listGroupsResult = client.listGroups()
      assertTrue(listGroupsResult.errors().get().isEmpty)

      TestUtils.waitUntilTrue(() => {
        val listGroupResultScala = client.listGroups().all().get().asScala
        val filteredStreamsGroups = listGroupResultScala.filter(_.groupId() == streamsGroupId)
        val filteredClassicGroups = listGroupResultScala.filter(_.groupId() == classicGroupId)
        val filteredConsumerGroups = listGroupResultScala.filter(_.groupId() == consumerGroupId)
        val filteredShareGroups = listGroupResultScala.filter(_.groupId() == shareGroupId)
        filteredClassicGroups.forall(_.groupState().orElse(null) == GroupState.STABLE) &&
          filteredConsumerGroups.forall(_.groupState().orElse(null) == GroupState.STABLE) &&
          filteredShareGroups.forall(_.groupState().orElse(null) == GroupState.STABLE) &&
          filteredStreamsGroups.forall(_.groupState().orElse(null) == GroupState.STABLE)
      }, "Groups not stable yet")

      listGroupsResult = client.listGroups(new ListGroupsOptions().withTypes(util.Set.of(GroupType.CLASSIC)))
      assertTrue(listGroupsResult.errors().get().isEmpty)
      assertEquals(Set(classicGroupListing, simpleGroupListing), listGroupsResult.all().get().asScala.toSet)
      assertEquals(Set(classicGroupListing, simpleGroupListing), listGroupsResult.valid().get().asScala.toSet)

      listGroupsResult = client.listGroups(new ListGroupsOptions().withTypes(util.Set.of(GroupType.CONSUMER)))
      assertTrue(listGroupsResult.errors().get().isEmpty)
      assertEquals(Set(consumerGroupListing), listGroupsResult.all().get().asScala.toSet)
      assertEquals(Set(consumerGroupListing), listGroupsResult.valid().get().asScala.toSet)

      listGroupsResult = client.listGroups(new ListGroupsOptions().withTypes(util.Set.of(GroupType.SHARE)))
      assertTrue(listGroupsResult.errors().get().isEmpty)
      assertEquals(Set(shareGroupListing), listGroupsResult.all().get().asScala.toSet)
      assertEquals(Set(shareGroupListing), listGroupsResult.valid().get().asScala.toSet)

      listGroupsResult = client.listGroups(new ListGroupsOptions().withTypes(util.Set.of(GroupType.STREAMS)))
      assertTrue(listGroupsResult.errors().get().isEmpty)
      assertEquals(Set(streamsGroupListing), listGroupsResult.all().get().asScala.toSet)
      assertEquals(Set(streamsGroupListing), listGroupsResult.valid().get().asScala.toSet)

    } finally {
      Utils.closeQuietly(classicGroup, "classicGroup")
      Utils.closeQuietly(consumerGroup, "consumerGroup")
      Utils.closeQuietly(shareGroup, "shareGroup")
      Utils.closeQuietly(streamsGroup, "streamsGroup")
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersClassicGroupProtocolOnly"))
  def testDescribeClassicGroups(groupProtocol: String): Unit = {
    val classicGroupId = "classic_group_id"
    val simpleGroupId = "simple_group_id"
    val testTopicName = "test_topic"

    val classicGroupConfig = new Properties(consumerConfig)
    classicGroupConfig.put(ConsumerConfig.GROUP_ID_CONFIG, classicGroupId)
    val classicGroup = createConsumer(configOverrides = classicGroupConfig)

    val config = createConfig
    client = Admin.create(config)
    try {
      client.createTopics(util.Set.of(
        new NewTopic(testTopicName, 1, 1.toShort)
      )).all().get()
      waitForTopics(client, List(testTopicName), List())
      val topicPartition = new TopicPartition(testTopicName, 0)

      classicGroup.subscribe(util.Set.of(testTopicName))
      classicGroup.poll(JDuration.ofMillis(1000))

      val alterConsumerGroupOffsetsResult = client.alterConsumerGroupOffsets(simpleGroupId,
        util.Map.of(topicPartition, new OffsetAndMetadata(0L)))
      assertNull(alterConsumerGroupOffsetsResult.all().get())
      assertNull(alterConsumerGroupOffsetsResult.partitionResult(topicPartition).get())

      val groupIds = util.List.of(simpleGroupId, classicGroupId)
      TestUtils.waitUntilTrue(() => {
        val groups = client.describeClassicGroups(groupIds).all().get()
        groups.size() == 2
      }, "Expected to find all groups")

      val classicConsumers = client.describeClassicGroups(groupIds).all().get()
      val classicConsumer = classicConsumers.get(classicGroupId)
      assertNotNull(classicConsumer)
      assertEquals(classicGroupId, classicConsumer.groupId)
      assertEquals("consumer", classicConsumer.protocol)
      assertFalse(classicConsumer.members.isEmpty)
      classicConsumer.members.forEach(member => assertTrue(member.upgraded.isEmpty))

      assertNotNull(classicConsumers.get(simpleGroupId))
      assertEquals(simpleGroupId, classicConsumers.get(simpleGroupId).groupId())
      assertTrue(classicConsumers.get(simpleGroupId).protocol().isEmpty)
    } finally {
      Utils.closeQuietly(classicGroup, "classicGroup")
      Utils.closeQuietly(client, "adminClient")
    }
  }

  /**
   * Verify that initially there are no share groups to list.
   */
  private def assertNoShareGroupsExist(): Unit = {
    val list = client.listGroups()
    assertEquals(0, list.all().get().size())
    assertEquals(0, list.errors().get().size())
    assertEquals(0, list.valid().get().size())
  }

  private def createShareConsumerThread[K,V](consumer: ShareConsumer[K,V], topic: String, latch: CountDownLatch): Thread = {
    new Thread {
      override def run : Unit = {
        consumer.subscribe(util.Set.of(topic))
        try {
          while (true) {
            consumer.poll(JDuration.ofSeconds(5))
            if (latch.getCount > 0L)
              latch.countDown()
            consumer.commitSync()
          }
        } catch {
          case _: InterruptException => // Suppress the output to stderr
        }
      }
    }
  }

  @Test
  def testShareGroups(): Unit = {
    val testGroupId = "test_group_id"
    val testClientId = "test_client_id"
    val fakeGroupId = "fake_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 2

    def createProperties(): Properties = {
      val newConsumerConfig = new Properties(consumerConfig)
      newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      newConsumerConfig
    }

    val consumerSet = Set(createShareConsumer(configOverrides = createProperties()))
    val topicSet = Set(testTopicName)
    val latch = new CountDownLatch(consumerSet.size)

    val config = createConfig
    client = Admin.create(config)
    try {
      assertNoShareGroupsExist()
      prepareTopics(List(testTopicName), testNumPartitions)
      prepareRecords(testTopicName)

      // Start consumers in a thread that will subscribe to a new group.
      val consumerThreads = consumerSet.zip(topicSet).map(zipped => createShareConsumerThread(zipped._1, zipped._2, latch))

      try {
        consumerThreads.foreach(_.start())
        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS))

        // listGroups is used to list share groups
        // Test that we can list the new group.
        TestUtils.waitUntilTrue(() => {
          client.listGroups.all.get.stream().filter(group =>
            group.groupId == testGroupId &&
              group.groupState.get == GroupState.STABLE).count() == 1
        }, s"Expected to be able to list $testGroupId")

        TestUtils.waitUntilTrue(() => {
          val options = new ListGroupsOptions().withTypes(util.Set.of(GroupType.SHARE)).inGroupStates(util.Set.of(GroupState.STABLE))
          client.listGroups(options).all.get.stream().filter(group =>
            group.groupId == testGroupId &&
              group.groupState.get == GroupState.STABLE).count() == 1
        }, s"Expected to be able to list $testGroupId in state Stable")

        TestUtils.waitUntilTrue(() => {
          val options = new ListGroupsOptions().withTypes(util.Set.of(GroupType.SHARE)).inGroupStates(util.Set.of(GroupState.EMPTY))
          client.listGroups(options).all.get.stream().filter(_.groupId == testGroupId).count() == 0
        }, s"Expected to find zero groups")

        var describeWithFakeGroupResult: DescribeShareGroupsResult = null

        TestUtils.waitUntilTrue(() => {
          describeWithFakeGroupResult = client.describeShareGroups(util.List.of(testGroupId, fakeGroupId),
            new DescribeShareGroupsOptions().includeAuthorizedOperations(true))
          val members = describeWithFakeGroupResult.describedGroups().get(testGroupId).get().members()
          members.asScala.flatMap(_.assignment().topicPartitions().asScala).groupBy(_.topic()).nonEmpty
        }, s"Could not get partitions assigned. Last response $describeWithFakeGroupResult.")

        assertEquals(2, describeWithFakeGroupResult.describedGroups().size())

        // Test that we can get information about the test share group.
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(testGroupId))
        assertEquals(2, describeWithFakeGroupResult.describedGroups().size())
        var testGroupDescription = describeWithFakeGroupResult.describedGroups().get(testGroupId).get()

        assertEquals(testGroupId, testGroupDescription.groupId())
        assertEquals(consumerSet.size, testGroupDescription.members().size())
        val members = testGroupDescription.members()
        members.forEach(member => assertEquals(testClientId, member.clientId()))
        val topicPartitionsByTopic = members.asScala.flatMap(_.assignment().topicPartitions().asScala).groupBy(_.topic())
        topicSet.foreach { topic =>
          val topicPartitions = topicPartitionsByTopic.getOrElse(topic, List.empty)
          assertEquals(testNumPartitions, topicPartitions.size)
        }

        val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP)
        assertEquals(expectedOperations, testGroupDescription.authorizedOperations())

        // Test that the fake group throws GroupIdNotFoundException
        assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(fakeGroupId))
        assertFutureThrows(classOf[GroupIdNotFoundException],
          describeWithFakeGroupResult.describedGroups().get(fakeGroupId),
          s"Group $fakeGroupId not found.")

        // Test that all() also throws GroupIdNotFoundException
        assertFutureThrows(classOf[GroupIdNotFoundException],
          describeWithFakeGroupResult.all(),
          s"Group $fakeGroupId not found.")

        val describeTestGroupResult = client.describeShareGroups(util.Set.of(testGroupId),
          new DescribeShareGroupsOptions().includeAuthorizedOperations(true))
        assertEquals(1, describeTestGroupResult.all().get().size())
        assertEquals(1, describeTestGroupResult.describedGroups().size())

        testGroupDescription = describeTestGroupResult.describedGroups().get(testGroupId).get()

        assertEquals(testGroupId, testGroupDescription.groupId)
        assertEquals(consumerSet.size, testGroupDescription.members().size())

        // Describing a share group using describeConsumerGroups reports it as a non-existent group
        // but the error message is different
        val describeConsumerGroupResult = client.describeConsumerGroups(util.Set.of(testGroupId),
          new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
        assertFutureThrows(classOf[GroupIdNotFoundException],
          describeConsumerGroupResult.all(),
          s"Group $testGroupId is not a consumer group.")
      } finally {
        consumerThreads.foreach {
          case consumerThread =>
            consumerThread.interrupt()
            consumerThread.join()
        }
      }
    } finally {
      consumerSet.foreach(consumer => Utils.closeQuietly(consumer, "consumer"))
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testDeleteShareGroupOffsets(): Unit = {
    val config = createConfig
    client = Admin.create(config)
    val testTopicName = "test_topic"
    val testGroupId = "test_group_id"
    val testClientId = "test_client_id"
    val fakeGroupId = "fake_group_id"
    val fakeTopicName = "foo"

    try {
      prepareTopics(List(testTopicName), 1)
      prepareRecords(testTopicName)

      val newShareConsumerConfig = new Properties(consumerConfig)
      newShareConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newShareConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)

      Using.resource(createShareConsumer(configOverrides = newShareConsumerConfig)) { consumer =>
        consumer.subscribe(util.List.of(testTopicName))
        consumer.poll(JDuration.ofMillis(DEFAULT_MAX_WAIT_MS))
        consumer.commitSync()

        // listGroups is used to list share groups
        // Test that we can list the new group.
        TestUtils.waitUntilTrue(() => {
          client.listGroups.all.get.stream().filter(group =>
            group.groupId == testGroupId &&
              group.groupState.get == GroupState.STABLE).count() == 1
        }, s"Expected to be able to list $testGroupId")

        // Test offset deletion while consuming
        val offsetDeleteResult = client.deleteShareGroupOffsets(testGroupId, util.Set.of(testTopicName, fakeTopicName))

        // Deleting the offset with real group ID should get GroupNotEmptyException
        assertFutureThrows(classOf[GroupNotEmptyException], offsetDeleteResult.all())
        assertFutureThrows(classOf[GroupNotEmptyException], offsetDeleteResult.topicResult(testTopicName))
        assertFutureThrows(classOf[GroupNotEmptyException], offsetDeleteResult.topicResult(fakeTopicName))

        // Test the fake group ID
        val fakeDeleteResult = client.deleteShareGroupOffsets(fakeGroupId, util.Set.of(testTopicName, fakeTopicName))

        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.all())
        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.topicResult(testTopicName))
        assertFutureThrows(classOf[GroupIdNotFoundException], fakeDeleteResult.topicResult(fakeTopicName))
      }

      // Test offset deletion when group is empty
      val offsetDeleteResult = client.deleteShareGroupOffsets(testGroupId, util.Set.of(testTopicName, fakeTopicName))

      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetDeleteResult.all())
      assertNull(offsetDeleteResult.topicResult(testTopicName).get())
      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetDeleteResult.topicResult(fakeTopicName))

      val tp1 = new TopicPartition(testTopicName, 0)
      val parts = client.listShareGroupOffsets(util.Map.of(testGroupId, new ListShareGroupOffsetsSpec().topicPartitions(util.List.of(tp1))))
        .partitionsToOffsetAndMetadata(testGroupId)
        .get()
      assertTrue(parts.containsKey(tp1))
      assertNull(parts.get(tp1))
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testAlterShareGroupOffsets(): Unit = {
    val config = createConfig
    client = Admin.create(config)
    val testTopicName = "test_topic"
    val testGroupId = "test_group_id"
    val testClientId = "test_client_id"
    val nonexistentGroupId = "nonexistent_group_id"
    val fakeTopicName = "foo"

    val tp1 = new TopicPartition(testTopicName, 0)
    val tp2 = new TopicPartition(fakeTopicName, 0)
    try {
      prepareTopics(List(testTopicName), 1)
      prepareRecords(testTopicName)

      val newShareConsumerConfig = new Properties(consumerConfig)
      newShareConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newShareConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)

      Using.resource(createShareConsumer(configOverrides = newShareConsumerConfig)) { consumer =>
        consumer.subscribe(util.List.of(testTopicName))
        consumer.poll(JDuration.ofMillis(DEFAULT_MAX_WAIT_MS))
        consumer.commitSync()

        // listGroups is used to list share groups
        // Test that we can list the new group.
        TestUtils.waitUntilTrue(() => {
          client.listGroups.all.get.stream().filter(group =>
            group.groupId == testGroupId &&
              group.groupState.get == GroupState.STABLE).count() == 1
        }, s"Expected to be able to list $testGroupId")

        // Test offset alter while consuming
        val offsetAlterResult = client.alterShareGroupOffsets(testGroupId, util.Map.of(tp1, 0, tp2, 0))

        // Altering the offset with real group ID should get GroupNotEmptyException
        assertFutureThrows(classOf[GroupNotEmptyException], offsetAlterResult.all())
        assertFutureThrows(classOf[GroupNotEmptyException], offsetAlterResult.partitionResult(tp1))
        assertFutureThrows(classOf[GroupNotEmptyException], offsetAlterResult.partitionResult(tp2))

        // Test the non-existent group ID
        val nonexistentAlterResult = client.alterShareGroupOffsets(nonexistentGroupId, util.Map.of(tp1, 0, tp2, 0))

        assertFutureThrows(classOf[UnknownTopicOrPartitionException], nonexistentAlterResult.all())
        assertNull(nonexistentAlterResult.partitionResult(tp1).get())
        assertFutureThrows(classOf[UnknownTopicOrPartitionException], nonexistentAlterResult.partitionResult(tp2))
      }

      // Test offset alter when group is empty
      val offsetAlterResult = client.alterShareGroupOffsets(testGroupId, util.Map.of(tp1, 0, tp2, 0))

      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetAlterResult.all())
      assertNull(offsetAlterResult.partitionResult(tp1).get())
      assertFutureThrows(classOf[UnknownTopicOrPartitionException], offsetAlterResult.partitionResult(tp2))

      val parts = client.listShareGroupOffsets(util.Map.of(testGroupId, new ListShareGroupOffsetsSpec().topicPartitions(util.List.of(tp1))))
        .partitionsToOffsetAndMetadata(testGroupId)
        .get()
      assertTrue(parts.containsKey(tp1))
      assertEquals(0, parts.get(tp1).offset())
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testListShareGroupOffsets(): Unit = {
    val config = createConfig
    client = Admin.create(config)
    val testTopicName = "test_topic"
    val testGroupId = "test_group_id"
    val testClientId = "test_client_id"

    val newShareConsumerConfig = new Properties(consumerConfig)
    newShareConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
    newShareConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
    val consumerSet = Set(createShareConsumer(configOverrides = newShareConsumerConfig))
    val topicSet = Set(testTopicName)
    val latch = new CountDownLatch(consumerSet.size)

    try {
      assertNoShareGroupsExist()
      prepareTopics(List(testTopicName), 2)
      prepareRecords(testTopicName)

      // Start consumers in a thread that will subscribe to a new group.
      val consumerThreads = consumerSet.zip(topicSet).map(zipped => createShareConsumerThread(zipped._1, zipped._2, latch))
      try {
        consumerThreads.foreach(_.start())
        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS))
        val tp1 = new TopicPartition(testTopicName, 0)
        val tp2 = new TopicPartition(testTopicName, 1)

        // Test listShareGroupOffsets
        TestUtils.waitUntilTrue(() => {
          val parts = client.listShareGroupOffsets(util.Map.of(testGroupId, new ListShareGroupOffsetsSpec()))
            .partitionsToOffsetAndMetadata(testGroupId)
            .get()
          parts.containsKey(tp1) && parts.containsKey(tp2)
        }, "Expected the result contains all partitions.")

        // Test listShareGroupOffsets with listShareGroupOffsetsSpec
        val groupSpecs = util.Map.of(testGroupId, new ListShareGroupOffsetsSpec().topicPartitions(util.List.of(tp1)))
        val parts = client.listShareGroupOffsets(groupSpecs).partitionsToOffsetAndMetadata(testGroupId).get()
        assertTrue(parts.containsKey(tp1))
        assertFalse(parts.containsKey(tp2))
      } finally {
        consumerThreads.foreach {
          case consumerThread =>
            consumerThread.interrupt()
            consumerThread.join()
        }
      }
    } finally {
      consumerSet.foreach(consumer => Utils.closeQuietly(consumer, "consumer"))
      Utils.closeQuietly(client, "adminClient")
    }
  }

  /**
   * Waits until the metadata for the given partition has fully propagated and become consistent across all brokers.
   *
   * @param partition The partition whose leader metadata should be verified across all brokers.
   */
    def waitForBrokerMetadataPropagation(partition: TopicPartition): Unit = {
      while (brokers.exists(_.metadataCache.getPartitionLeaderEndpoint(partition.topic, partition.partition(), listenerName).isEmpty) ||
        brokers.map(_.metadataCache.getPartitionLeaderEndpoint(partition.topic, partition.partition(), listenerName))
        .filter(_.isPresent)
        .map(_.get())
        .toSet.size != 1)
        TimeUnit.MILLISECONDS.sleep(300)
    }

  @Test
  def testElectPreferredLeaders(): Unit = {
    client = createAdminClient

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var reassignmentMap = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        reassignmentMap += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        reassignmentMap += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(reassignmentMap.asJava).all().get()

      TestUtils.waitUntilTrue(
        () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
        s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
        10000)
      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(partition1))
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    waitForBrokerMetadataPropagation(partition1)
    waitForBrokerMetadataPropagation(partition2)
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(partition1))
    assertEquals(util.Set.of(partition1), electResult.partitions.get.keySet)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    electResult.partitions.get.get(partition2)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition2"))
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(unknownPartition))
    assertEquals(util.Set.of(unknownPartition), electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    waitForBrokerMetadataPropagation(partition1)
    waitForBrokerMetadataPropagation(partition2)
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(unknownPartition, partition1))
    assertEquals(util.Set.of(unknownPartition, partition1), electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(partition2))
    assertEquals(util.Set.of(partition2), electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    // Now change the preferred leader to 1
    waitForBrokerMetadataPropagation(partition1)
    waitForBrokerMetadataPropagation(partition2)
    changePreferredLeader(prefer1)
    // but shut it down...
    killBroker(1)
    waitForBrokerMetadataPropagation(partition1)
    waitForBrokerMetadataPropagation(partition2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      assertTrue(exception.getMessage.contains(
        "The preferred leader was not available."),
        s"Unexpected message: ${exception.getMessage}")
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, util.Set.of(partition1), shortTimeout)
    assertEquals(util.Set.of(partition1), electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @Test
  def testElectUncleanLeadersForOnePartition(): Unit = {
    // Case: unclean leader election with one topic partition
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val partition1 = new TopicPartition("unclean-test-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> assignment1))

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(partition1))
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    TestUtils.assertLeader(client, partition1, broker2)
  }

  @Test
  def testElectUncleanLeadersForManyPartitions(): Unit = {
    // Case: unclean leader election with many topic partitions
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertNoLeader(client, partition2)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(partition1, partition2))
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    electResult.partitions.get.get(partition2)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition2"))
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker2)
  }

  @Test
  def testElectUncleanLeadersForAllPartitions(): Unit = {
    // Case: noop unclean leader election and valid unclean leader election for all partitions
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, null)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @Test
  def testElectUncleanLeadersForUnknownPartitions(): Unit = {
    // Case: unclean leader election for unknown topic
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val unknownPartition = new TopicPartition(topic, 1)
    val unknownTopic = new TopicPartition("unknown-topic", 0)

    createTopicWithAssignment(
      topic,
      Map(0 -> assignment1)
    )

    TestUtils.assertLeader(client, new TopicPartition(topic, 0), broker1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(unknownPartition, unknownTopic))
    assertTrue(electResult.partitions.get.get(unknownPartition).get.isInstanceOf[UnknownTopicOrPartitionException])
    assertTrue(electResult.partitions.get.get(unknownTopic).get.isInstanceOf[UnknownTopicOrPartitionException])
  }

  @Test
  def testElectUncleanLeadersWhenNoLiveBrokers(): Unit = {
    // Case: unclean leader election with no live brokers
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1)
    )

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(partition1))
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[EligibleLeadersNotAvailableException])
  }

  @Test
  def testElectUncleanLeadersNoop(): Unit = {
    // Case: noop unclean leader election with explicit topic partitions
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1)
    )

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker1)
    TestUtils.assertLeader(client, partition1, broker2)
    brokers(broker1).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(partition1))
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[ElectionNotNeededException])
  }

  @Test
  def testElectUncleanLeadersAndNoop(): Unit = {
    // Case: one noop unclean leader election and one valid unclean leader election
    client = createAdminClient
    disableEligibleLeaderReplicas(client)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, util.Set.of(partition1, partition2))
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    assertTrue(electResult.partitions.get.get(partition2).get.isInstanceOf[ElectionNotNeededException])
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @Test
  def testListReassignmentsDoesNotShowNonReassigningPartitions(): Unit = {
    client = createAdminClient

    // Create topics
    val topic = "list-reassignments-no-reassignments"
    createTopic(topic, replicationFactor = 3)
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(util.Set.of(tp)).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @Test
  def testListReassignmentsDoesNotShowDeletedPartitions(): Unit = {
    client = createAdminClient

    val topic = "list-reassignments-no-reassignments"
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(util.Set.of(tp)).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @Test
  def testValidIncrementalAlterConfigs(): Unit = {
    client = createAdminClient

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topic1CreateConfigs = new Properties
    topic1CreateConfigs.setProperty(TopicConfig.RETENTION_MS_CONFIG, "60000000")
    topic1CreateConfigs.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topic1CreateConfigs)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Alter topic configs
    var topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, "1000"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.APPEND),
      new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, ""), AlterConfigOp.OpType.DELETE)
    )

    // Test SET and APPEND on previously unset properties
    var topic2AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
    )

    var alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs,
      topic2Resource, topic2AlterConfigs
    ))

    assertEquals(util.Set.of(topic1Resource, topic2Resource), alterResult.values.keySet)
    alterResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    var describeResult = client.describeConfigs(util.List.of(topic1Resource, topic2Resource))
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value)
    assertEquals("compact,delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_RETENTION_MS.toString, configs.get(topic1Resource).get(TopicConfig.RETENTION_MS_CONFIG).value)

    assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals("lz4", configs.get(topic2Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)
    assertEquals("delete,compact", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)

    // verify subtract operation, including from an empty property
    topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SUBTRACT),
      new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0"), AlterConfigOp.OpType.SUBTRACT)
    )

    // subtract all from this list property
    topic2AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.SUBTRACT)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs,
      topic2Resource, topic2AlterConfigs
    ))
    assertEquals(util.Set.of(topic1Resource, topic2Resource), alterResult.values.keySet)
    alterResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    describeResult = client.describeConfigs(util.List.of(topic1Resource, topic2Resource))
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)
    assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value) // verify previous change is still intact
    assertEquals("", configs.get(topic1Resource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value)
    assertEquals("", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value )

    // Alter topics with validateOnly=true
    topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs
    ), new AlterConfigsOptions().validateOnly(true))
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    describeResult = client.describeConfigs(util.List.of(topic1Resource))
    configs = describeResult.all.get

    assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)

    // Alter topics with validateOnly=true with invalid configs
    topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zip"), AlterConfigOp.OpType.SET)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs
    ), new AlterConfigsOptions().validateOnly(true))

    assertFutureThrows(classOf[InvalidConfigurationException], alterResult.values().get(topic1Resource),
      "Invalid value zip for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer")
  }

  @Test
  def testAppendAlreadyExistsConfigsAndSubtractNotExistsConfigs(): Unit = {
    client = createAdminClient

    // Create topics
    val topic = "incremental-alter-configs-topic"
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

    val appendValues = s"0:${brokers.head.config.brokerId}"
    val subtractValues = brokers.tail.map(broker => s"0:${broker.config.brokerId}").mkString(",")
    assertNotEquals("", subtractValues)

    val topicCreateConfigs = new Properties
    topicCreateConfigs.setProperty(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues)
    createTopic(topic, numPartitions = 1, replicationFactor = 1, topicCreateConfigs)

    // Append value that is already present
    val topicAppendConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues), AlterConfigOp.OpType.APPEND),
    )

    val appendResult = client.incrementalAlterConfigs(util.Map.of(topicResource, topicAppendConfigs))
    appendResult.all.get

    // Subtract values that are not present
    val topicSubtractConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, subtractValues), AlterConfigOp.OpType.SUBTRACT)
    )
    val subtractResult = client.incrementalAlterConfigs(util.Map.of(topicResource, topicSubtractConfigs))
    subtractResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    val describeResult = client.describeConfigs(util.List.of(topicResource))
    val configs = describeResult.all.get

    assertEquals(appendValues, configs.get(topicResource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value)
  }

  @Test
  def testIncrementalAlterConfigsDeleteAndSetBrokerConfigs(): Unit = {
    client = createAdminClient
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    client.incrementalAlterConfigs(util.Map.of(broker0Resource,
      util.List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "123"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "456"),
          AlterConfigOp.OpType.SET)
      ))).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(util.List.of(broker0Resource)).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      "123".equals(broker0Configs.getOrElse(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "456".equals(broker0Configs.getOrElse(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""))
    }, "Expected to see the broker properties we just set", pause=25)
    client.incrementalAlterConfigs(util.Map.of(broker0Resource,
      util.List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
        AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "654"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "987"),
          AlterConfigOp.OpType.SET)
      ))).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(util.List.of(broker0Resource)).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      "".equals(broker0Configs.getOrElse(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "654".equals(broker0Configs.getOrElse(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "987".equals(broker0Configs.getOrElse(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""))
    }, "Expected to see the broker properties we just modified", pause=25)
  }

  @Test
  def testIncrementalAlterConfigsDeleteBrokerConfigs(): Unit = {
    client = createAdminClient
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    client.incrementalAlterConfigs(util.Map.of(broker0Resource,
      util.List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "123"),
        AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "456"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "789"),
          AlterConfigOp.OpType.SET)
      ))).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(util.List.of(broker0Resource)).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      "123".equals(broker0Configs.getOrElse(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "456".equals(broker0Configs.getOrElse(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "789".equals(broker0Configs.getOrElse(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""))
    }, "Expected to see the broker properties we just set", pause=25)
    client.incrementalAlterConfigs(util.Map.of(broker0Resource,
      util.List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
        AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
          AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""),
          AlterConfigOp.OpType.DELETE)
      ))).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(util.List.of(broker0Resource)).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      "".equals(broker0Configs.getOrElse(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "".equals(broker0Configs.getOrElse(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
        "".equals(broker0Configs.getOrElse(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""))
    }, "Expected to see the broker properties we just removed to be deleted", pause=25)
  }

  @Test
  def testInvalidIncrementalAlterConfigs(): Unit = {
    client = createAdminClient

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopic(topic1)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Add duplicate Keys for topic1
    var topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.75"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.65"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.SET) // valid entry
    )

    // Add valid config for topic2
    var topic2AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET)
    )

    var alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs,
      topic2Resource, topic2AlterConfigs
    ))
    assertEquals(util.Set.of(topic1Resource, topic2Resource), alterResult.values.keySet)

    // InvalidRequestException error for topic1
    assertFutureThrows(classOf[InvalidRequestException], alterResult.values().get(topic1Resource),
      "Error due to duplicate config keys")

    // Operation should succeed for topic2
    alterResult.values().get(topic2Resource).get()
    ensureConsistentKRaftMetadata()

    // Verify that topic1 is not config not updated, and topic2 config is updated
    val describeResult = client.describeConfigs(util.List.of(topic1Resource, topic2Resource))
    val configs = describeResult.all.get
    assertEquals(2, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString, configs.get(topic1Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(topic1Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)
    assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)

    // Check invalid use of append/subtract operation types
    topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.APPEND)
    )

    topic2AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"), AlterConfigOp.OpType.SUBTRACT)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs,
      topic2Resource, topic2AlterConfigs
    ))
    assertEquals(util.Set.of(topic1Resource, topic2Resource), alterResult.values.keySet)

    assertFutureThrows(classOf[InvalidConfigurationException],alterResult.values().get(topic1Resource),
      "Can't APPEND to key compression.type because its type is not LIST.")

    assertFutureThrows(classOf[InvalidConfigurationException],alterResult.values().get(topic2Resource),
      "Can't SUBTRACT to key compression.type because its type is not LIST.")

    // Try to add invalid config
    topic1AlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), AlterConfigOp.OpType.SET)
    )

    alterResult = client.incrementalAlterConfigs(util.Map.of(
      topic1Resource, topic1AlterConfigs
    ))
    assertEquals(util.Set.of(topic1Resource), alterResult.values.keySet)

    assertFutureThrows(classOf[InvalidConfigurationException], alterResult.values().get(topic1Resource),
      "Invalid value 1.1 for configuration min.cleanable.dirty.ratio: Value must be no more than 1")
  }

  @Test
  def testInvalidAlterPartitionReassignments(): Unit = {
    client = createAdminClient
    val topic = "alter-reassignments-topic-1"
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val tp3 = new TopicPartition(topic, 2)
    createTopic(topic, numPartitions = 4, replicationFactor = 2)

    val validAssignment = Optional.of(new NewPartitionReassignment(
      (0 until brokerCount).map(_.asInstanceOf[Integer]).asJava
    ))

    val alterOptions = new AlterPartitionReassignmentsOptions
    alterOptions.allowReplicationFactorChange(false)
    val alterReplicaNumberTo1 = Optional.of(new NewPartitionReassignment(util.List.of(1.asInstanceOf[Integer])))
    val alterReplicaNumberTo2 = Optional.of(new NewPartitionReassignment((0 until brokerCount - 1).map(_.asInstanceOf[Integer]).asJava))
    val alterReplicaNumberTo3 = Optional.of(new NewPartitionReassignment((0 until brokerCount).map(_.asInstanceOf[Integer]).asJava))
    val alterReplicaResults = client.alterPartitionReassignments(util.Map.of(
      tp1, alterReplicaNumberTo1,
      tp2, alterReplicaNumberTo2,
      tp3, alterReplicaNumberTo3,
    ), alterOptions).values()
    assertDoesNotThrow(() => alterReplicaResults.get(tp2).get())
    assertEquals("The replication factor is changed from 2 to 1",
      assertFutureThrows(classOf[InvalidReplicationFactorException], alterReplicaResults.get(tp1)).getMessage)
    assertEquals("The replication factor is changed from 2 to 3",
      assertFutureThrows(classOf[InvalidReplicationFactorException], alterReplicaResults.get(tp3)).getMessage)

    val nonExistentTp1 = new TopicPartition("topicA", 0)
    val nonExistentTp2 = new TopicPartition(topic, 4)
    val nonExistentPartitionsResult = client.alterPartitionReassignments(util.Map.of(
      tp1, validAssignment,
      tp2, validAssignment,
      tp3, validAssignment,
      nonExistentTp1, validAssignment,
      nonExistentTp2, validAssignment
    )).values()
    assertFutureThrows(classOf[UnknownTopicOrPartitionException], nonExistentPartitionsResult.get(nonExistentTp1))
    assertFutureThrows(classOf[UnknownTopicOrPartitionException], nonExistentPartitionsResult.get(nonExistentTp2))

    val extraNonExistentReplica = Optional.of(new NewPartitionReassignment((0 until brokerCount + 1).map(_.asInstanceOf[Integer]).asJava))
    val negativeIdReplica = Optional.of(new NewPartitionReassignment(Seq(-3, -2, -1).map(_.asInstanceOf[Integer]).asJava))
    val duplicateReplica = Optional.of(new NewPartitionReassignment(Seq(0, 1, 1).map(_.asInstanceOf[Integer]).asJava))
    val invalidReplicaResult = client.alterPartitionReassignments(util.Map.of(
      tp1, extraNonExistentReplica,
      tp2, negativeIdReplica,
      tp3, duplicateReplica
    )).values()
    assertFutureThrows(classOf[InvalidReplicaAssignmentException], invalidReplicaResult.get(tp1))
    assertFutureThrows(classOf[InvalidReplicaAssignmentException], invalidReplicaResult.get(tp2))
    assertFutureThrows(classOf[InvalidReplicaAssignmentException], invalidReplicaResult.get(tp3))
  }

  @Test
  def testLongTopicNames(): Unit = {
    val client = createAdminClient
    val longTopicName = String.join("", Collections.nCopies(249, "x"))
    val invalidTopicName = String.join("", Collections.nCopies(250, "x"))
    val newTopics2 = util.List.of(new NewTopic(invalidTopicName, 3, 3.toShort),
                         new NewTopic(longTopicName, 3, 3.toShort))
    val results = client.createTopics(newTopics2).values()
    assertTrue(results.containsKey(longTopicName))
    results.get(longTopicName).get()
    assertTrue(results.containsKey(invalidTopicName))
    assertFutureThrows(classOf[InvalidTopicException], results.get(invalidTopicName))
    assertFutureThrows(classOf[InvalidTopicException],
      client.alterReplicaLogDirs(
        util.Map.of(new TopicPartitionReplica(longTopicName, 0, 0), brokers(0).config.logDirs.get(0))).all())
    client.close()
  }

  // Verify that createTopics and alterConfigs fail with null values
  @Test
  def testNullConfigs(): Unit = {

    def validateLogConfig(compressionType: String): Unit = {
      ensureConsistentKRaftMetadata()
      val topicProps = brokers.head.metadataCache.topicConfig(topic)
      val logConfig = LogConfig.fromProps(util.Map.of[String, AnyRef], topicProps)

      assertEquals(compressionType, logConfig.originals.get(TopicConfig.COMPRESSION_TYPE_CONFIG))
      assertNull(logConfig.originals.get(TopicConfig.RETENTION_BYTES_CONFIG))
      assertEquals(ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, logConfig.retentionSize)
    }

    client = createAdminClient
    val invalidConfigs = Map[String, String](
      TopicConfig.RETENTION_BYTES_CONFIG -> null,
      TopicConfig.COMPRESSION_TYPE_CONFIG -> "producer"
    ).asJava
    val newTopic = new NewTopic(topic, 2, brokerCount.toShort)
    assertFutureThrows(classOf[InvalidConfigurationException],
      client.createTopics(util.List.of(newTopic.configs(invalidConfigs))).all,
      "Null value not supported for topic configs: retention.bytes"
    )

    val validConfigs = util.Map.of[String, String](TopicConfig.COMPRESSION_TYPE_CONFIG, "producer")
    client.createTopics(util.List.of(newTopic.configs(validConfigs))).all.get()
    waitForTopics(client, expectedPresent = Seq(topic), expectedMissing = List())
    validateLogConfig(compressionType = "producer")

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val alterOps = util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, null), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET)
    )
    assertFutureThrows(classOf[InvalidRequestException],
      client.incrementalAlterConfigs(util.Map.of(topicResource, alterOps)).all,
      "Null value not supported for : retention.bytes"
    )
    validateLogConfig(compressionType = "producer")
  }

  @Test
  def testDescribeConfigsForLog4jLogLevels(): Unit = {
    client = createAdminClient
    LoggerFactory.getLogger("kafka.cluster.Replica").trace("Message to create the logger")
    val loggerConfig = describeBrokerLoggers()
    val kafkaLogLevel = loggerConfig.get("kafka").value()
    val clusterReplicaLogLevel = loggerConfig.get("kafka.cluster.Replica")
    // we expect the log level to be inherited from the first ancestor with a level configured
    assertEquals(kafkaLogLevel, clusterReplicaLogLevel.value())
    assertEquals("kafka.cluster.Replica", clusterReplicaLogLevel.name())
    assertEquals(ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, clusterReplicaLogLevel.source())
    assertEquals(false, clusterReplicaLogLevel.isReadOnly)
    assertEquals(false, clusterReplicaLogLevel.isSensitive)
    assertTrue(clusterReplicaLogLevel.synonyms().isEmpty)
  }

  @Test
  def testIncrementalAlterConfigsForLog4jLogLevels(): Unit = {
    client = createAdminClient

    val ancestorLogger = "kafka"
    val initialLoggerConfig = describeBrokerLoggers()
    val initialAncestorLogLevel = initialLoggerConfig.get("kafka").value()
    val initialControllerServerLogLevel = initialLoggerConfig.get("kafka.server.ControllerServer").value()
    val initialLogCleanerLogLevel = initialLoggerConfig.get("kafka.log.LogCleaner").value()
    val initialReplicaManagerLogLevel = initialLoggerConfig.get("kafka.server.ReplicaManager").value()

    val newAncestorLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL
    val alterAncestorLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET)
    )
    // Test validateOnly does not change anything
    alterBrokerLoggers(alterAncestorLoggerEntry, validateOnly = true)
    val validatedLoggerConfig = describeBrokerLoggers()
    assertEquals(initialAncestorLogLevel, validatedLoggerConfig.get(ancestorLogger).value())
    assertEquals(initialControllerServerLogLevel, validatedLoggerConfig.get("kafka.server.ControllerServer").value())
    assertEquals(initialLogCleanerLogLevel, validatedLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(initialReplicaManagerLogLevel, validatedLoggerConfig.get("kafka.server.ReplicaManager").value())

    // test that we can change them and unset loggers still use the root's log level
    alterBrokerLoggers(alterAncestorLoggerEntry)
    val changedAncestorLoggerConfig = describeBrokerLoggers()
    assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(ancestorLogger).value())
    assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.server.ControllerServer").value())
    assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get("kafka.server.ReplicaManager").value())

    // alter the LogCleaner's logger so we can later test resetting it
    val alterLogCleanerLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET)
    )
    alterBrokerLoggers(alterLogCleanerLoggerEntry)
    val changedBrokerLoggerConfig = describeBrokerLoggers()
    assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, changedBrokerLoggerConfig.get("kafka.log.LogCleaner").value())

    // properly test various set operations and one delete
    val alterLogLevelsEntries = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry("kafka.server.ReplicaManager", LogLevelConfig.TRACE_LOG_LEVEL), AlterConfigOp.OpType.SET),
    )
    alterBrokerLoggers(alterLogLevelsEntries)
    val alteredLoggerConfig = describeBrokerLoggers()
    assertEquals(newAncestorLogLevel, alteredLoggerConfig.get(ancestorLogger).value())
    assertEquals(LogLevelConfig.INFO_LOG_LEVEL, alteredLoggerConfig.get("kafka.server.ControllerServer").value())
    assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, alteredLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(LogLevelConfig.TRACE_LOG_LEVEL, alteredLoggerConfig.get("kafka.server.ReplicaManager").value())
  }

  /**
    * 1. Assume kafka logger == TRACE
    * 2. Change kafka.server.ControllerServer logger to INFO
    * 3. Unset kafka.server.ControllerServer via AlterConfigOp.OpType.DELETE (resets it to the kafka logger - TRACE)
    * 4. Change kafka logger to ERROR
    * 5. Ensure the kafka.server.ControllerServer logger's level is ERROR (the current kafka logger level)
    */
  @Test
  def testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot(): Unit = {
    client = createAdminClient
    val ancestorLogger = "kafka"
    // step 1 - configure kafka logger
    val initialAncestorLogLevel = LogLevelConfig.TRACE_LOG_LEVEL
    val alterAncestorLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry(ancestorLogger, initialAncestorLogLevel), AlterConfigOp.OpType.SET)
    )
    alterBrokerLoggers(alterAncestorLoggerEntry)
    val initialLoggerConfig = describeBrokerLoggers()
    assertEquals(initialAncestorLogLevel, initialLoggerConfig.get(ancestorLogger).value())
    assertEquals(initialAncestorLogLevel, initialLoggerConfig.get("kafka.server.ControllerServer").value())

    // step 2 - change ControllerServer logger to INFO
    val alterControllerLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET)
    )
    alterBrokerLoggers(alterControllerLoggerEntry)
    val changedControllerLoggerConfig = describeBrokerLoggers()
    assertEquals(initialAncestorLogLevel, changedControllerLoggerConfig.get(ancestorLogger).value())
    assertEquals(LogLevelConfig.INFO_LOG_LEVEL, changedControllerLoggerConfig.get("kafka.server.ControllerServer").value())

    // step 3 - unset ControllerServer logger
    val deleteControllerLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.ControllerServer", ""), AlterConfigOp.OpType.DELETE)
    )
    alterBrokerLoggers(deleteControllerLoggerEntry)
    val deletedControllerLoggerConfig = describeBrokerLoggers()
    assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get(ancestorLogger).value())
    assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get("kafka.server.ControllerServer").value())

    val newAncestorLogLevel = LogLevelConfig.ERROR_LOG_LEVEL
    val newAlterAncestorLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET)
    )
    alterBrokerLoggers(newAlterAncestorLoggerEntry)
    val newAncestorLoggerConfig = describeBrokerLoggers()
    assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get(ancestorLogger).value())
    assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get("kafka.server.ControllerServer").value())
  }

  @Test
  def testIncrementalAlterConfigsForLog4jLogLevelsCanSetToRootLogger(): Unit = {
    client = createAdminClient
    val initialLoggerConfig = describeBrokerLoggers()
    val initialRootLogLevel = initialLoggerConfig.get(LoggingController.ROOT_LOGGER).value()
    val newRootLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL

    val alterRootLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, newRootLogLevel), AlterConfigOp.OpType.SET)
    )

    alterBrokerLoggers(alterRootLoggerEntry, validateOnly = true)
    val validatedRootLoggerConfig = describeBrokerLoggers()
    assertEquals(initialRootLogLevel, validatedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value())

    alterBrokerLoggers(alterRootLoggerEntry)
    val changedRootLoggerConfig = describeBrokerLoggers()
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value())
  }

  @Test
  def testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger(): Unit = {
    client = createAdminClient
    val deleteRootLoggerEntry = util.List.of(
      new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, ""), AlterConfigOp.OpType.DELETE)
    )

    assertTrue(assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(deleteRootLoggerEntry)).getCause.isInstanceOf[InvalidRequestException])
  }

  @Test
  def testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs(): Unit = {
    client = createAdminClient
    val validLoggerName = "kafka.server.KafkaRequestHandler"
    val expectedValidLoggerLogLevel = describeBrokerLoggers().get(validLoggerName)
    def assertLogLevelDidNotChange(): Unit = {
      assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers().get(validLoggerName))
    }

    val appendLogLevelEntries = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.APPEND) // append is not supported
    )
    assertInstanceOf(classOf[InvalidRequestException], assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(appendLogLevelEntries)).getCause)
    assertLogLevelDidNotChange()

    val subtractLogLevelEntries = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SUBTRACT) // subtract is not supported
    )
    assertInstanceOf(classOf[InvalidRequestException], assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(subtractLogLevelEntries)).getCause)
    assertLogLevelDidNotChange()

    val invalidLogLevelLogLevelEntries = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", "OFF"), AlterConfigOp.OpType.SET) // OFF is not a valid log level
    )
    assertInstanceOf(classOf[InvalidConfigurationException], assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(invalidLogLevelLogLevelEntries)).getCause)
    assertLogLevelDidNotChange()

    val invalidLoggerNameLogLevelEntries = util.List.of(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("Some Other LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET) // invalid logger name is not supported
    )
    assertInstanceOf(classOf[InvalidConfigurationException], assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(invalidLoggerNameLogLevelEntries)).getCause)
    assertLogLevelDidNotChange()
  }

  def alterBrokerLoggers(entries: util.Collection[AlterConfigOp], validateOnly: Boolean = false): Unit = {
    client.incrementalAlterConfigs(util.Map.of(brokerLoggerConfigResource, entries), new AlterConfigsOptions().validateOnly(validateOnly))
      .values.get(brokerLoggerConfigResource).get()
  }

  def describeBrokerLoggers(): Config =
    client.describeConfigs(util.List.of(brokerLoggerConfigResource)).values.get(brokerLoggerConfigResource).get()

  @Test
  def testAppendConfigToEmptyDefaultValue(): Unit = {
    testAppendConfig(new Properties(), "0:0", "0:0")
  }

  @Test
  def testAppendConfigToExistentValue(): Unit = {
    val props = new Properties()
    props.setProperty(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1")
    testAppendConfig(props, "0:0", "1:1,0:0")
  }

  private def disableEligibleLeaderReplicas(admin: Admin): Unit = {
    if (metadataVersion.isAtLeast(MetadataVersion.IBP_4_1_IV0)) {
      admin.updateFeatures(
        util.Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, new FeatureUpdate(0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),
        new UpdateFeaturesOptions()).all().get()
    }
  }

  private def testAppendConfig(props: Properties, append: String, expected: String): Unit = {
    client = createAdminClient
    createTopic(topic, topicConfig = props)
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val topicAlterConfigs = util.List.of(
      new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, append), AlterConfigOp.OpType.APPEND),
    )

    val alterResult = client.incrementalAlterConfigs(util.Map.of(
      topicResource, topicAlterConfigs
    ))
    alterResult.all().get(15, TimeUnit.SECONDS)

    ensureConsistentKRaftMetadata()
    val config = client.describeConfigs(util.List.of(topicResource)).all().get().get(topicResource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
    assertEquals(expected, config.value())
  }

  @Test
  def testListClientMetricsResources(): Unit = {
    client = createAdminClient
    client.createTopics(util.Set.of(new NewTopic(topic, partition, 0.toShort)))
    assertTrue(client.listClientMetricsResources().all().get().isEmpty)
    val name = "name"
    val configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, name)
    val configEntry = new ConfigEntry("interval.ms", "111")
    val configOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)
    client.incrementalAlterConfigs(util.Map.of(configResource, util.List.of(configOp))).all().get()
    TestUtils.waitUntilTrue(() => {
      val results = client.listClientMetricsResources().all().get()
      results.size() == 1 && results.iterator().next().equals(new ClientMetricsResourceListing(name))
    }, "metadata timeout")
  }

  @Test
  @Timeout(30)
  def testListClientMetricsResourcesTimeoutMs(): Unit = {
    client = createInvalidAdminClient()
    try {
      val timeoutOption = new ListClientMetricsResourcesOptions().timeoutMs(0)
      val exception = assertThrows(classOf[ExecutionException], () =>
        client.listClientMetricsResources(timeoutOption).all().get())
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  @Test
  def testListConfigResources(): Unit = {
    client = createAdminClient

    // Alter group and client metric config to add group and client metric config resource
    val clientMetric = "client-metrics"
    val group = "group"
    val clientMetricResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetric)
    val groupResource = new ConfigResource(ConfigResource.Type.GROUP, group)
    val alterResult = client.incrementalAlterConfigs(util.Map.of(
      clientMetricResource,
      util.Set.of(new AlterConfigOp(new ConfigEntry("interval.ms", "111"), AlterConfigOp.OpType.SET)),
      groupResource,
      util.Set.of(new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000"), AlterConfigOp.OpType.SET))
    ))
    assertEquals(util.Set.of(clientMetricResource, groupResource), alterResult.values.keySet)
    alterResult.all.get(15, TimeUnit.SECONDS)

    ensureConsistentKRaftMetadata()

    // non-specified config resource type retrieves all config resources
    var configResources = client.listConfigResources().all().get()
    assertEquals(9, configResources.size())
    brokerServers.foreach(b => {
      assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER, b.config.nodeId.toString)))
      assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, b.config.nodeId.toString)))
    })
    assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME)))
    assertTrue(configResources.contains(groupResource))
    assertTrue(configResources.contains(clientMetricResource))

    // BROKER config resource type retrieves only broker config resources
    configResources = client.listConfigResources(util.Set.of(ConfigResource.Type.BROKER), new ListConfigResourcesOptions()).all().get()
    assertEquals(3, configResources.size())
    brokerServers.foreach(b => {
      assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER, b.config.nodeId.toString)))
      assertFalse(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, b.config.nodeId.toString)))
    })
    assertFalse(configResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME)))
    assertFalse(configResources.contains(groupResource))
    assertFalse(configResources.contains(clientMetricResource))

    // BROKER_LOGGER config resource type retrieves only broker logger config resources
    configResources = client.listConfigResources(util.Set.of(ConfigResource.Type.BROKER_LOGGER), new ListConfigResourcesOptions()).all().get()
    assertEquals(3, configResources.size())
    brokerServers.foreach(b => {
      assertFalse(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER, b.config.nodeId.toString)))
      assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, b.config.nodeId.toString)))
    })
    assertFalse(configResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME)))
    assertFalse(configResources.contains(groupResource))
    assertFalse(configResources.contains(clientMetricResource))

    // TOPIC config resource type retrieves only topic config resources
    configResources = client.listConfigResources(util.Set.of(ConfigResource.Type.TOPIC), new ListConfigResourcesOptions()).all().get()
    assertEquals(1, configResources.size())
    assertTrue(configResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, Topic.GROUP_METADATA_TOPIC_NAME)))

    // GROUP config resource type retrieves only group config resources
    configResources = client.listConfigResources(util.Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all().get()
    assertEquals(1, configResources.size())
    assertTrue(configResources.contains(groupResource))

    // CLIENT_METRICS config resource type retrieves only client metric config resources
    configResources = client.listConfigResources(util.Set.of(ConfigResource.Type.CLIENT_METRICS), new ListConfigResourcesOptions()).all().get()
    assertEquals(1, configResources.size())
    assertTrue(configResources.contains(clientMetricResource))

    // UNKNOWN config resource type gets UNSUPPORTED_VERSION error
    assertThrows(classOf[ExecutionException], () => {
      client.listConfigResources(util.Set.of(ConfigResource.Type.UNKNOWN), new ListConfigResourcesOptions()).all().get()
    })
  }

  @Test
  @Timeout(30)
  def testListConfigResourcesTimeoutMs(): Unit = {
    client = createInvalidAdminClient()
    try {
      val timeoutOption = new ListConfigResourcesOptions().timeoutMs(0)
      val exception = assertThrows(classOf[ExecutionException], () =>
        client.listConfigResources(util.Set.of(), timeoutOption).all().get())
      assertInstanceOf(classOf[TimeoutException], exception.getCause)
    } finally client.close(time.Duration.ZERO)
  }

  /**
   * Test that createTopics returns the dynamic configurations of the topics that were created.
   *
   * Note: this test requires some custom static broker and controller configurations, which are set up in
   * BaseAdminIntegrationTest.modifyConfigs.
   */
  @Test
  def testCreateTopicsReturnsConfigs(): Unit = {
    client = Admin.create(super.createConfig)

    val newLogRetentionProperties = new Properties
    newLogRetentionProperties.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "10800000")
    TestUtils.incrementalAlterConfigs(null, client, newLogRetentionProperties, perBrokerConfig = false)
      .all().get(15, TimeUnit.SECONDS)

    val newLogCleanerDeleteRetention = new Properties
    newLogCleanerDeleteRetention.put(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, "34")
    TestUtils.incrementalAlterConfigs(brokers, client, newLogCleanerDeleteRetention, perBrokerConfig = true)
      .all().get(15, TimeUnit.SECONDS)

    // In KRaft mode, we don't yet support altering configs on controller nodes, except by setting
    // default node configs. Therefore, we have to set the dynamic config directly to test this.
    val controllerNodeResource = new ConfigResource(ConfigResource.Type.BROKER,
      controllerServer.config.nodeId.toString)
    controllerServer.controller.incrementalAlterConfigs(ANONYMOUS_CONTEXT,
      util.Map.of(controllerNodeResource,
        util.Map.of(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP,
          new SimpleImmutableEntry(AlterConfigOp.OpType.SET, "34"))), false).get()
    ensureConsistentKRaftMetadata()

    waitUntilTrue(() => brokers.forall(_.config.originals.getOrDefault(
      CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, "").toString.equals("34")),
      s"Timed out waiting for change to ${CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP}",
      waitTimeMs = 60000L)

    waitUntilTrue(() => brokers.forall(_.config.originals.getOrDefault(
      ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "").toString.equals("10800000")),
      s"Timed out waiting for change to ${ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG}",
      waitTimeMs = 60000L)

    val newTopics = Seq(new NewTopic("foo", util.Map.of(0: Integer, util.List.of[Integer](1, 2),
      1: Integer, util.List.of[Integer](2, 0))).
      configs(util.Map.of(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "9999999")),
      new NewTopic("bar", 3, 3.toShort),
      new NewTopic("baz", Optional.empty[Integer], Optional.empty[java.lang.Short])
    )
    val result = client.createTopics(newTopics.asJava)
    result.all.get()
    waitForTopics(client, newTopics.map(_.name()).toList, List())

    assertEquals(2, result.numPartitions("foo").get())
    assertEquals(2, result.replicationFactor("foo").get())
    assertEquals(3, result.numPartitions("bar").get())
    assertEquals(3, result.replicationFactor("bar").get())
    assertEquals(configs.head.numPartitions, result.numPartitions("baz").get())
    assertEquals(configs.head.defaultReplicationFactor, result.replicationFactor("baz").get())

    val topicConfigs = result.config("foo").get()

    // From the topic configuration defaults.
    assertEquals(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete",
      ConfigSource.DEFAULT_CONFIG, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG))

    // From dynamic cluster config via the synonym LogRetentionTimeHoursProp.
    assertEquals(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "10800000",
      ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.RETENTION_MS_CONFIG))

    // From dynamic broker config via LogCleanerDeleteRetentionMsProp.
    assertEquals(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "34",
      ConfigSource.DYNAMIC_BROKER_CONFIG, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.DELETE_RETENTION_MS_CONFIG))

    // From static broker config by SegmentJitterMsProp.
    assertEquals(new ConfigEntry(TopicConfig.SEGMENT_JITTER_MS_CONFIG, "123",
      ConfigSource.STATIC_BROKER_CONFIG, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG))

    // From static broker config by the synonym LogRollTimeHoursProp.
    val segmentMsPropType = ConfigSource.STATIC_BROKER_CONFIG
    assertEquals(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, "7200000",
      segmentMsPropType, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.SEGMENT_MS_CONFIG))

    // From the dynamic topic config.
    assertEquals(new ConfigEntry(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "9999999",
      ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, util.List.of, null, null),
      topicConfigs.get(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG))
  }

  class BackgroundConsumerSet(defaultConsumerConfig: Properties) {
    private val consumerSet: scala.collection.mutable.Set[Consumer[Array[Byte], Array[Byte]]] = scala.collection.mutable.Set.empty
    private val consumerThreads: scala.collection.mutable.Set[Thread] = scala.collection.mutable.Set.empty
    private var startLatch: CountDownLatch = new CountDownLatch(0)
    private var stopLatch: CountDownLatch = new CountDownLatch(0)
    private var consumerThreadRunning = new AtomicBoolean(false)

    def addConsumer(topic: String, configOverrides: Properties = new Properties()): Unit = {
      val newConsumerConfig = defaultConsumerConfig.clone().asInstanceOf[Properties]
      newConsumerConfig.putAll(configOverrides)

      val consumer = createConsumer(configOverrides = newConsumerConfig)
      val consumerThread = createConsumerThread(consumer, topic)
      consumerSet.add(consumer)
      consumerThreads.add(consumerThread)
    }

    def start(): Unit = {
      startLatch = new CountDownLatch(consumerSet.size)
      stopLatch = new CountDownLatch(consumerSet.size)
      consumerThreadRunning = new AtomicBoolean(true)
      consumerThreads.foreach(_.start())
      assertTrue(startLatch.await(30000, TimeUnit.MILLISECONDS), "Failed to start consumer threads in time")
    }

    def stop(): Unit = {
      consumerSet.foreach(_.wakeup())
      consumerThreadRunning.set(false)
      assertTrue(stopLatch.await(30000, TimeUnit.MILLISECONDS), "Failed to stop consumer threads in time")
    }

    def close(): Unit = {
      // stop the consumers and wait for consumer threads stopped
      stop()
      consumerThreads.foreach(_.join())
    }

    private def createConsumerThread[K,V](consumer: Consumer[K,V], topic: String): Thread = {
      new Thread {
        override def run : Unit = {
          consumer.subscribe(util.Set.of(topic))
          try {
            while (consumerThreadRunning.get()) {
              consumer.poll(JDuration.ofSeconds(5))
              if (!consumer.assignment.isEmpty && startLatch.getCount > 0L)
                startLatch.countDown()
              try {
                consumer.commitSync()
              } catch {
                case _: CommitFailedException => // Ignore and retry on next iteration.
              }
            }
          } catch {
            case _: WakeupException => // ignore
            case _: InterruptException => // ignore
          } finally {
            consumer.close()
            stopLatch.countDown()
          }
        }
      }
    }
  }

  @Test
  def testDescribeStreamsGroups(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 1

    val config = createConfig
    client = Admin.create(config)

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)

    val streams = createStreamsGroup(
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId
    )
    streams.poll(JDuration.ofMillis(500L))

    try {
      TestUtils.waitUntilTrue(() => {
        val firstGroup = client.listGroups().all().get().stream()
          .filter(g => g.groupId() == streamsGroupId).findFirst().orElse(null)
        firstGroup.groupState().orElse(null) == GroupState.STABLE && firstGroup.groupId() == streamsGroupId
      }, "Streams group did not transition to STABLE before timeout")

      // Verify the describe call works correctly
      val describedGroups = client.describeStreamsGroups(util.List.of(streamsGroupId)).all().get()
      val group = describedGroups.get(streamsGroupId)
      assertNotNull(group)
      assertEquals(streamsGroupId, group.groupId())
      assertFalse(group.members().isEmpty)
      assertNotNull(group.subtopologies())
      assertFalse(group.subtopologies().isEmpty)

      // Verify the topology contains the expected source and sink topics
      val subtopologies = group.subtopologies().asScala
      assertTrue(subtopologies.exists(subtopology =>
        subtopology.sourceTopics().contains(testTopicName)))

      // Test describing a non-existing group
      val nonExistingGroup = "non_existing_stream_group"
      val describedNonExistingGroupResponse = client.describeStreamsGroups(util.List.of(nonExistingGroup))
      assertFutureThrows(classOf[GroupIdNotFoundException], describedNonExistingGroupResponse.all())

    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testDescribeStreamsGroupsNotReady(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"

    val config = createConfig
    client = Admin.create(config)

    val streams = createStreamsGroup(
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId
    )
    streams.poll(JDuration.ofMillis(500L))

    try {
      TestUtils.waitUntilTrue(() => {
        val firstGroup = client.listGroups().all().get().stream()
          .filter(g => g.groupId() == streamsGroupId).findFirst().orElse(null)
        firstGroup.groupState().orElse(null) == GroupState.NOT_READY && firstGroup.groupId() == streamsGroupId
      }, "Streams group did not transition to NOT_READY before timeout")

      // Verify the describe call works correctly
      val describedGroups = client.describeStreamsGroups(util.List.of(streamsGroupId)).all().get()
      val group = describedGroups.get(streamsGroupId)
      assertNotNull(group)
      assertEquals(streamsGroupId, group.groupId())
      assertFalse(group.members().isEmpty)
      assertNotNull(group.subtopologies())
      assertFalse(group.subtopologies().isEmpty)

      // Verify the topology contains the expected source and sink topics
      val subtopologies = group.subtopologies().asScala
      assertTrue(subtopologies.exists(subtopology =>
        subtopology.sourceTopics().contains(testTopicName)))

    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testDescribeStreamsGroupsForStatelessTopology(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 1

    val config = createConfig
    client = Admin.create(config)

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)

    val streams = createStreamsGroup(
      inputTopics = Set(testTopicName),
      streamsGroupId = streamsGroupId
    )
    streams.poll(JDuration.ofMillis(500L))

    try {
      TestUtils.waitUntilTrue(() => {
        val firstGroup = client.listGroups().all().get().stream().findFirst().orElse(null)
        firstGroup.groupState().orElse(null) == GroupState.STABLE && firstGroup.groupId() == streamsGroupId
      }, "Streams group did not transition to STABLE before timeout")

      // Verify the describe call works correctly
      val describedGroups = client.describeStreamsGroups(util.List.of(streamsGroupId)).all().get()
      val group = describedGroups.get(streamsGroupId)
      assertNotNull(group)
      assertEquals(streamsGroupId, group.groupId())
      assertFalse(group.members().isEmpty)
      assertNotNull(group.subtopologies())
      assertFalse(group.subtopologies().isEmpty)

      // Verify the topology contains the expected source and sink topics
      val subtopologies = group.subtopologies().asScala
      assertTrue(subtopologies.exists(subtopology =>
        subtopology.sourceTopics().contains(testTopicName)))

      // Test describing a non-existing group
      val nonExistingGroup = "non_existing_stream_group"
      val describedNonExistingGroupResponse = client.describeStreamsGroups(util.List.of(nonExistingGroup))
      assertFutureThrows(classOf[GroupIdNotFoundException], describedNonExistingGroupResponse.all())

    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
    }
  }
  
  @Test
  def testDeleteStreamsGroups(): Unit = {
    val testTopicName = "test_topic"
    val testNumPartitions = 3
    val testNumStreamsGroup = 3

    val targetDeletedGroups = util.List.of("stream_group_id_2", "stream_group_id_3")
    val targetRemainingGroups = util.List.of("stream_group_id_1")

    val config = createConfig
    client = Admin.create(config)

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)

    val streamsList = scala.collection.mutable.ListBuffer[(String, AsyncKafkaConsumer[_,_])]()

    try {
      for (i <- 1 to testNumStreamsGroup) {
        val streamsGroupId = s"stream_group_id_$i"

        val streams = createStreamsGroup(
          inputTopics = Set(testTopicName),
          changelogTopics = Set(testTopicName + "-changelog"),
          streamsGroupId = streamsGroupId,
        )
        streams.poll(JDuration.ofMillis(500L))
        streamsList += ((streamsGroupId, streams))
      }

      TestUtils.waitUntilTrue(() => {
        val groups = client.listGroups().all().get()
        groups.stream()
          .anyMatch(g => g.groupId().startsWith("stream_group_id_")) &&  testNumStreamsGroup == groups.size()
      }, "Streams groups not ready to delete yet")

      // Test deletion of non-empty existing groups
      var deleteStreamsGroupResult = client.deleteStreamsGroups(targetDeletedGroups)
      assertFutureThrows(classOf[GroupNotEmptyException], deleteStreamsGroupResult.all())
      assertEquals(2, deleteStreamsGroupResult.deletedGroups().size())

      // Stop and clean up the streams for the groups that are going to be deleted
      streamsList
        .filter { case (groupId, _) => targetDeletedGroups.contains(groupId) }
        .foreach { case (_, streams) =>
          streams.close()
        }

      val listTopicResult = client.listTopics()
      assertEquals(2, listTopicResult.names().get().size())

      // Test deletion of emptied existing streams groups
      deleteStreamsGroupResult = client.deleteStreamsGroups(targetDeletedGroups)
      assertEquals(2, deleteStreamsGroupResult.deletedGroups().size())

      // Wait for the deleted groups to be removed
      TestUtils.waitUntilTrue(() => {
        val groupIds = client.listGroups().all().get().asScala.map(_.groupId()).toSet
        targetDeletedGroups.asScala.forall(id => !groupIds.contains(id))
      }, "Deleted groups not yet deleted")

      // Verify that the deleted groups are no longer present
      val remainingGroups = client.listGroups().all().get()
      assertEquals(targetRemainingGroups.size(), remainingGroups.size())
      remainingGroups.stream().forEach(g => {
        assertTrue(targetRemainingGroups.contains(g.groupId()))
      })

      // Test deletion of a non-existing group
      val nonExistingGroup = "non_existing_stream_group"
      val deleteNonExistingGroupResult = client.deleteStreamsGroups(util.List.of(nonExistingGroup))
      assertFutureThrows(classOf[GroupIdNotFoundException], deleteNonExistingGroupResult.all())
      assertEquals(deleteNonExistingGroupResult.deletedGroups().size(), 1)

    } finally{
      streamsList.foreach { case (_, streams) =>
        streams.close()
      }
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testListStreamsGroupOffsets(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 3

    val config = createConfig
    client = Admin.create(config)
    val producer = createProducer(configOverrides = new Properties())

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)

    // Producer sends messages
    val numRecords = 20

    for (i <- 1 to numRecords) {
      TestUtils.waitUntilTrue(() => {
        val producerRecord = producer.send(
            new ProducerRecord[Array[Byte], Array[Byte]](testTopicName, s"key-$i".getBytes(), s"value-$i".getBytes()))
          .get()
        producerRecord != null && producerRecord.topic() == testTopicName
      }, "Fail to produce record to topic")
    }

    val consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streams = createStreamsGroup(
      configOverrides = consumerConfig,
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId,
    )

    try {
      var counter = 0

      def verifyRecordCount(records: ConsumerRecords[Nothing, Nothing]): Boolean = {
        counter += records.count()
        counter >= numRecords
      }
      TestUtils.pollRecordsUntilTrue(
        streams,
        verifyRecordCount,
        s"Consumer not assigned to partitions"
      )

      streams.commitSync()

      TestUtils.waitUntilTrue(() => {
        val firstGroup = client.listGroups().all().get().stream().findFirst().orElse(null)
        firstGroup.groupState().orElse(null) == GroupState.STABLE && firstGroup.groupId() == streamsGroupId
      }, "Streams group did not transition to STABLE before timeout")

      val allTopicPartitions = client.listStreamsGroupOffsets(
        util.Map.of(streamsGroupId, new ListStreamsGroupOffsetsSpec())
      ).partitionsToOffsetAndMetadata(streamsGroupId).get()
      assertNotNull(allTopicPartitions)
      assertEquals(allTopicPartitions.size(), 3)
      allTopicPartitions.forEach((topicPartition, offsetAndMetadata) => {
        assertNotNull(topicPartition)
        assertNotNull(offsetAndMetadata)
        assertTrue(topicPartition.topic().startsWith(testTopicName))
        assertTrue(offsetAndMetadata.offset() >= 0)
      })

    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
      Utils.closeQuietly(producer, "producer")
    }
  }

  @Test
  def testDeleteStreamsGroupOffsets(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 3

    val config = createConfig
    client = Admin.create(config)
    val producer = createProducer(configOverrides = new Properties())

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)
    // Producer sends messages
    val numRecords = 20

    for (i <- 1 to numRecords) {
      TestUtils.waitUntilTrue(() => {
        val producerRecord = producer.send(
            new ProducerRecord[Array[Byte], Array[Byte]](testTopicName, s"key-$i".getBytes(), s"value-$i".getBytes()))
          .get()
        producerRecord != null && producerRecord.topic() == testTopicName
      }, "Fail to produce record to topic")
    }

    val consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streams = createStreamsGroup(
      configOverrides = consumerConfig,
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId,
    )

    try {
      var counter = 0

      def verifyRecordCount(records: ConsumerRecords[Nothing, Nothing]): Boolean = {
        counter += records.count()
        counter >= numRecords
      }
      TestUtils.pollRecordsUntilTrue(
        streams,
        verifyRecordCount,
        s"Consumer not assigned to partitions"
      )

      streams.commitSync()

      // List streams group offsets
      TestUtils.waitUntilTrue(() => {
        val allTopicPartitions = client.listStreamsGroupOffsets(
          util.Map.of(streamsGroupId, new ListStreamsGroupOffsetsSpec())
        ).partitionsToOffsetAndMetadata(streamsGroupId).get()
        allTopicPartitions!=null && allTopicPartitions.size() == testNumPartitions
      },"Streams group offsets not ready to list yet")

      // Verify running Kstreams group cannot delete its own offsets
      var deleteStreamsGroupOffsetsResult = client.deleteStreamsGroupOffsets(streamsGroupId, util.Set.of(new TopicPartition(testTopicName, 0)))
      assertFutureThrows(classOf[GroupSubscribedToTopicException], deleteStreamsGroupOffsetsResult.all())

      // Verity stopped Kstreams group can delete its own offsets
      streams.close()
      TestUtils.waitUntilTrue(() => {
        val groupDescription = client.describeStreamsGroups(util.List.of(streamsGroupId)).all().get()
        groupDescription.get(streamsGroupId).groupState() == GroupState.EMPTY
      }, "Streams group not closed yet")
      deleteStreamsGroupOffsetsResult = client.deleteStreamsGroupOffsets(streamsGroupId, util.Set.of(new TopicPartition(testTopicName, 0)))
      val res = deleteStreamsGroupOffsetsResult.partitionResult(new TopicPartition(testTopicName, 0)).get()
      assertNull(res)

      // Verify the group offsets after deletion
      val allTopicPartitions = client.listStreamsGroupOffsets(
        util.Map.of(streamsGroupId, new ListStreamsGroupOffsetsSpec())
      ).partitionsToOffsetAndMetadata(streamsGroupId).get()
      assertEquals(testNumPartitions-1, allTopicPartitions.size())

      // Verify non-existing topic partition couldn't be deleted
      val deleteStreamsGroupOffsetsResultWithFakeTopic = client.deleteStreamsGroupOffsets(streamsGroupId, util.Set.of(new TopicPartition("mock-topic", 1)))
      assertFutureThrows(classOf[UnknownTopicOrPartitionException], deleteStreamsGroupOffsetsResultWithFakeTopic.all())
      val deleteStreamsGroupOffsetsResultWithFakePartition = client.deleteStreamsGroupOffsets(streamsGroupId, util.Set.of(new TopicPartition(testTopicName, testNumPartitions)))
      assertFutureThrows(classOf[UnknownTopicOrPartitionException], deleteStreamsGroupOffsetsResultWithFakePartition.all())
    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
      Utils.closeQuietly(producer, "producer")
    }
  }

  @Test
  def testAlterStreamsGroupOffsets(): Unit = {
    val streamsGroupId = "stream_group_id"
    val testTopicName = "test_topic"
    val testNumPartitions = 3

    val config = createConfig
    client = Admin.create(config)
    val producer = createProducer(configOverrides = new Properties())

    prepareTopics(List(testTopicName), testNumPartitions)
    prepareRecords(testTopicName)

    // Producer sends messages
    val numRecords = 20

    for (i <- 1 to numRecords) {
      TestUtils.waitUntilTrue(() => {
        val producerRecord = producer.send(
            new ProducerRecord[Array[Byte], Array[Byte]](testTopicName, s"key-$i".getBytes(), s"value-$i".getBytes()))
          .get()
        producerRecord != null && producerRecord.topic() == testTopicName
      }, "Fail to produce record to topic")
    }

    val consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streams = createStreamsGroup(
      configOverrides = consumerConfig,
      inputTopics = Set(testTopicName),
      changelogTopics = Set(testTopicName + "-changelog"),
      streamsGroupId = streamsGroupId,
    )

    try {
      var counter = 0

      def verifyRecordCount(records: ConsumerRecords[Nothing, Nothing]): Boolean = {
        counter += records.count()
        counter >= numRecords
      }
      TestUtils.pollRecordsUntilTrue(
        streams,
        verifyRecordCount,
        s"Consumer not assigned to partitions"
      )

      streams.commitSync()

      // List streams group offsets
      TestUtils.waitUntilTrue(() => {
        val allTopicPartitions = client.listStreamsGroupOffsets(
          util.Map.of(streamsGroupId, new ListStreamsGroupOffsetsSpec())
        ).partitionsToOffsetAndMetadata(streamsGroupId).get()
        allTopicPartitions!=null && allTopicPartitions.size() == testNumPartitions
      },"Streams group offsets not ready to list yet")

      // Verity stopped Kstreams group can delete its own offsets
      streams.close()
      TestUtils.waitUntilTrue(() => {
        val groupDescription = client.describeStreamsGroups(util.List.of(streamsGroupId)).all().get()
        groupDescription.get(streamsGroupId).groupState() == GroupState.EMPTY
      }, "Streams group not closed yet")

      val offsets = util.Map.of(
        new TopicPartition(testTopicName, 0), new OffsetAndMetadata(1L),
        new TopicPartition(testTopicName, 1), new OffsetAndMetadata(10L)
      )
      val alterStreamsGroupOffsetsResult = client.alterStreamsGroupOffsets(streamsGroupId, offsets)
      val res0 =  alterStreamsGroupOffsetsResult.partitionResult(new TopicPartition(testTopicName, 0)).get()
      val res1 =  alterStreamsGroupOffsetsResult.partitionResult(new TopicPartition(testTopicName, 1)).get()
      assertTrue(res0 == null && res1 == null, "Alter streams group offsets should return null for each partition result")

      val allTopicPartitions = client.listStreamsGroupOffsets(
        util.Map.of(streamsGroupId, new ListStreamsGroupOffsetsSpec())
      ).partitionsToOffsetAndMetadata(streamsGroupId).get()
      assertNotNull(allTopicPartitions)
      assertEquals(testNumPartitions, allTopicPartitions.size())
      assertEquals(1L, allTopicPartitions.get(new TopicPartition(testTopicName, 0)).offset())
      assertEquals(10L, allTopicPartitions.get(new TopicPartition(testTopicName, 1)).offset())

    } finally {
      Utils.closeQuietly(streams, "streams")
      Utils.closeQuietly(client, "adminClient")
      Utils.closeQuietly(producer, "producer")
    }
  }
}

object PlaintextAdminIntegrationTest {

  def checkValidAlterConfigs(
    admin: Admin,
    test: KafkaServerTestHarness,
    topicResource1: ConfigResource,
    topicResource2: ConfigResource,
    maxMessageBytes: String,
    retentionMs: String): Unit = {
    // Alter topics
    val alterConfigs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    alterConfigs.put(topicResource1, util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, "1000"), OpType.SET)))
    alterConfigs.put(topicResource2, util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), OpType.SET)
    ))
    var alterResult = admin.incrementalAlterConfigs(alterConfigs)

    assertEquals(util.Set.of(topicResource1, topicResource2), alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were updated correctly
    test.ensureConsistentKRaftMetadata()
    // Intentionally include duplicate resources to test if describeConfigs can handle them correctly.
    var describeResult = admin.describeConfigs(util.List.of(topicResource1, topicResource2, topicResource2))
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topicResource1).get(TopicConfig.FLUSH_MS_CONFIG).value)
    assertEquals(maxMessageBytes, configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).value)
    assertEquals(retentionMs, configs.get(topicResource1).get(TopicConfig.RETENTION_MS_CONFIG).value)

    assertEquals("0.9", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals("lz4", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    // Alter topics with validateOnly=true
    alterConfigs.put(topicResource1, util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10"), OpType.SET)))
    alterConfigs.put(topicResource2, util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.3"), OpType.SET)))
    alterResult = admin.incrementalAlterConfigs(alterConfigs, new AlterConfigsOptions().validateOnly(true))

    assertEquals(util.Set.of(topicResource1, topicResource2), alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(util.List.of(topicResource1, topicResource2))
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals(maxMessageBytes, configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).value)
    assertEquals("0.9", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
  }

  def checkInvalidAlterConfigs(
    test: KafkaServerTestHarness,
    admin: Admin
  ): Unit = {
    // Create topics
    val topic1 = "invalid-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopicWithAdmin(admin, topic1, test.brokers, test.controllerServers, numPartitions = 1, replicationFactor = 1)

    val topic2 = "invalid-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopicWithAdmin(admin, topic2, test.brokers, test.controllerServers, numPartitions = 1, replicationFactor = 1)

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, test.brokers.head.config.brokerId.toString)

    // Alter configs: first and third are invalid, second is valid
    val alterConfigs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    alterConfigs.put(topicResource1, util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), OpType.SET)
    ))
    alterConfigs.put(topicResource2, util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"), OpType.SET)))
    alterConfigs.put(brokerResource, util.List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0,INTERNAL://localhost:0"), OpType.SET)))
    var alterResult = admin.incrementalAlterConfigs(alterConfigs)

    assertEquals(util.Set.of(topicResource1, topicResource2, brokerResource), alterResult.values.keySet)
    assertFutureThrows(classOf[InvalidConfigurationException], alterResult.values.get(topicResource1))
    alterResult.values.get(topicResource2).get
    assertFutureThrows(classOf[InvalidRequestException], alterResult.values.get(brokerResource))

    // Verify that first and third resources were not updated and second was updated
    test.ensureConsistentKRaftMetadata()
    var describeResult = admin.describeConfigs(util.List.of(topicResource1, topicResource2, brokerResource))
    var configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString,
      configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT,
      configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(brokerResource).get(ServerConfigs.COMPRESSION_TYPE_CONFIG).value)

    // Alter configs with validateOnly = true: first and third are invalid, second is valid
    alterConfigs.put(topicResource1, util.List.of(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), OpType.SET)
    ))
    alterConfigs.put(topicResource2, util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), OpType.SET)))
    alterConfigs.put(brokerResource, util.List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0,INTERNAL://localhost:0"), OpType.SET)))
    alterResult = admin.incrementalAlterConfigs(alterConfigs, new AlterConfigsOptions().validateOnly(true))

    assertEquals(util.Set.of(topicResource1, topicResource2, brokerResource), alterResult.values.keySet)
    assertFutureThrows(classOf[InvalidConfigurationException], alterResult.values.get(topicResource1))
    alterResult.values.get(topicResource2).get
    assertFutureThrows(classOf[InvalidRequestException], alterResult.values.get(brokerResource))

    // Verify that no resources are updated since validate_only = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(util.List.of(topicResource1, topicResource2, brokerResource))
    configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString,
      configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT,
      configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(brokerResource).get(ServerConfigs.COMPRESSION_TYPE_CONFIG).value)
  }
}
