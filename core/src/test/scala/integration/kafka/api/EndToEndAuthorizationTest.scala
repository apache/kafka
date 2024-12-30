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

import com.yammer.metrics.core.Gauge

import java.util.{Collections, Properties}
import java.util.concurrent.ExecutionException
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import kafka.utils._
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType._
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.resource._
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.security.auth._
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource}

import scala.jdk.CollectionConverters._

/**
  * The test cases here verify that a producer authorized to publish to a topic
  * is able to, and that consumers in a group authorized to consume are able to
  * do so.
  *
  * This test relies on a chain of test harness traits to set up. It directly
  * extends IntegrationTestHarness. IntegrationTestHarness creates producers and
  * consumers, and it extends KafkaServerTestHarness. KafkaServerTestHarness starts
  * brokers.
  *
  * To start brokers we need to set a cluster ACL, which happens optionally in KafkaServerTestHarness.
  * The remaining ACLs to enable access to producers and consumers are set here.
  *
  * Finally, we rely on SaslSetup to bootstrap and setup Kerberos. We don't use
  * SaslTestHarness here directly because it extends QuorumTestHarness, and we
  * would end up with QuorumTestHarness twice.
  */
@Timeout(60)
abstract class EndToEndAuthorizationTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3

  val numRecords = 1
  val groupPrefix = "gr"
  val group = s"${groupPrefix}oup"
  val topicPrefix = "e2e"
  val topic = s"${topicPrefix}topic"
  val wildcard = "*"
  val part = 0
  val tp = new TopicPartition(topic, part)

  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))

  val topicResource = new ResourcePattern(TOPIC, topic, LITERAL)
  val groupResource =  new ResourcePattern(GROUP, group, LITERAL)
  val clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  val prefixedTopicResource =  new ResourcePattern(TOPIC, topicPrefix, PREFIXED)
  val prefixedGroupResource =  new ResourcePattern(GROUP, groupPrefix, PREFIXED)
  val wildcardTopicResource =  new ResourcePattern(TOPIC, wildcard, LITERAL)
  val wildcardGroupResource =  new ResourcePattern(GROUP, wildcard, LITERAL)

  def clientPrincipal: KafkaPrincipal
  def kafkaPrincipal: KafkaPrincipal

  def GroupReadAcl = Set(new AccessControlEntry(clientPrincipal.toString, WILDCARD_HOST, READ, ALLOW))
  def TopicReadAcl = Set(new AccessControlEntry(clientPrincipal.toString, WILDCARD_HOST, READ, ALLOW))
  def TopicWriteAcl = Set(new AccessControlEntry(clientPrincipal.toString, WILDCARD_HOST, WRITE, ALLOW))
  def TopicDescribeAcl = Set(new AccessControlEntry(clientPrincipal.toString, WILDCARD_HOST, DESCRIBE, ALLOW))
  def TopicCreateAcl = Set(new AccessControlEntry(clientPrincipal.toString, WILDCARD_HOST, CREATE, ALLOW))

  def AclTopicWrite(topicResource : ResourcePattern = topicResource) = new AclBinding(topicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  def AclTopicCreate(topicResource : ResourcePattern = topicResource) = new AclBinding(topicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.CREATE, AclPermissionType.ALLOW))
  def AclTopicDescribe(topicResource : ResourcePattern = topicResource) = new AclBinding(topicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
  def AclTopicRead(topicResource : ResourcePattern = topicResource) = new AclBinding(topicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))
  def AclGroupRead = new AclBinding(groupResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))

  def AclWildcardTopicWrite = new AclBinding(wildcardTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  def AclWildcardTopicCreate = new AclBinding(wildcardTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.CREATE, AclPermissionType.ALLOW))
  def AclWildcardTopicDescribe = new AclBinding(wildcardTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
  def AclWildcardTopicRead = new AclBinding(wildcardTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))
  def AclWildcardGroupRead = new AclBinding(wildcardGroupResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))

  def AclPrefixedTopicWrite = new AclBinding(prefixedTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  def AclPrefixedTopicCreate = new AclBinding(prefixedTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.CREATE, AclPermissionType.ALLOW))
  def AclPrefixedTopicDescribe = new AclBinding(prefixedTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
  def AclPrefixedTopicRead = new AclBinding(prefixedTopicResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))
  def AclPrefixedGroupRead = new AclBinding(prefixedGroupResource,
    new AccessControlEntry(clientPrincipal.toString, "*", AclOperation.READ, AclPermissionType.ALLOW))

  // Some needed configuration for brokers, producers, and consumers
  this.serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
  this.serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
  this.serverConfig.setProperty(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
  this.serverConfig.setProperty(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "3")
  this.serverConfig.setProperty(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, "1500")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1500")


  /**
    * Starts MiniKDC and only then sets up the parent trait.
    */
  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.serverConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString)
    this.controllerConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString + ";" + "User:ANONYMOUS")
    this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)
    this.controllerConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)

    super.setUp(testInfo)

    // create the test topic with all the brokers as replicas
    val superuserAdminClient = createSuperuserAdminClient()
    TestUtils.createTopicWithAdmin(admin = superuserAdminClient, topic = topic, brokers = brokers, controllers = controllerServers,
      replicationFactor = 3, topicConfig = new Properties)
  }

  /**
    * Closes MiniKDC last when tearing down.
    */
  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  /**
    * Tests the ability of producing and consuming with the appropriate ACLs set.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceConsumeViaAssign(quorum: String, groupProtocol: String): Unit = {
    setAclsAndProduce(tp)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  protected def confirmReauthenticationMetrics(): Unit = {
    val expiredConnectionsKilledCountTotal = getGauge("ExpiredConnectionsKilledCount").value()
    brokers.foreach { s =>
        val numExpiredKilled = TestUtils.totalMetricValue(s, "expired-connections-killed-count")
        assertEquals(0, numExpiredKilled, "Should have been zero expired connections killed: " + numExpiredKilled + "(total=" + expiredConnectionsKilledCountTotal + ")")
    }
    assertEquals(0, expiredConnectionsKilledCountTotal, 0.0, "Should have been zero expired connections killed total")
    brokers.foreach { s =>
      assertEquals(0, TestUtils.totalMetricValue(s, "failed-reauthentication-total"), "failed re-authentications not 0")
    }
  }

  private def getGauge(metricName: String) = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName == metricName }
      .getOrElse(throw new RuntimeException( "Unable to find metric " + metricName))
      ._2.asInstanceOf[Gauge[Double]]
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceConsumeViaSubscribe(quorum: String, groupProtocol: String): Unit = {
    setAclsAndProduce(tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceConsumeWithWildcardAcls(quorum: String, groupProtocol: String): Unit = {
    setWildcardResourceAcls()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceConsumeWithPrefixedAcls(quorum: String, groupProtocol: String): Unit = {
    setPrefixedResourceAcls()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceConsumeTopicAutoCreateTopicCreateAcl(quorum: String, groupProtocol: String): Unit = {
    // topic2 is not created on setup()
    val tp2 = new TopicPartition("topic2", 0)
    setAclsAndProduce(tp2)
    val consumer = createConsumer()
    consumer.assign(List(tp2).asJava)
    consumeRecords(consumer, numRecords, topic = tp2.topic)
    confirmReauthenticationMetrics()
  }

  private def setWildcardResourceAcls(): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclWildcardTopicWrite, AclWildcardTopicCreate, AclWildcardTopicDescribe, AclWildcardTopicRead).asJava).values
    superuserAdminClient.createAcls(List(AclWildcardGroupRead).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, wildcardTopicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, wildcardGroupResource)
    }
  }

  private def setPrefixedResourceAcls(): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclPrefixedTopicWrite, AclPrefixedTopicCreate, AclPrefixedTopicDescribe, AclPrefixedTopicRead).asJava).values
    superuserAdminClient.createAcls(List(AclPrefixedGroupRead).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, prefixedTopicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, prefixedGroupResource)
    }
  }

  private def setReadAndWriteAcls(tp: TopicPartition): Unit = {
    val topicResource = new ResourcePattern(TOPIC, tp.topic, LITERAL)
    val superuserAdminClient = createSuperuserAdminClient()

    superuserAdminClient.createAcls(List(AclTopicWrite(topicResource), AclTopicCreate(topicResource), AclTopicDescribe(topicResource)).asJava).values
    superuserAdminClient.createAcls(List(AclTopicRead(topicResource)).asJava).values
    superuserAdminClient.createAcls(List(AclGroupRead).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get,
        new ResourcePattern(TOPIC, tp.topic, LITERAL))
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  protected def setAclsAndProduce(tp: TopicPartition): Unit = {
    setReadAndWriteAcls(tp)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  private def setConsumerGroupAcls(): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclGroupRead).asJava).values
    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  /**
    * Tests that producer, consumer and adminClient fail to publish messages, consume
    * messages and describe topics respectively when the describe ACL isn't set.
    * Also verifies that subsequent publish, consume and describe to authorized topic succeeds.
    */
  @ParameterizedTest(name = "{displayName}.quorum={0}.groupProtocol={1}.isIdempotenceEnabled={2}")
  @CsvSource(value = Array(
    "kraft, classic, true",
    //"kraft, consumer, true",
    "kraft, classic, false",
    //"kraft, consumer, false",
  ))
  def testNoDescribeProduceOrConsumeWithoutTopicDescribeAcl(quorum:String, groupProtocol:String, isIdempotenceEnabled:Boolean): Unit = {
    // Set consumer group acls since we are testing topic authorization
    setConsumerGroupAcls()

    // Verify produce/consume/describe throw TopicAuthorizationException
    val prop = new Properties()
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled.toString)
    val producer = createProducer(configOverrides = prop)

    assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer, numRecords, topic = tp.topic))
    val adminClient = createAdminClient()
    val e1 = assertThrows(classOf[ExecutionException], () => adminClient.describeTopics(Set(topic).asJava).allTopicNames().get())
    assertTrue(e1.getCause.isInstanceOf[TopicAuthorizationException], "Unexpected exception " + e1.getCause)

    // Verify successful produce/consume/describe on another topic using the same producer, consumer and adminClient
    val topic2 = "topic2"
    val tp2 = new TopicPartition(topic2, 0)

    setReadAndWriteAcls(tp2)
    // in idempotence producer, we need to create another producer because the previous one is in FATAL_ERROR state (due to authorization error)
    // If the transaction state in FATAL_ERROR, it'll never transit to other state. check TransactionManager#isTransitionValid for detail
    val producer2 = if (isIdempotenceEnabled)
      createProducer(configOverrides = prop)
    else
      producer

    sendRecords(producer2, numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeRecords(consumer, numRecords, topic = topic2)
    val describeResults = adminClient.describeTopics(Set(topic, topic2).asJava).topicNameValues()
    assertEquals(1, describeResults.get(topic2).get().partitions().size())

    val e2 = assertThrows(classOf[ExecutionException], () => adminClient.describeTopics(Set(topic).asJava).allTopicNames().get())
    assertTrue(e2.getCause.isInstanceOf[TopicAuthorizationException], "Unexpected exception " + e2.getCause)

    // Verify that consumer manually assigning both authorized and unauthorized topic doesn't consume
    // from the unauthorized topic and throw; since we can now return data during the time we are updating
    // metadata / fetching positions, it is possible that the authorized topic record is returned during this time.
    consumer.assign(List(tp, tp2).asJava)
    sendRecords(producer2, numRecords, tp2)
    var topic2RecordConsumed = false
    def verifyNoRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]): Boolean = {
      assertEquals(Collections.singleton(tp2), records.partitions(), "Consumed records with unexpected partitions: " + records)
      topic2RecordConsumed = true
      false
    }
    assertThrows(classOf[TopicAuthorizationException],
      () => TestUtils.pollRecordsUntilTrue(consumer, verifyNoRecords, "Consumer didn't fail with authorization exception within timeout"))

    // Add ACLs and verify successful produce/consume/describe on first topic
    setReadAndWriteAcls(tp)
    if (!topic2RecordConsumed) {
      consumeRecordsIgnoreOneAuthorizationException(consumer, numRecords, startingOffset = 1, topic2)
    }
    sendRecords(producer2, numRecords, tp)
    consumeRecordsIgnoreOneAuthorizationException(consumer, numRecords, startingOffset = 0, topic)
    val describeResults2 = adminClient.describeTopics(Set(topic, topic2).asJava).topicNameValues
    assertEquals(1, describeResults2.get(topic).get().partitions().size())
    assertEquals(1, describeResults2.get(topic2).get().partitions().size())
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "kraft, true",
    "kraft, false",
  ))
  def testNoProduceWithDescribeAcl(quorum:String, isIdempotenceEnabled:Boolean): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclTopicDescribe()).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicDescribeAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
    }

    val prop = new Properties()
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled.toString)
    val producer = createProducer(configOverrides = prop)

    if (isIdempotenceEnabled) {
      // in idempotent producer, it'll fail at InitProducerId request
      assertThrows(classOf[KafkaException], () => sendRecords(producer, numRecords, tp))
    } else {
      val e = assertThrows(classOf[TopicAuthorizationException], () => sendRecords(producer, numRecords, tp))
      assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    }
    confirmReauthenticationMetrics()
  }

   /**
    * Tests that a consumer fails to consume messages without the appropriate
    * ACL set.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNoConsumeWithoutDescribeAclViaAssign(quorum: String, groupProtocol: String): Unit = {
    noConsumeWithoutDescribeAclSetup()
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    // the exception is expected when the consumer attempts to lookup offsets
    assertThrows(classOf[KafkaException], () => consumeRecords(consumer))
    confirmReauthenticationMetrics()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNoConsumeWithoutDescribeAclViaSubscribe(quorum: String, groupProtocol: String): Unit = {
    noConsumeWithoutDescribeAclSetup()
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    // this should timeout since the consumer will not be able to fetch any metadata for the topic
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer, timeout = 3000))

    // Verify that no records are consumed even if one of the requested topics is authorized
    setReadAndWriteAcls(tp)
    consumer.subscribe(List(topic, "topic2").asJava)
    assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer, timeout = 3000))

    // Verify that records are consumed if all topics are authorized
    consumer.subscribe(List(topic).asJava)
    consumeRecordsIgnoreOneAuthorizationException(consumer)
  }

  private def noConsumeWithoutDescribeAclSetup(): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclTopicWrite(), AclTopicCreate(), AclTopicDescribe()).asJava).values
    superuserAdminClient.createAcls(List(AclGroupRead).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }

    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    superuserAdminClient.deleteAcls(List(AclTopicDescribe().toFilter).asJava).values
    superuserAdminClient.deleteAcls(List(AclTopicWrite().toFilter).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNoConsumeWithDescribeAclViaAssign(quorum: String, groupProtocol: String): Unit = {
    noConsumeWithDescribeAclSetup()
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    confirmReauthenticationMetrics()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNoConsumeWithDescribeAclViaSubscribe(quorum: String, groupProtocol: String): Unit = {
    noConsumeWithDescribeAclSetup()
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)

    val e = assertThrows(classOf[TopicAuthorizationException], () => consumeRecords(consumer))
    assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    confirmReauthenticationMetrics()
  }

  private def noConsumeWithDescribeAclSetup(): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclTopicWrite(), AclTopicCreate(), AclTopicDescribe()).asJava).values
    superuserAdminClient.createAcls(List(AclGroupRead).asJava).values

    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  /**
    * Tests that a consumer fails to consume messages without the appropriate
    * ACL set.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testNoGroupAcl(quorum: String, groupProtocol: String): Unit = {
    val superuserAdminClient = createSuperuserAdminClient()
    superuserAdminClient.createAcls(List(AclTopicWrite(), AclTopicCreate(), AclTopicDescribe()).asJava).values
    brokers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
    }
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    val e = assertThrows(classOf[GroupAuthorizationException], () => consumeRecords(consumer))
    assertEquals(group, e.groupId())
    confirmReauthenticationMetrics()
  }

  protected final def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                                  numRecords: Int, tp: TopicPartition): Unit = {
    val futures = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  protected final def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                                     numRecords: Int = 1,
                                     startingOffset: Int = 0,
                                     topic: String = topic,
                                     part: Int = part,
                                     timeout: Long = 10000): Unit = {
    val records = TestUtils.consumeRecords(consumer, numRecords, timeout)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic)
      assertEquals(part, record.partition)
      assertEquals(offset.toLong, record.offset)
    }
  }

  protected def createScramAdminClient(scramMechanism: String, user: String, password: String): Admin = {
    createAdminClient(bootstrapServers(), securityProtocol, trustStoreFile, clientSaslProperties,
      scramMechanism, user, password)
  }

  // Consume records, ignoring at most one TopicAuthorization exception from previously sent request
  private def consumeRecordsIgnoreOneAuthorizationException(consumer: Consumer[Array[Byte], Array[Byte]],
                                                            numRecords: Int = 1,
                                                            startingOffset: Int = 0,
                                                            topic: String = topic): Unit = {
    try {
      consumeRecords(consumer, numRecords, startingOffset, topic)
    } catch {
      case _: TopicAuthorizationException => consumeRecords(consumer, numRecords, startingOffset, topic)
    }
  }
}

