/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import java.util
import java.util.Properties
import kafka.admin.ConfigCommand.ConfigCommandOptions
import kafka.cluster.Broker
import kafka.server.{ConfigEntityName, ConfigType}
import kafka.utils.{Exit, Logging, TestUtils}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.Sanitizer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, times, verify, when}

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class ConfigCommandTest extends Logging {

  private val zkConnect = "localhost:2181"
  private val dummyAdminZkClient = new DummyAdminZkClient(null)

  private val zookeeperBootstrap = Array("--zookeeper", zkConnect)
  private val brokerBootstrap = Array("--bootstrap-server", "localhost:9092")
  private val controllerBootstrap = Array("--bootstrap-controller", "localhost:9093")

  @Test
  def shouldExitWithNonZeroStatusOnArgError(): Unit = {
    assertNonZeroStatusExit(Array("--blah"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnZkCommandWithTopicsEntity(): Unit = {
    assertNonZeroStatusExit(zookeeperBootstrap  ++ Array(
      "--entity-type", "topics",
      "--describe"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnZkCommandWithClientsEntity(): Unit = {
    assertNonZeroStatusExit(zookeeperBootstrap ++ Array(
      "--entity-type", "clients",
      "--describe"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnZkCommandWithIpsEntity(): Unit = {
    assertNonZeroStatusExit(zookeeperBootstrap ++ Array(
      "--entity-type", "ips",
      "--describe"))
  }

  @Test
  def shouldExitWithNonZeroStatusAlterUserQuotaWithoutEntityName(): Unit = {
    assertNonZeroStatusExit(brokerBootstrap ++ Array(
      "--entity-type", "users",
      "--alter", "--add-config", "consumer_byte_rate=20000"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnBrokerCommandError(): Unit = {
    assertNonZeroStatusExit(Array(
      "--bootstrap-server", "invalid host",
      "--entity-type", "brokers",
      "--entity-name", "1",
      "--describe"))
  }

  @Test
  def shouldExitWithNonZeroStatusIfBothBootstrapServerAndBootstrapControllerGiven(): Unit = {
    assertNonZeroStatusExit(brokerBootstrap ++ controllerBootstrap ++ Array(
      "--describe", "--broker-defaults" ))
  }

  @Test
  def shouldExitWithNonZeroStatusOnBrokerCommandWithZkTlsConfigFile(): Unit = {
    assertNonZeroStatusExit(Array(
      "--bootstrap-server", "invalid host",
      "--entity-type", "users",
      "--zk-tls-config-file", "zk_tls_config.properties",
      "--describe"))
  }

  private def assertNonZeroStatusExit(args: Array[String]): Unit = {
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }

    try {
      ConfigCommand.main(args)
    } catch {
      case _: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  @Test
  def shouldFailParseArgumentsForClientsEntityTypeUsingZookeeper(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => testArgumentParse(zookeeperBootstrap, "clients"))
  }

  @Test
  def shouldParseArgumentsForClientsEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "clients")
  }

  @Test
  def shouldParseArgumentsForClientsEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "clients")
  }

  @Test
  def shouldParseArgumentsForUsersEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse(zookeeperBootstrap, "users")
  }

  @Test
  def shouldParseArgumentsForUsersEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "users")
  }

  @Test
  def shouldParseArgumentsForUsersEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "users")
  }

  @Test
  def shouldFailParseArgumentsForTopicsEntityTypeUsingZookeeper(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => testArgumentParse(zookeeperBootstrap, "topics"))
  }

  @Test
  def shouldParseArgumentsForTopicsEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "topics")
  }

  @Test
  def shouldParseArgumentsForTopicsEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "topics")
  }

  @Test
  def shouldParseArgumentsForBrokersEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse(zookeeperBootstrap, "brokers")
  }

  @Test
  def shouldParseArgumentsForBrokersEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "brokers")
  }

  @Test
  def shouldParseArgumentsForBrokersEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "brokers")
  }

  @Test
  def shouldParseArgumentsForBrokerLoggersEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "broker-loggers")
  }

  @Test
  def shouldParseArgumentsForBrokerLoggersEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "broker-loggers")
  }

  @Test
  def shouldFailParseArgumentsForIpEntityTypeUsingZookeeper(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => testArgumentParse(zookeeperBootstrap, "ips"))
  }

  @Test
  def shouldParseArgumentsForIpEntityTypeWithBrokerBootstrap(): Unit = {
    testArgumentParse(brokerBootstrap, "ips")
  }

  @Test
  def shouldParseArgumentsForIpEntityTypeWithControllerBootstrap(): Unit = {
    testArgumentParse(controllerBootstrap, "ips")
  }

  def testArgumentParse(
    bootstrapArguments: Array[String],
    entityType: String
  ): Unit = {
    val shortFlag: String = s"--${entityType.dropRight(1)}"

    val connectOpts = (bootstrapArguments(0), bootstrapArguments(1))

    // Should parse correctly
    var createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--describe"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--describe"))
    createOpts.checkArgs()

    // For --alter and added config
    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=d"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--alter",
      "--add-config-file", "/tmp/new.properties"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config", "a=b,c=d"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config-file", "/tmp/new.properties"))
    createOpts.checkArgs()

    // For alter and deleted config
    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--alter",
      "--delete-config", "a,b,c"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--delete-config", "a,b,c"))
    createOpts.checkArgs()

    // For alter and both added, deleted config
    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=d",
      "--delete-config", "a"))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config", "a=b,c=d",
      "--delete-config", "a"))
    createOpts.checkArgs()

    val addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(2, addedProps.size())
    assertEquals("b", addedProps.getProperty("a"))
    assertEquals("d", addedProps.getProperty("c"))

    val deletedProps = ConfigCommand.parseConfigsToBeDeleted(createOpts)
    assertEquals(1, deletedProps.size)
    assertEquals("a", deletedProps.head)

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      "--entity-name", "1",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=,d=e,f="))
    createOpts.checkArgs()

    createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config", "a._-c=b,c=,d=e,f="))
    createOpts.checkArgs()

    val addedProps2 = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(4, addedProps2.size())
    assertEquals("b", addedProps2.getProperty("a._-c"))
    assertEquals("e", addedProps2.getProperty("d"))
    assertTrue(addedProps2.getProperty("c").isEmpty)
    assertTrue(addedProps2.getProperty("f").isEmpty)

    var inValidCreateOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config", "a;c=b"))

    assertThrows(classOf[IllegalArgumentException],
      () => ConfigCommand.parseConfigsToBeAdded(inValidCreateOpts))

    inValidCreateOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2,
      shortFlag, "1",
      "--alter",
      "--add-config", "a,=b"))

    assertThrows(classOf[IllegalArgumentException],
      () => ConfigCommand.parseConfigsToBeAdded(inValidCreateOpts))
  }

  @Test
  def shouldFailIfAddAndAddFile(): Unit = {
    // Should not parse correctly
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b,c=d",
      "--add-config-file", "/tmp/new.properties"
    ))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def testParseConfigsToBeAddedForAddConfigFile(): Unit = {
    val fileContents =
      """a=b
        |c = d
        |json = {"key": "val"}
        |nested = [[1, 2], [3, 4]]
        |""".stripMargin

    val file = TestUtils.tempFile(fileContents)

    val addConfigFileArgs = Array("--add-config-file", file.getPath)

    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter")
      ++ addConfigFileArgs)
    createOpts.checkArgs()

    val addedProps = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(4, addedProps.size())
    assertEquals("b", addedProps.getProperty("a"))
    assertEquals("d", addedProps.getProperty("c"))
    assertEquals("{\"key\": \"val\"}", addedProps.getProperty("json"))
    assertEquals("[[1, 2], [3, 4]]", addedProps.getProperty("nested"))
  }

  def doTestOptionEntityTypeNames(zkConfig: Boolean): Unit = {
    val connectOpts = if (zkConfig)
      ("--zookeeper", zkConnect)
    else
      ("--bootstrap-server", "localhost:9092")

    def testExpectedEntityTypeNames(expectedTypes: List[String], expectedNames: List[String], args: String*): Unit = {
      val createOpts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2, "--describe") ++ args)
      createOpts.checkArgs()
      assertEquals(createOpts.entityTypes, expectedTypes)
      assertEquals(createOpts.entityNames, expectedNames)
    }

    // zookeeper config only supports "users" and "brokers" entity type
    if (!zkConfig) {
      testExpectedEntityTypeNames(List(ConfigType.Topic), List("A"), "--entity-type", "topics", "--entity-name", "A")
      testExpectedEntityTypeNames(List(ConfigType.Ip), List("1.2.3.4"), "--entity-name", "1.2.3.4", "--entity-type", "ips")
      testExpectedEntityTypeNames(List(ConfigType.User, ConfigType.Client), List("A", ""),
        "--entity-type", "users", "--entity-type", "clients", "--entity-name", "A", "--entity-default")
      testExpectedEntityTypeNames(List(ConfigType.User, ConfigType.Client), List("", "B"),
        "--entity-default", "--entity-name", "B", "--entity-type", "users", "--entity-type", "clients")
      testExpectedEntityTypeNames(List(ConfigType.Topic), List("A"), "--topic", "A")
      testExpectedEntityTypeNames(List(ConfigType.Ip), List("1.2.3.4"), "--ip", "1.2.3.4")
      testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("B", "A"), "--client", "B", "--user", "A")
      testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("B", ""), "--client", "B", "--user-defaults")
      testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("A"),
        "--entity-type", "clients", "--entity-type", "users", "--entity-name", "A")
      testExpectedEntityTypeNames(List(ConfigType.Topic), List.empty, "--entity-type", "topics")
      testExpectedEntityTypeNames(List(ConfigType.Ip), List.empty, "--entity-type", "ips")
    }

    testExpectedEntityTypeNames(List(ConfigType.Broker), List("0"), "--entity-name", "0", "--entity-type", "brokers")
    testExpectedEntityTypeNames(List(ConfigType.Broker), List("0"), "--broker", "0")
    testExpectedEntityTypeNames(List(ConfigType.User), List.empty, "--entity-type", "users")
    testExpectedEntityTypeNames(List(ConfigType.Broker), List.empty, "--entity-type", "brokers")
  }

  @Test
  def testOptionEntityTypeNamesUsingZookeeper(): Unit = {
    doTestOptionEntityTypeNames(zkConfig = true)
  }

  @Test
  def testOptionEntityTypeNames(): Unit = {
    doTestOptionEntityTypeNames(zkConfig = false)
  }

  @Test
  def shouldFailIfUnrecognisedEntityTypeUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldFailIfUnrecognisedEntityType(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldFailIfBrokerEntityTypeIsNotAnIntegerUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "A", "--entity-type", "brokers", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldFailIfBrokerEntityTypeIsNotAnInteger(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "A", "--entity-type", "brokers", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldFailIfShortBrokerEntityTypeIsNotAnIntegerUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--broker", "A", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldFailIfShortBrokerEntityTypeIsNotAnInteger(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--broker", "A", "--alter", "--add-config", "a=b,c=d"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldFailIfMixedEntityTypeFlagsUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "A", "--entity-type", "users", "--client", "B", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldFailIfMixedEntityTypeFlags(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "A", "--entity-type", "users", "--client", "B", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldFailIfInvalidHost(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "A,B", "--entity-type", "ips", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldFailIfInvalidHostUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "A,B", "--entity-type", "ips", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldFailIfUnresolvableHost(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "RFC2606.invalid", "--entity-type", "ips", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldFailIfUnresolvableHostUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "RFC2606.invalid", "--entity-type", "ips", "--describe"))
    assertThrows(classOf[IllegalArgumentException], () => createOpts.checkArgs())
  }

  @Test
  def shouldAddClientConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-client-id",
      "--entity-type", "clients",
      "--alter",
      "--add-config", "a=b,c=d"))

    val zkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new Properties())

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeClientIdConfig(clientId: String, configChange: Properties): Unit = {
        assertEquals("my-client-id", clientId)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
  }

  @Test
  def shouldAddIpConfigsUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1.2.3.4",
      "--entity-type", "ips",
      "--alter",
      "--add-config", "a=b,c=d"))

    val zkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new Properties())

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeIpConfig(ip: String, configChange: Properties): Unit = {
        assertEquals("1.2.3.4", ip)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
  }

  private def toValues(entityName: Option[String], entityType: String): (Array[String], Map[String, String]) = {
    val command = entityType match {
      case ClientQuotaEntity.USER => "users"
      case ClientQuotaEntity.CLIENT_ID => "clients"
      case ClientQuotaEntity.IP => "ips"
    }
    entityName match {
      case Some(null) =>
        (Array("--entity-type", command, "--entity-default"), Map(entityType -> null))
      case Some(name) =>
        (Array("--entity-type", command, "--entity-name", name), Map(entityType -> name))
      case None => (Array.empty, Map.empty)
    }
  }

  private def verifyAlterCommandFails(expectedErrorMessage: String, alterOpts: Seq[String]): Unit = {
    val mockAdminClient: Admin = mock(classOf[Admin])
    val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--alter") ++ alterOpts)
    val e = assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(mockAdminClient, opts))
    assertTrue(e.getMessage.contains(expectedErrorMessage), s"Unexpected exception: $e")
  }

  @Test
  def shouldNotAlterNonQuotaIpConfigsUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter anything that is not a connection quota
    // for ip entities
    val ipEntityOpts = List("--entity-type", "ips", "--entity-name", "127.0.0.1")
    val invalidProp = "some_config"
    verifyAlterCommandFails(invalidProp, ipEntityOpts ++ List("--add-config", "connection_creation_rate=10000,some_config=10"))
    verifyAlterCommandFails(invalidProp, ipEntityOpts ++ List("--add-config", "some_config=10"))
    verifyAlterCommandFails(invalidProp, ipEntityOpts ++ List("--delete-config", "connection_creation_rate=10000,some_config=10"))
    verifyAlterCommandFails(invalidProp, ipEntityOpts ++ List("--delete-config", "some_config=10"))
  }

  private def verifyDescribeQuotas(describeArgs: List[String], expectedFilter: ClientQuotaFilter): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--describe") ++ describeArgs)
    val describeFuture = new KafkaFutureImpl[util.Map[ClientQuotaEntity, util.Map[String, java.lang.Double]]]
    describeFuture.complete(Map.empty[ClientQuotaEntity, util.Map[String, java.lang.Double]].asJava)
    val describeResult: DescribeClientQuotasResult = mock(classOf[DescribeClientQuotasResult])
    when(describeResult.entities()).thenReturn(describeFuture)

    var describedConfigs = false
    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions): DescribeClientQuotasResult = {
        assertTrue(filter.strict)
        assertEquals(expectedFilter.components().asScala.toSet, filter.components.asScala.toSet)
        describedConfigs = true
        describeResult
      }
    }
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    assertTrue(describedConfigs)
  }

  @Test
  def testDescribeIpConfigs(): Unit = {
    val entityType = ClientQuotaEntity.IP
    val knownHost = "1.2.3.4"
    val defaultIpFilter = ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofDefaultEntity(entityType)).asJava)
    val singleIpFilter = ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntity(entityType, knownHost)).asJava)
    val allIpsFilter = ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntityType(entityType)).asJava)
    verifyDescribeQuotas(List("--entity-default", "--entity-type", "ips"), defaultIpFilter)
    verifyDescribeQuotas(List("--ip-defaults"), defaultIpFilter)
    verifyDescribeQuotas(List("--entity-type", "ips", "--entity-name", knownHost), singleIpFilter)
    verifyDescribeQuotas(List("--ip", knownHost), singleIpFilter)
    verifyDescribeQuotas(List("--entity-type", "ips"), allIpsFilter)
  }

  def verifyAlterQuotas(alterOpts: Seq[String], expectedAlterEntity: ClientQuotaEntity,
                        expectedProps: Map[String, java.lang.Double], expectedAlterOps: Set[ClientQuotaAlteration.Op]): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--alter") ++ alterOpts)

    var describedConfigs = false
    val describeFuture = new KafkaFutureImpl[util.Map[ClientQuotaEntity, util.Map[String, java.lang.Double]]]
    describeFuture.complete(Map(expectedAlterEntity -> expectedProps.asJava).asJava)
    val describeResult: DescribeClientQuotasResult = mock(classOf[DescribeClientQuotasResult])
    when(describeResult.entities()).thenReturn(describeFuture)

    val expectedFilterComponents = expectedAlterEntity.entries.asScala.map { case (entityType, entityName) =>
      if (entityName == null)
        ClientQuotaFilterComponent.ofDefaultEntity(entityType)
      else
        ClientQuotaFilterComponent.ofEntity(entityType, entityName)
    }.toSet

    var alteredConfigs = false
    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterClientQuotasResult = mock(classOf[AlterClientQuotasResult])
    when(alterResult.all()).thenReturn(alterFuture)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions): DescribeClientQuotasResult = {
        assertTrue(filter.strict)
        assertEquals(expectedFilterComponents, filter.components().asScala.toSet)
        describedConfigs = true
        describeResult
      }

      override def alterClientQuotas(entries: util.Collection[ClientQuotaAlteration], options: AlterClientQuotasOptions): AlterClientQuotasResult = {
        assertFalse(options.validateOnly)
        assertEquals(1, entries.size)
        val alteration = entries.asScala.head
        assertEquals(expectedAlterEntity, alteration.entity)
        val ops = alteration.ops.asScala
        assertEquals(expectedAlterOps, ops.toSet)
        alteredConfigs = true
        alterResult
      }
    }
    ConfigCommand.alterConfig(mockAdminClient, createOpts)
    assertTrue(describedConfigs)
    assertTrue(alteredConfigs)
  }

  @Test
  def testAlterIpConfig(): Unit = {
    val (singleIpArgs, singleIpEntry) = toValues(Some("1.2.3.4"), ClientQuotaEntity.IP)
    val singleIpEntity = new ClientQuotaEntity(singleIpEntry.asJava)
    val (defaultIpArgs, defaultIpEntry) = toValues(Some(null), ClientQuotaEntity.IP)
    val defaultIpEntity = new ClientQuotaEntity(defaultIpEntry.asJava)

    val deleteArgs = List("--delete-config", "connection_creation_rate")
    val deleteAlterationOps = Set(new ClientQuotaAlteration.Op("connection_creation_rate", null))
    val propsToDelete = Map("connection_creation_rate" -> Double.box(50.0))

    val addArgs = List("--add-config", "connection_creation_rate=100")
    val addAlterationOps = Set(new ClientQuotaAlteration.Op("connection_creation_rate", 100.0))

    verifyAlterQuotas(singleIpArgs ++ deleteArgs, singleIpEntity, propsToDelete, deleteAlterationOps)
    verifyAlterQuotas(singleIpArgs ++ addArgs, singleIpEntity, Map.empty, addAlterationOps)
    verifyAlterQuotas(defaultIpArgs ++ deleteArgs, defaultIpEntity, propsToDelete, deleteAlterationOps)
    verifyAlterQuotas(defaultIpArgs ++ addArgs, defaultIpEntity, Map.empty, addAlterationOps)
  }

  @Test
  def shouldAddClientConfig(): Unit = {
    val alterArgs = List("--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000",
      "--delete-config", "request_percentage")
    val propsToDelete = Map("request_percentage" -> Double.box(50.0))

    val alterationOps = Set(
      new ClientQuotaAlteration.Op("consumer_byte_rate", Double.box(20000)),
      new ClientQuotaAlteration.Op("producer_byte_rate", Double.box(10000)),
      new ClientQuotaAlteration.Op("request_percentage", null)
    )

    def verifyAlterUserClientQuotas(userOpt: Option[String], clientOpt: Option[String]): Unit = {
      val (userArgs, userEntry) = toValues(userOpt, ClientQuotaEntity.USER)
      val (clientArgs, clientEntry) = toValues(clientOpt, ClientQuotaEntity.CLIENT_ID)

      val commandArgs = alterArgs ++ userArgs ++ clientArgs
      val clientQuotaEntity = new ClientQuotaEntity((userEntry ++ clientEntry).asJava)
      verifyAlterQuotas(commandArgs, clientQuotaEntity, propsToDelete, alterationOps)
    }
    verifyAlterUserClientQuotas(Some("test-user-1"), Some("test-client-1"))
    verifyAlterUserClientQuotas(Some("test-user-2"), Some(null))
    verifyAlterUserClientQuotas(Some("test-user-3"), None)
    verifyAlterUserClientQuotas(Some(null), Some("test-client-2"))
    verifyAlterUserClientQuotas(Some(null), Some(null))
    verifyAlterUserClientQuotas(Some(null), None)
    verifyAlterUserClientQuotas(None, Some("test-client-3"))
    verifyAlterUserClientQuotas(None, Some(null))
  }

  private val userEntityOpts = List("--entity-type", "users", "--entity-name", "admin")
  private val clientEntityOpts = List("--entity-type", "clients", "--entity-name", "admin")
  private val addScramOpts = List("--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
  private val deleteScramOpts = List("--delete-config", "SCRAM-SHA-256")

  @Test
  def shouldNotAlterNonQuotaNonScramUserOrClientConfigUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter anything that is not a quota and not a SCRAM credential
    // for both user and client entities
    val invalidProp = "some_config"
    verifyAlterCommandFails(invalidProp, userEntityOpts ++
      List("-add-config", "consumer_byte_rate=20000,producer_byte_rate=10000,some_config=10"))
    verifyAlterCommandFails(invalidProp, userEntityOpts ++
      List("--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000,some_config=10"))
    verifyAlterCommandFails(invalidProp, clientEntityOpts ++ List("--add-config", "some_config=10"))
    verifyAlterCommandFails(invalidProp, userEntityOpts ++ List("--delete-config", "consumer_byte_rate,some_config"))
    verifyAlterCommandFails(invalidProp, userEntityOpts ++ List("--delete-config", "SCRAM-SHA-256,some_config"))
    verifyAlterCommandFails(invalidProp, clientEntityOpts ++ List("--delete-config", "some_config"))
  }

  @Test
  def shouldNotAlterScramClientConfigUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter SCRAM credentials for client entities
    verifyAlterCommandFails("SCRAM-SHA-256", clientEntityOpts ++ addScramOpts)
    verifyAlterCommandFails("SCRAM-SHA-256", clientEntityOpts ++ deleteScramOpts)
  }

  @Test
  def shouldNotCreateUserScramCredentialConfigWithUnderMinimumIterationsUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to create a SCRAM credential for a user
    // with an iterations value less than the minimum
    verifyAlterCommandFails("SCRAM-SHA-256", userEntityOpts ++ List("--add-config", "SCRAM-SHA-256=[iterations=100,password=foo-secret]"))
  }

  @Test
  def shouldNotAlterUserScramCredentialAndClientQuotaConfigsSimultaneouslyUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter both SCRAM credentials and quotas for user entities
    val expectedErrorMessage = "SCRAM-SHA-256"
    val secondUserEntityOpts = List("--entity-type", "users", "--entity-name", "admin1")
    val addQuotaOpts = List("--add-config", "consumer_byte_rate=20000")
    val deleteQuotaOpts = List("--delete-config", "consumer_byte_rate")

    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ addScramOpts ++ userEntityOpts ++ deleteQuotaOpts)
    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ addScramOpts ++ secondUserEntityOpts ++ deleteQuotaOpts)
    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ deleteScramOpts ++ userEntityOpts ++ addQuotaOpts)
    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ deleteScramOpts ++ secondUserEntityOpts ++ addQuotaOpts)

    // change order of quota/SCRAM commands, verify alter still fails
    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ deleteQuotaOpts ++ userEntityOpts ++ addScramOpts)
    verifyAlterCommandFails(expectedErrorMessage, secondUserEntityOpts ++ deleteQuotaOpts ++ userEntityOpts ++ addScramOpts)
    verifyAlterCommandFails(expectedErrorMessage, userEntityOpts ++ addQuotaOpts ++ userEntityOpts ++ deleteScramOpts)
    verifyAlterCommandFails(expectedErrorMessage, secondUserEntityOpts ++ addQuotaOpts ++ userEntityOpts ++ deleteScramOpts)
  }

  @Test
  def shouldNotDescribeUserScramCredentialsWithEntityDefaultUsingBootstrapServer(): Unit = {
    def verifyUserScramCredentialsNotDescribed(requestOpts: List[String]): Unit = {
      // User SCRAM credentials should not be described when specifying
      // --describe --entity-type users --entity-default (or --user-defaults) with --bootstrap-server
      val describeFuture = new KafkaFutureImpl[util.Map[ClientQuotaEntity, util.Map[String, java.lang.Double]]]
      describeFuture.complete(Map(new ClientQuotaEntity(Map("" -> "").asJava) -> Map("request_percentage" -> Double.box(50.0)).asJava).asJava)
      val describeClientQuotasResult: DescribeClientQuotasResult = mock(classOf[DescribeClientQuotasResult])
      when(describeClientQuotasResult.entities()).thenReturn(describeFuture)
      val node = new Node(1, "localhost", 9092)
      val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
        override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions):  DescribeClientQuotasResult = {
          describeClientQuotasResult
        }
        override def describeUserScramCredentials(users: util.List[String], options: DescribeUserScramCredentialsOptions): DescribeUserScramCredentialsResult = {
          throw new IllegalStateException("Incorrectly described SCRAM credentials when specifying --entity-default with --bootstrap-server")
        }
      }
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092", "--describe") ++ requestOpts)
      ConfigCommand.describeConfig(mockAdminClient, opts) // fails if describeUserScramCredentials() is invoked
    }

    val expectedMsg = "The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server."
    val defaultUserOpt = List("--user-defaults")
    val verboseDefaultUserOpts = List("--entity-type", "users", "--entity-default")
    verifyAlterCommandFails(expectedMsg, verboseDefaultUserOpts ++ addScramOpts)
    verifyAlterCommandFails(expectedMsg, verboseDefaultUserOpts ++ deleteScramOpts)
    verifyUserScramCredentialsNotDescribed(verboseDefaultUserOpts)
    verifyAlterCommandFails(expectedMsg, defaultUserOpt ++ addScramOpts)
    verifyAlterCommandFails(expectedMsg, defaultUserOpt ++ deleteScramOpts)
    verifyUserScramCredentialsNotDescribed(defaultUserOpt)
  }

  @Test
  def shouldAddTopicConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=d"))

    val zkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new Properties())

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals("my-topic", topic)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
  }

  @Test
  def shouldAlterTopicConfig(): Unit = {
    doShouldAlterTopicConfig(false)
  }

  @Test
  def shouldAlterTopicConfigFile(): Unit = {
    doShouldAlterTopicConfig(true)
  }

  def doShouldAlterTopicConfig(file: Boolean): Unit = {
    var filePath = ""
    val addedConfigs = Map("delete.retention.ms" -> "1000000", "min.insync.replicas" -> "2")
    if (file) {
      val file = TestUtils.tempPropertiesFile(addedConfigs)
      filePath = file.getPath
    }

    val resourceName = "my-topic"
    val alterOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", resourceName,
      "--entity-type", "topics",
      "--alter",
      if (file) "--add-config-file" else "--add-config",
      if (file) filePath else addedConfigs.map{case (k, v) => k + "=" + v}.mkString(","),
      "--delete-config", "unclean.leader.election.enable"))
    var alteredConfigs = false

    def newConfigEntry(name: String, value: String): ConfigEntry =
      ConfigTest.newConfigEntry(name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, List.empty[ConfigEntry.ConfigSynonym].asJava)

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    val configEntries = List(newConfigEntry("min.insync.replicas", "1"), newConfigEntry("unclean.leader.election.enable", "1")).asJava
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(configEntries)))
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = mock(classOf[AlterConfigsResult])
    when(alterResult.all()).thenReturn(alterFuture)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily")
        assertEquals(1, resources.size)
        val resource = resources.iterator.next
        assertEquals(resource.`type`, ConfigResource.Type.TOPIC)
        assertEquals(resource.name, resourceName)
        describeResult
      }

      override def incrementalAlterConfigs(configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]], options: AlterConfigsOptions): AlterConfigsResult = {
        assertEquals(1, configs.size)
        val entry = configs.entrySet.iterator.next
        val resource = entry.getKey
        val alterConfigOps = entry.getValue
        assertEquals(ConfigResource.Type.TOPIC, resource.`type`)
        assertEquals(3, alterConfigOps.size)

        val expectedConfigOps = Set(
          new AlterConfigOp(newConfigEntry("delete.retention.ms", "1000000"), AlterConfigOp.OpType.SET),
          new AlterConfigOp(newConfigEntry("min.insync.replicas", "2"), AlterConfigOp.OpType.SET),
          new AlterConfigOp(newConfigEntry("unclean.leader.election.enable", ""), AlterConfigOp.OpType.DELETE)
        )
        assertEquals(expectedConfigOps.size, alterConfigOps.size)
        expectedConfigOps.foreach { expectedOp =>
          val actual = alterConfigOps.asScala.find(_.configEntry.name == expectedOp.configEntry.name)
          assertNotEquals(actual, None)
          assertEquals(expectedOp.opType, actual.get.opType)
          assertEquals(expectedOp.configEntry.name, actual.get.configEntry.name)
          assertEquals(expectedOp.configEntry.value, actual.get.configEntry.value)
        }
        alteredConfigs = true
        alterResult
      }
    }
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertTrue(alteredConfigs)
    verify(describeResult).all()
  }

  @Test
  def shouldDescribeConfigSynonyms(): Unit = {
    val resourceName = "my-topic"
    val describeOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", resourceName,
      "--entity-type", "topics",
      "--describe",
      "--all"))

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(util.Collections.emptyList[ConfigEntry])))
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertTrue(options.includeSynonyms(), "Synonyms not requested")
        assertEquals(Set(resource), resources.asScala.toSet)
        describeResult
      }
    }
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    verify(describeResult).all()
  }

  @Test
  def shouldNotAllowAddBrokerQuotaConfigWhileBrokerUpUsingZookeeper(): Unit = {
    val alterOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10,follower.replication.throttled.rate=20"))

    val mockZkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    val mockBroker: Broker = mock(classOf[Broker])
    when(mockZkClient.getBroker(1)).thenReturn(Option(mockBroker))

    assertThrows(classOf[IllegalArgumentException],
      () => ConfigCommand.alterConfigWithZk(mockZkClient, alterOpts, dummyAdminZkClient))
  }

  @Test
  def shouldNotAllowDescribeBrokerWhileBrokerUpUsingZookeeper(): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--describe"))

    val mockZkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    val mockBroker: Broker = mock(classOf[Broker])
    when(mockZkClient.getBroker(1)).thenReturn(Option(mockBroker))

    assertThrows(classOf[IllegalArgumentException],
      () => ConfigCommand.describeConfigWithZk(mockZkClient, describeOpts, dummyAdminZkClient))
  }

  @Test
  def shouldSupportDescribeBrokerBeforeBrokerUpUsingZookeeper(): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--describe"))

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def fetchEntityConfig(rootEntityType: String, sanitizedEntityName: String): Properties = {
        assertEquals("brokers", rootEntityType)
        assertEquals("1", sanitizedEntityName)

        new Properties()
      }
    }

    val mockZkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    when(mockZkClient.getBroker(1)).thenReturn(None)

    ConfigCommand.describeConfigWithZk(mockZkClient, describeOpts, new TestAdminZkClient(null))
  }

  @Test
  def shouldAddBrokerLoggerConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterBrokerLoggerConfig(node, "1", "1", List(
      new ConfigEntry("kafka.log.LogCleaner", "INFO"),
      new ConfigEntry("kafka.server.ReplicaManager", "INFO"),
      new ConfigEntry("kafka.server.KafkaApi", "INFO")
    ))
  }

  @Test
  def testNoSpecifiedEntityOptionWithDescribeBrokersInZKIsAllowed(): Unit = {
    val optsList = List("--zookeeper", zkConnect,
      "--entity-type", ConfigType.Broker,
      "--describe"
    )

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test
  def testNoSpecifiedEntityOptionWithDescribeBrokersInBootstrapServerIsAllowed(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigType.Broker,
      "--describe"
    )

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test
  def testDescribeAllBrokerConfig(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigType.Broker,
      "--entity-name", "1",
      "--describe",
      "--all")

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test
  def testDescribeAllTopicConfig(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigType.Topic,
      "--entity-name", "foo",
      "--describe",
      "--all")

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test
  def testDescribeAllBrokerConfigBootstrapServerRequired(): Unit = {
    val optsList = List("--zookeeper", zkConnect,
      "--entity-type", ConfigType.Broker,
      "--entity-name", "1",
      "--describe",
      "--all")

    assertThrows(classOf[IllegalArgumentException], () => new ConfigCommandOptions(optsList.toArray).checkArgs())
  }

  @Test
  def testEntityDefaultOptionWithDescribeBrokerLoggerIsNotAllowed(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigCommand.BrokerLoggerConfigType,
      "--entity-default",
      "--describe"
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConfigCommandOptions(optsList.toArray).checkArgs())
  }

  @Test
  def testEntityDefaultOptionWithAlterBrokerLoggerIsNotAllowed(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigCommand.BrokerLoggerConfigType,
      "--entity-default",
      "--alter",
      "--add-config", "kafka.log.LogCleaner=DEBUG"
    )

    assertThrows(classOf[IllegalArgumentException], () => new ConfigCommandOptions(optsList.toArray).checkArgs())
  }

  @Test
  def shouldRaiseInvalidConfigurationExceptionWhenAddingInvalidBrokerLoggerConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    // verifyAlterBrokerLoggerConfig tries to alter kafka.log.LogCleaner, kafka.server.ReplicaManager and kafka.server.KafkaApi
    // yet, we make it so DescribeConfigs returns only one logger, implying that kafka.server.ReplicaManager and kafka.log.LogCleaner are invalid
    assertThrows(classOf[InvalidConfigurationException], () => verifyAlterBrokerLoggerConfig(node, "1", "1", List(
      new ConfigEntry("kafka.server.KafkaApi", "INFO")
    )))
  }

  @Test
  def shouldAddDefaultBrokerDynamicConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterBrokerConfig(node, "", List("--entity-default"))
  }

  @Test
  def shouldAddBrokerDynamicConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterBrokerConfig(node, "1", List("--entity-name", "1"))
  }

  def verifyAlterBrokerConfig(node: Node, resourceName: String, resourceOpts: List[String]): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "message.max.bytes=10,leader.replication.throttled.rate=10") ++ resourceOpts
    val alterOpts = new ConfigCommandOptions(optsList.toArray)
    val brokerConfigs = mutable.Map[String, String]("num.io.threads" -> "5")

    val resource = new ConfigResource(ConfigResource.Type.BROKER, resourceName)
    val configEntries = util.Collections.singletonList(new ConfigEntry("num.io.threads", "5"))
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(configEntries)))
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = mock(classOf[AlterConfigsResult])
    when(alterResult.all()).thenReturn(alterFuture)

    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertFalse(options.includeSynonyms(), "Config synonyms requested unnecessarily")
        assertEquals(1, resources.size)
        val resource = resources.iterator.next
        assertEquals(ConfigResource.Type.BROKER, resource.`type`)
        assertEquals(resourceName, resource.name)
        describeResult
      }

      override def alterConfigs(configs: util.Map[ConfigResource, Config], options: AlterConfigsOptions): AlterConfigsResult = {
        assertEquals(1, configs.size)
        val entry = configs.entrySet.iterator.next
        val resource = entry.getKey
        val config = entry.getValue
        assertEquals(ConfigResource.Type.BROKER, resource.`type`)
        config.entries.forEach { e => brokerConfigs.put(e.name, e.value) }
        alterResult
      }
    }
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertEquals(Map("message.max.bytes" -> "10", "num.io.threads" -> "5", "leader.replication.throttled.rate" -> "10"),
      brokerConfigs.toMap)
    verify(describeResult).all()
  }

  @Test
  def shouldDescribeConfigBrokerWithoutEntityName(): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-type", "brokers",
      "--describe"))

    val BrokerDefaultEntityName = ""
    val resourceCustom = new ConfigResource(ConfigResource.Type.BROKER, "1")
    val resourceDefault = new ConfigResource(ConfigResource.Type.BROKER, BrokerDefaultEntityName)
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    val emptyConfig = new Config(util.Collections.emptyList[ConfigEntry])
    val resultMap = Map(resourceCustom -> emptyConfig, resourceDefault -> emptyConfig).asJava
    future.complete(resultMap)
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    // make sure it will be called 2 times: (1) for broker "1" (2) for default broker ""
    when(describeResult.all()).thenReturn(future)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertTrue(options.includeSynonyms(), "Synonyms not requested")
        val resource = resources.iterator.next
        assertEquals(ConfigResource.Type.BROKER, resource.`type`)
        assertTrue(resourceCustom.name == resource.name || resourceDefault.name == resource.name)
        assertEquals(1, resources.size)
        describeResult
      }
    }
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    verify(describeResult, times(2)).all()
  }

  private def verifyAlterBrokerLoggerConfig(node: Node, resourceName: String, entityName: String,
                                    describeConfigEntries: List[ConfigEntry]): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigCommand.BrokerLoggerConfigType,
      "--alter",
      "--entity-name", entityName,
      "--add-config", "kafka.log.LogCleaner=DEBUG",
      "--delete-config", "kafka.server.ReplicaManager,kafka.server.KafkaApi")
    val alterOpts = new ConfigCommandOptions(optsList.toArray)
    var alteredConfigs = false

    val resource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, resourceName)
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(describeConfigEntries.asJava)))
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = mock(classOf[AlterConfigsResult])
    when(alterResult.all()).thenReturn(alterFuture)

    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertEquals(1, resources.size)
        val resource = resources.iterator.next
        assertEquals(ConfigResource.Type.BROKER_LOGGER, resource.`type`)
        assertEquals(resourceName, resource.name)
        describeResult
      }

      override def incrementalAlterConfigs(configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]], options: AlterConfigsOptions): AlterConfigsResult = {
        assertEquals(1, configs.size)
        val entry = configs.entrySet.iterator.next
        val resource = entry.getKey
        val alterConfigOps = entry.getValue
        assertEquals(ConfigResource.Type.BROKER_LOGGER, resource.`type`)
        assertEquals(3, alterConfigOps.size)

        val expectedConfigOps = List(
          new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", "DEBUG"), AlterConfigOp.OpType.SET),
          new AlterConfigOp(new ConfigEntry("kafka.server.ReplicaManager", ""), AlterConfigOp.OpType.DELETE),
          new AlterConfigOp(new ConfigEntry("kafka.server.KafkaApi", ""), AlterConfigOp.OpType.DELETE)
        )
        assertEquals(expectedConfigOps, alterConfigOps.asScala.toList)
        alteredConfigs = true
        alterResult
      }
    }
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertTrue(alteredConfigs)
    verify(describeResult).all()
  }

  @Test
  def shouldSupportCommaSeparatedValuesUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=[d,e ,f],g=[h,i]"))

    val zkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new Properties())

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals("my-topic", topic)
        assertEquals("b", configChange.get("a"))
        assertEquals("d,e ,f", configChange.get("c"))
        assertEquals("h,i", configChange.get("g"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedEntityNameUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedEntityName(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=="))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=="))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts))
  }

  @Test
  def shouldNotUpdateConfigIfNonExistingConfigIsDeletedUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--delete-config", "missing_config1, missing_config2"))
    assertThrows(classOf[InvalidConfigurationException], () => ConfigCommand.alterConfigWithZk(null, createOpts, dummyAdminZkClient))
  }

  @Test
  def shouldNotUpdateConfigIfNonExistingConfigIsDeleted(): Unit = {
    val resourceName = "my-topic"
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", resourceName,
      "--entity-type", "topics",
      "--alter",
      "--delete-config", "missing_config1, missing_config2"))

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    val configEntries = List.empty[ConfigEntry].asJava
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(configEntries)))
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertEquals(1, resources.size)
        val resource = resources.iterator.next
        assertEquals(resource.`type`, ConfigResource.Type.TOPIC)
        assertEquals(resource.name, resourceName)
        describeResult
      }
    }

    assertThrows(classOf[InvalidConfigurationException], () => ConfigCommand.alterConfig(mockAdminClient, createOpts))
    verify(describeResult).all()
  }

  @Test
  def shouldNotDeleteBrokerConfigWhileBrokerUpUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--delete-config", "a,c"))

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {
        val properties: Properties = new Properties
        properties.put("a", "b")
        properties.put("c", "d")
        properties.put("e", "f")
        properties
      }

      override def changeBrokerConfig(brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals("f", configChange.get("e"))
        assertEquals(1, configChange.size())
      }
    }

    val mockZkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    val mockBroker: Broker = mock(classOf[Broker])
    when(mockZkClient.getBroker(1)).thenReturn(Option(mockBroker))

    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(mockZkClient, createOpts, new TestAdminZkClient(null)))
  }

  @Test
  def testScramCredentials(): Unit = {
    def createOpts(user: String, config: String): ConfigCommandOptions = {
      new ConfigCommandOptions(Array("--zookeeper", zkConnect,
        "--entity-name", user,
        "--entity-type", "users",
        "--alter",
        "--add-config", config))
    }

    def deleteOpts(user: String, mechanism: String) = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
        "--entity-name", user,
        "--entity-type", "users",
        "--alter",
        "--delete-config", mechanism))

    val credentials = mutable.Map[String, Properties]()
    case class CredentialChange(user: String, mechanisms: Set[String], iterations: Int) extends AdminZkClient(null) {
      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {
        credentials.getOrElse(entityName, new Properties())
      }
      override def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configChange: Properties, isUserClientId: Boolean = false): Unit = {
        assertEquals(user, sanitizedEntityName)
        assertEquals(mechanisms, configChange.keySet().asScala)
        for (mechanism <- mechanisms) {
          val value = configChange.getProperty(mechanism)
          assertEquals(-1, value.indexOf("password="))
          val scramCredential = ScramCredentialUtils.credentialFromString(value)
          assertEquals(iterations, scramCredential.iterations)
          if (configChange != null)
              credentials.put(user, configChange)
        }
      }
    }
    val optsA = createOpts("userA", "SCRAM-SHA-256=[iterations=8192,password=abc, def]")
    ConfigCommand.alterConfigWithZk(null, optsA, CredentialChange("userA", Set("SCRAM-SHA-256"), 8192))
    val optsB = createOpts("userB", "SCRAM-SHA-256=[iterations=4096,password=abc, def],SCRAM-SHA-512=[password=1234=abc]")
    ConfigCommand.alterConfigWithZk(null, optsB, CredentialChange("userB", Set("SCRAM-SHA-256", "SCRAM-SHA-512"), 4096))

    val del256 = deleteOpts("userB", "SCRAM-SHA-256")
    ConfigCommand.alterConfigWithZk(null, del256, CredentialChange("userB", Set("SCRAM-SHA-512"), 4096))
    val del512 = deleteOpts("userB", "SCRAM-SHA-512")
    ConfigCommand.alterConfigWithZk(null, del512, CredentialChange("userB", Set(), 4096))
  }

  @Test
  def testQuotaConfigEntityUsingZookeeperNotAllowed(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => doTestQuotaConfigEntity(zkConfig = true))
  }

  def doTestQuotaConfigEntity(zkConfig: Boolean): Unit = {
    val connectOpts = if (zkConfig)
      ("--zookeeper", zkConnect)
    else
      ("--bootstrap-server", "localhost:9092")

    def createOpts(entityType: String, entityName: Option[String], otherArgs: Array[String]) : ConfigCommandOptions = {
      val optArray = Array(connectOpts._1, connectOpts._2, "--entity-type", entityType)
      val nameArray = entityName match {
        case Some(name) => Array("--entity-name", name)
        case None => Array[String]()
      }
      new ConfigCommandOptions(optArray ++ nameArray ++ otherArgs)
    }

    def checkEntity(entityType: String, entityName: Option[String], expectedEntityName: String, otherArgs: Array[String]): Unit = {
      val opts = createOpts(entityType, entityName, otherArgs)
      opts.checkArgs()
      val entity = ConfigCommand.parseEntity(opts)
      assertEquals(entityType, entity.root.entityType)
      assertEquals(expectedEntityName, entity.fullSanitizedName)
    }

    def checkInvalidArgs(entityType: String, entityName: Option[String], otherArgs: Array[String]): Unit = {
      val opts = createOpts(entityType, entityName, otherArgs)
      assertThrows(classOf[IllegalArgumentException], () => opts.checkArgs())
    }

    def checkInvalidEntity(entityType: String, entityName: Option[String], otherArgs: Array[String]): Unit = {
      val opts = createOpts(entityType, entityName, otherArgs)
      opts.checkArgs()
      assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.parseEntity(opts))
    }

    val describeOpts = Array("--describe")
    val alterOpts = Array("--alter", "--add-config", "a=b,c=d")

    // <client-id> quota
    val clientId = "client-1"
    for (opts <- Seq(describeOpts, alterOpts)) {
      checkEntity("clients", Some(clientId), clientId, opts)
      checkEntity("clients", Some(""), ConfigEntityName.Default, opts)
    }
    checkEntity("clients", None, "", describeOpts)
    checkInvalidArgs("clients", None, alterOpts)

    // <user> quota
    val principal = "CN=ConfigCommandTest,O=Apache,L=<default>"
    val sanitizedPrincipal = Sanitizer.sanitize(principal)
    assertEquals(-1, sanitizedPrincipal.indexOf('='))
    assertEquals(principal, Sanitizer.desanitize(sanitizedPrincipal))
    for (opts <- Seq(describeOpts, alterOpts)) {
      checkEntity("users", Some(principal), sanitizedPrincipal, opts)
      checkEntity("users", Some(""), ConfigEntityName.Default, opts)
    }
    checkEntity("users", None, "", describeOpts)
    checkInvalidArgs("users", None, alterOpts)

    // <user, client-id> quota
    val userClient = sanitizedPrincipal + "/clients/" + clientId
    def clientIdOpts(name: String) = Array("--entity-type", "clients", "--entity-name", name)
    for (opts <- Seq(describeOpts, alterOpts)) {
      checkEntity("users", Some(principal), userClient, opts ++ clientIdOpts(clientId))
      checkEntity("users", Some(principal), sanitizedPrincipal + "/clients/" + ConfigEntityName.Default, opts ++ clientIdOpts(""))
      checkEntity("users", Some(""), ConfigEntityName.Default + "/clients/" + clientId, describeOpts ++ clientIdOpts(clientId))
      checkEntity("users", Some(""), ConfigEntityName.Default + "/clients/" + ConfigEntityName.Default, opts ++ clientIdOpts(""))
    }
    checkEntity("users", Some(principal), sanitizedPrincipal + "/clients", describeOpts ++ Array("--entity-type", "clients"))
    // Both user and client-id must be provided for alter
    checkInvalidEntity("users", Some(principal), alterOpts ++ Array("--entity-type", "clients"))
    checkInvalidEntity("users", None, alterOpts ++ clientIdOpts(clientId))
    checkInvalidArgs("users", None, alterOpts ++ Array("--entity-type", "clients"))
  }

  @Test
  def testQuotaConfigEntity(): Unit = {
    doTestQuotaConfigEntity(zkConfig = false)
  }

  @Test
  def testUserClientQuotaOptsUsingZookeeperNotAllowed(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => doTestUserClientQuotaOpts(zkConfig = true))
  }

  def doTestUserClientQuotaOpts(zkConfig: Boolean): Unit = {
    val connectOpts = if (zkConfig)
      ("--zookeeper", zkConnect)
    else
      ("--bootstrap-server", "localhost:9092")

    def checkEntity(expectedEntityType: String, expectedEntityName: String, args: String*): Unit = {
      val opts = new ConfigCommandOptions(Array(connectOpts._1, connectOpts._2) ++ args)
      opts.checkArgs()
      val entity = ConfigCommand.parseEntity(opts)
      assertEquals(expectedEntityType, entity.root.entityType)
      assertEquals(expectedEntityName, entity.fullSanitizedName)
    }

    // <default> is a valid user principal and client-id (can be handled with URL-encoding),
    checkEntity("users", Sanitizer.sanitize("<default>"),
        "--entity-type", "users", "--entity-name", "<default>",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("clients", Sanitizer.sanitize("<default>"),
        "--entity-type", "clients", "--entity-name", "<default>",
        "--alter", "--add-config", "a=b,c=d")

    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients/client1",
        "--entity-type", "users", "--entity-name", "CN=user1", "--entity-type", "clients", "--entity-name", "client1",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients/client1",
        "--entity-name", "CN=user1", "--entity-type", "users", "--entity-name", "client1", "--entity-type", "clients",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients/client1",
        "--entity-type", "clients", "--entity-name", "client1", "--entity-type", "users", "--entity-name", "CN=user1",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients/client1",
        "--entity-name", "client1", "--entity-type", "clients", "--entity-name", "CN=user1", "--entity-type", "users",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients",
        "--entity-type", "clients", "--entity-name", "CN=user1", "--entity-type", "users",
        "--describe")
    checkEntity("users", "/clients",
        "--entity-type", "clients", "--entity-type", "users",
        "--describe")
    checkEntity("users", Sanitizer.sanitize("CN=user1") + "/clients/" + Sanitizer.sanitize("client1?@%"),
        "--entity-name", "client1?@%", "--entity-type", "clients", "--entity-name", "CN=user1", "--entity-type", "users",
        "--alter", "--add-config", "a=b,c=d")
  }

  @Test
  def testUserClientQuotaOpts(): Unit = {
    doTestUserClientQuotaOpts(zkConfig = false)
  }

  @Test
  def testQuotaDescribeEntities(): Unit = {
    val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])

    def checkEntities(opts: Array[String], expectedFetches: Map[String, Seq[String]], expectedEntityNames: Seq[String]): Unit = {
      val entity = ConfigCommand.parseEntity(new ConfigCommandOptions(opts :+ "--describe"))
      expectedFetches.foreach {
        case (name, values) => when(zkClient.getAllEntitiesWithConfig(name)).thenReturn(values)
      }
      val entities = entity.getAllEntities(zkClient)
      assertEquals(expectedEntityNames, entities.map(e => e.fullSanitizedName))
    }

    val clientId = "a-client"
    val principal = "CN=ConfigCommandTest.testQuotaDescribeEntities , O=Apache, L=<default>"
    val sanitizedPrincipal = Sanitizer.sanitize(principal)
    val userClient = sanitizedPrincipal + "/clients/" + clientId

    var opts = Array("--entity-type", "clients", "--entity-name", clientId)
    checkEntities(opts, Map.empty, Seq(clientId))

    opts = Array("--entity-type", "clients", "--entity-default")
    checkEntities(opts, Map.empty, Seq("<default>"))

    opts = Array("--entity-type", "clients")
    checkEntities(opts, Map("clients" -> Seq(clientId)), Seq(clientId))

    opts = Array("--entity-type", "users", "--entity-name", principal)
    checkEntities(opts, Map.empty, Seq(sanitizedPrincipal))

    opts = Array("--entity-type", "users", "--entity-default")
    checkEntities(opts, Map.empty, Seq("<default>"))

    opts = Array("--entity-type", "users")
    checkEntities(opts, Map("users" -> Seq("<default>", sanitizedPrincipal)), Seq("<default>", sanitizedPrincipal))

    opts = Array("--entity-type", "users", "--entity-name", principal, "--entity-type", "clients", "--entity-name", clientId)
    checkEntities(opts, Map.empty, Seq(userClient))

    opts = Array("--entity-type", "users", "--entity-name", principal, "--entity-type", "clients", "--entity-default")
    checkEntities(opts, Map.empty, Seq(sanitizedPrincipal + "/clients/<default>"))

    opts = Array("--entity-type", "users", "--entity-name", principal, "--entity-type", "clients")
    checkEntities(opts,
        Map("users/" + sanitizedPrincipal + "/clients" -> Seq("client-4")),
        Seq(sanitizedPrincipal + "/clients/client-4"))

    opts = Array("--entity-type", "users", "--entity-default", "--entity-type", "clients")
    checkEntities(opts,
        Map("users/<default>/clients" -> Seq("client-5")),
        Seq("<default>/clients/client-5"))

    opts = Array("--entity-type", "users", "--entity-type", "clients")
    val userMap = Map("users/" + sanitizedPrincipal + "/clients" -> Seq("client-2"))
    val defaultUserMap = Map("users/<default>/clients" -> Seq("client-3"))
    checkEntities(opts,
        Map("users" -> Seq("<default>", sanitizedPrincipal)) ++ defaultUserMap ++ userMap,
        Seq("<default>/clients/client-3", sanitizedPrincipal + "/clients/client-2"))
  }

  class DummyAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
    override def changeBrokerConfig(brokerIds: Seq[Int], configs: Properties): Unit = {}
    override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    override def changeClientIdConfig(clientId: String, configs: Properties): Unit = {}
    override def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configs: Properties, isUserClientId: Boolean = false): Unit = {}
    override def changeTopicConfig(topic: String, configs: Properties): Unit = {}
  }

  class DummyAdminClient(node: Node) extends MockAdminClient(util.Collections.singletonList(node), node) {
    override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult =
      mock(classOf[DescribeConfigsResult])
    override def incrementalAlterConfigs(configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]],
      options: AlterConfigsOptions): AlterConfigsResult = mock(classOf[AlterConfigsResult])
    override def alterConfigs(configs: util.Map[ConfigResource, Config], options: AlterConfigsOptions): AlterConfigsResult =
      mock(classOf[AlterConfigsResult])
    override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions): DescribeClientQuotasResult =
      mock(classOf[DescribeClientQuotasResult])
    override def alterClientQuotas(entries: util.Collection[ClientQuotaAlteration],
      options: AlterClientQuotasOptions): AlterClientQuotasResult =
      mock(classOf[AlterClientQuotasResult])
  }
}
