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
import kafka.api.ApiVersion
import kafka.cluster.{Broker, EndPoint}
import kafka.server.{ConfigEntityName, ConfigType, KafkaConfig}
import kafka.utils.{Exit, Logging}
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.Sanitizer
import org.apache.kafka.test.TestUtils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class ConfigCommandTest extends ZooKeeperTestHarness with Logging {

  @Test
  def shouldExitWithNonZeroStatusOnArgError(): Unit = {
    assertNonZeroStatusExit(Array("--blah"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnZkCommandError(): Unit = {
    assertNonZeroStatusExit(Array(
      "--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "security.inter.broker.protocol=PLAINTEXT"))
  }

  @Test
  def shouldExitWithNonZeroStatusOnBrokerCommandError(): Unit = {
    assertNonZeroStatusExit(Array(
      "--bootstrap-server", "invalid host",
      "--entity-type", "brokers",
      "--entity-name", "1",
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
      case e: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  @Test
  def shouldParseArgumentsForClientsEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse("clients", zkConfig = true)
  }

  @Test
  def shouldParseArgumentsForClientsEntityType(): Unit = {
    testArgumentParse("clients", zkConfig = false)
  }

  @Test
  def shouldParseArgumentsForUsersEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse("users", zkConfig = true)
  }

  @Test
  def shouldParseArgumentsForUsersEntityType(): Unit = {
    testArgumentParse("users", zkConfig = false)
  }

  @Test
  def shouldParseArgumentsForTopicsEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse("topics", zkConfig = true)
  }

  @Test
  def shouldParseArgumentsForTopicsEntityType(): Unit = {
    testArgumentParse("topics", zkConfig = false)
  }

  @Test
  def shouldParseArgumentsForBrokersEntityTypeUsingZookeeper(): Unit = {
    testArgumentParse("brokers", zkConfig = true)
  }

  @Test
  def shouldParseArgumentsForBrokersEntityType(): Unit = {
    testArgumentParse("brokers", zkConfig = false)
  }

  @Test
  def shouldParseArgumentsForBrokerLoggersEntityType(): Unit = {
    testArgumentParse("broker-loggers", zkConfig = false)
  }

  def testArgumentParse(entityType: String, zkConfig: Boolean): Unit = {
    val shortFlag: String = s"--${entityType.dropRight(1)}"

    val connectOpts = if (zkConfig)
      ("--zookeeper", zkConnect)
    else
      ("--bootstrap-server", "localhost:9092")

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
      "--add-config", "a=b,c=,d=e,f="))
    createOpts.checkArgs()

    val addedProps2 = ConfigCommand.parseConfigsToBeAdded(createOpts)
    assertEquals(4, addedProps2.size())
    assertEquals("b", addedProps2.getProperty("a"))
    assertEquals("e", addedProps2.getProperty("d"))
    assertTrue(addedProps2.getProperty("c").isEmpty)
    assertTrue(addedProps2.getProperty("f").isEmpty)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfAddAndAddFile(): Unit = {
    // Should not parse correctly
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b,c=d",
      "--add-config-file", "/tmp/new.properties"
    ))
    createOpts.checkArgs()
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

    testExpectedEntityTypeNames(List(ConfigType.Topic), List("A"), "--entity-type", "topics", "--entity-name", "A")
    testExpectedEntityTypeNames(List(ConfigType.Broker), List("0"), "--entity-name", "0", "--entity-type", "brokers")
    testExpectedEntityTypeNames(List(ConfigType.User, ConfigType.Client), List("A", ""),
      "--entity-type", "users", "--entity-type", "clients", "--entity-name", "A", "--entity-default")
    testExpectedEntityTypeNames(List(ConfigType.User, ConfigType.Client), List("", "B"),
      "--entity-default", "--entity-name", "B", "--entity-type", "users", "--entity-type", "clients")

    testExpectedEntityTypeNames(List(ConfigType.Topic), List("A"), "--topic", "A")
    testExpectedEntityTypeNames(List(ConfigType.Broker), List("0"), "--broker", "0")
    testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("B", "A"), "--client", "B", "--user", "A")
    testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("B", ""), "--client", "B", "--user-defaults")
    testExpectedEntityTypeNames(List(ConfigType.Client, ConfigType.User), List("A"),
      "--entity-type", "clients", "--entity-type", "users", "--entity-name", "A")

    testExpectedEntityTypeNames(List(ConfigType.Topic), List.empty, "--entity-type", "topics")
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

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfUnrecognisedEntityTypeUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfUnrecognisedEntityType(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfBrokerEntityTypeIsNotAnIntegerUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "A", "--entity-type", "brokers", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfBrokerEntityTypeIsNotAnInteger(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "A", "--entity-type", "brokers", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfShortBrokerEntityTypeIsNotAnIntegerUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--broker", "A", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfShortBrokerEntityTypeIsNotAnInteger(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--broker", "A", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfMixedEntityTypeFlagsUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "A", "--entity-type", "users", "--client", "B", "--describe"))
    createOpts.checkArgs()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfMixedEntityTypeFlags(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "A", "--entity-type", "users", "--client", "B", "--describe"))
    createOpts.checkArgs()
  }

  @Test
  def shouldAddClientConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-client-id",
      "--entity-type", "clients",
      "--alter",
      "--add-config", "a=b,c=d"))

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeClientIdConfig(clientId: String, configChange: Properties): Unit = {
        assertEquals("my-client-id", clientId)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
  }

  def testShouldAddClientConfig(user: Option[String], clientId: Option[String]): Unit = {
    def toValues(entityName: Option[String], entityType: String, command: String):
        (Array[String], Option[String], Option[ClientQuotaFilterComponent]) = {
      entityName match {
        case Some(null) =>
          (Array("--entity-type", command, "--entity-default"), Some(null),
            Some(ClientQuotaFilterComponent.ofDefaultEntity(entityType)))
        case Some(name) =>
          (Array("--entity-type", command, "--entity-name", name), Some(name),
            Some(ClientQuotaFilterComponent.ofEntity(entityType, name)))
        case None => (Array.empty, None, None)
      }
    }
    val (userArgs, userEntity, userComponent) = toValues(user, ClientQuotaEntity.USER, "users")
    val (clientIdArgs, clientIdEntity, clientIdComponent) = toValues(clientId, ClientQuotaEntity.CLIENT_ID, "clients")

    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--alter",
      "--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000",
      "--delete-config", "request_percentage") ++ userArgs ++ clientIdArgs)

    // Explicitly populate a HashMap to ensure nulls are recorded properly.
    val entityMap = new java.util.HashMap[String, String]
    userEntity.foreach(u => entityMap.put(ClientQuotaEntity.USER, u))
    clientIdEntity.foreach(c => entityMap.put(ClientQuotaEntity.CLIENT_ID, c))
    val entity = new ClientQuotaEntity(entityMap)

    var describedConfigs = false
    val describeFuture = new KafkaFutureImpl[util.Map[ClientQuotaEntity, util.Map[String, java.lang.Double]]]
    describeFuture.complete(Map((entity -> Map(("request_percentage" -> Double.box(50.0))).asJava)).asJava)
    val describeResult: DescribeClientQuotasResult = EasyMock.createNiceMock(classOf[DescribeClientQuotasResult])
    EasyMock.expect(describeResult.entities()).andReturn(describeFuture)

    var alteredConfigs = false
    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterClientQuotasResult = EasyMock.createNiceMock(classOf[AlterClientQuotasResult])
    EasyMock.expect(alterResult.all()).andReturn(alterFuture)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions): DescribeClientQuotasResult = {
        assertTrue(filter.strict)
        val components = filter.components.asScala.toSeq
        userComponent.foreach(c => assertTrue(components.contains(c)))
        clientIdComponent.foreach(c => assertTrue(components.contains(c)))
        describedConfigs = true
        describeResult
      }

      override def alterClientQuotas(entries: util.Collection[ClientQuotaAlteration], options: AlterClientQuotasOptions): AlterClientQuotasResult = {
        assertFalse(options.validateOnly)
        assertEquals(1, entries.size)
        val alteration = entries.asScala.head
        assertEquals(entity, alteration.entity)
        val ops = alteration.ops.asScala
        assertEquals(3, ops.size)
        val expectedOps = Set(
          new ClientQuotaAlteration.Op("consumer_byte_rate", Double.box(20000)),
          new ClientQuotaAlteration.Op("producer_byte_rate", Double.box(10000)),
          new ClientQuotaAlteration.Op("request_percentage", null)
        )
        assertEquals(expectedOps, ops.toSet)
        alteredConfigs = true
        alterResult
      }
    }
    EasyMock.replay(alterResult, describeResult)
    ConfigCommand.alterConfig(mockAdminClient, createOpts)
    assertTrue(describedConfigs)
    assertTrue(alteredConfigs)
    EasyMock.reset(alterResult, describeResult)
  }


  @Test
  def shouldAddClientConfig(): Unit = {
    testShouldAddClientConfig(Some("test-user-1"), Some("test-client-1"))
    testShouldAddClientConfig(Some("test-user-2"), Some(null))
    testShouldAddClientConfig(Some("test-user-3"), None)
    testShouldAddClientConfig(Some(null), Some("test-client-2"))
    testShouldAddClientConfig(Some(null), Some(null))
    testShouldAddClientConfig(Some(null), None)
    testShouldAddClientConfig(None, Some("test-client-3"))
    testShouldAddClientConfig(None, Some(null))
  }

  @Test
  def shouldNotAlterNonQuotaNonScramUserOrClientConfigUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter anything that is not a quota and not a SCRAM credential
    // for both user and client entities
    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node)

    def verifyCommand(entityType: String, alterOpts: String*): Unit = {
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--entity-type", entityType, "--entity-name", "admin",
        "--alter") ++ alterOpts)
      val e = intercept[IllegalArgumentException] {
        ConfigCommand.alterConfig(mockAdminClient, opts)
      }
      assertTrue(s"Unexpected exception: $e", e.getMessage.contains("some_config"))
    }

    verifyCommand("users", "--add-config", "consumer_byte_rate=20000,producer_byte_rate=10000,some_config=10")
    verifyCommand("users", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret],some_config=10")
    verifyCommand("clients", "--add-config", "some_config=10")
    verifyCommand("users", "--delete-config", "consumer_byte_rate,some_config")
    verifyCommand("users", "--delete-config", "SCRAM-SHA-256,some_config")
    verifyCommand("clients", "--delete-config", "some_config")
  }

  @Test
  def shouldNotAlterScramClientConfigUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter SCRAM credentials for client entities
    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node)

    def verifyCommand(entityType: String, alterOpts: String*): Unit = {
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--entity-type", entityType, "--entity-name", "admin",
        "--alter") ++ alterOpts)
      val e = intercept[IllegalArgumentException] {
        ConfigCommand.alterConfig(mockAdminClient, opts)
      }
      assertTrue(s"Unexpected exception: $e", e.getMessage.contains("SCRAM-SHA-256"))
    }

    verifyCommand("clients", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
    verifyCommand("clients", "--delete-config", "SCRAM-SHA-256")
  }

  @Test
  def shouldNotCreateUserScramCredentialConfigWithUnderMinimumIterationsUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to create a SCRAM credential for a user
    // with an iterations value less than the minimum
    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node)

    def verifyCommand(entityType: String, alterOpts: String*): Unit = {
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--entity-type", entityType, "--entity-name", "admin",
        "--alter") ++ alterOpts)
      val e = intercept[IllegalArgumentException] {
        ConfigCommand.alterConfig(mockAdminClient, opts)
      }
      assertTrue(s"Unexpected exception: $e", e.getMessage.contains("SCRAM-SHA-256"))
    }

    verifyCommand("users", "--add-config", "SCRAM-SHA-256=[iterations=100,password=foo-secret]")
  }

  @Test
  def shouldNotAlterUserScramCredentialAndClientQuotaConfigsSimultaneouslyUsingBootstrapServer(): Unit = {
    // when using --bootstrap-server, it should be illegal to alter both SCRAM credentials and quotas for user entities
    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node)

    def verifyCommand(alterOpts: String*): Unit = {
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092", "--alter") ++ alterOpts)
      val e = intercept[IllegalArgumentException] {
        ConfigCommand.alterConfig(mockAdminClient, opts)
      }
      assertTrue(s"Unexpected exception: $e", e.getMessage.contains("SCRAM-SHA-256"))
    }

    verifyCommand("--entity-type", "users", "--entity-name", "admin", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]",
      "--entity-type", "users", "--entity-name", "admin", "--delete-config", "consumer_byte_rate")
    verifyCommand("--entity-type", "users", "--entity-name", "admin", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]",
      "--entity-type", "users", "--entity-name", "admin1", "--delete-config", "consumer_byte_rate")
    verifyCommand("--entity-type", "users", "--entity-name", "admin", "--delete-config", "SCRAM-SHA-256",
      "--entity-type", "users", "--entity-name", "admin", "--add-config", "consumer_byte_rate=20000")
    verifyCommand("--entity-type", "users", "--entity-name", "admin", "--delete-config", "SCRAM-SHA-256",
      "--entity-type", "users", "--entity-name", "admin1", "--add-config", "consumer_byte_rate=20000")

    verifyCommand("--entity-type", "clients", "--entity-name", "admin", "--delete-config", "consumer_byte_rate",
      "--entity-type", "users", "--entity-name", "admin", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
    verifyCommand( "--entity-type", "clients", "--entity-name", "admin1", "--delete-config", "consumer_byte_rate",
      "--entity-type", "users", "--entity-name", "admin", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
    verifyCommand("--entity-type", "clients", "--entity-name", "admin", "--add-config", "consumer_byte_rate=20000",
      "--entity-type", "users", "--entity-name", "admin", "--delete-config", "SCRAM-SHA-256")
    verifyCommand("--entity-type", "users", "--entity-name", "admin1", "--add-config", "consumer_byte_rate=20000",
      "--entity-type", "users", "--entity-name", "admin", "--delete-config", "SCRAM-SHA-256")
  }

  @Test
  def shouldNotDescribeUserScramCredentialsWithEntityDefaultUsingBootstrapServer(): Unit = {
    // User SCRAM credentials should not be described when specifying
    // --describe --entity-type users --entity-default (or --user-defaults) with --bootstrap-server
    val describeFuture = new KafkaFutureImpl[util.Map[ClientQuotaEntity, util.Map[String, java.lang.Double]]]
    describeFuture.complete(Map((new ClientQuotaEntity(Map("" -> "").asJava) -> Map(("request_percentage" -> Double.box(50.0))).asJava)).asJava)
    val describeClientQuotasResult: DescribeClientQuotasResult = EasyMock.createNiceMock(classOf[DescribeClientQuotasResult])
    EasyMock.expect(describeClientQuotasResult.entities()).andReturn(describeFuture).times(2)
    EasyMock.replay(describeClientQuotasResult)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions):  DescribeClientQuotasResult = {
        describeClientQuotasResult
      }
      override def describeUserScramCredentials(users: util.List[String], options: DescribeUserScramCredentialsOptions): DescribeUserScramCredentialsResult = {
        throw new IllegalStateException("Incorrectly described SCRAM credentials when specifying --entity-default with --bootstrap-server")
      }
    }

    def verifyCommand(expectedMessage: String, alterOrDescribeOpt: String, requestOpts: String*): Unit = {
      val opts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
        alterOrDescribeOpt) ++ requestOpts)
      if (alterOrDescribeOpt.equals("--describe"))
        ConfigCommand.describeConfig(mockAdminClient, opts) // fails if describeUserScramCredentials() is invoked
      else {
        val e = intercept[IllegalArgumentException] {
          ConfigCommand.alterConfig(mockAdminClient, opts)
        }
        assertTrue(s"Unexpected exception: $e", e.getMessage.contains(expectedMessage))
      }
    }

    val expectedMsg = "The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server."
    verifyCommand(expectedMsg, "--alter", "--entity-type", "users", "--entity-default", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
    verifyCommand(expectedMsg, "--alter", "--entity-type", "users", "--entity-default", "--delete-config", "SCRAM-SHA-256")
    verifyCommand(expectedMsg, "--describe", "--entity-type", "users", "--entity-default")
    verifyCommand(expectedMsg, "--alter", "--user-defaults", "--add-config", "SCRAM-SHA-256=[iterations=8192,password=foo-secret]")
    verifyCommand(expectedMsg, "--alter", "--user-defaults", "--delete-config", "SCRAM-SHA-256")
    verifyCommand(expectedMsg, "--describe", "--user-defaults")
  }

  @Test
  def shouldAddTopicConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=d"))

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
    val addedConfigs = Seq("delete.retention.ms=1000000", "min.insync.replicas=2")
    if (file) {
      val file = TestUtils.tempFile(addedConfigs.mkString("\n"))
      filePath = file.getPath
    }

    val resourceName = "my-topic"
    val alterOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", resourceName,
      "--entity-type", "topics",
      "--alter",
      if (file) "--add-config-file" else "--add-config",
      if (file) filePath else addedConfigs.mkString(","),
      "--delete-config", "unclean.leader.election.enable"))
    var alteredConfigs = false

    def newConfigEntry(name: String, value: String): ConfigEntry =
      ConfigTest.newConfigEntry(name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, List.empty[ConfigEntry.ConfigSynonym].asJava)

    val resource = new ConfigResource(ConfigResource.Type.TOPIC, resourceName)
    val configEntries = List(newConfigEntry("min.insync.replicas", "1"), newConfigEntry("unclean.leader.election.enable", "1")).asJava
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(configEntries)))
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    EasyMock.expect(describeResult.all()).andReturn(future).once()

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = EasyMock.createNiceMock(classOf[AlterConfigsResult])
    EasyMock.expect(alterResult.all()).andReturn(alterFuture)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertFalse("Config synonyms requested unnecessarily", options.includeSynonyms())
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
        assertEquals(expectedConfigOps, alterConfigOps.asScala.toSet)
        alteredConfigs = true
        alterResult
      }
    }
    EasyMock.replay(alterResult, describeResult)
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertTrue(alteredConfigs)
    EasyMock.reset(alterResult, describeResult)
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
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    EasyMock.expect(describeResult.all()).andReturn(future).once()

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertTrue("Synonyms not requested", options.includeSynonyms())
        assertEquals(Set(resource), resources.asScala.toSet)
        describeResult
      }
    }
    EasyMock.replay(describeResult)
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    EasyMock.reset(describeResult)
  }

  @Test
  def shouldAddBrokerQuotaConfig(): Unit = {
    val alterOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10,follower.replication.throttled.rate=20"))

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeBrokerConfig(brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
        assertEquals("10", configChange.get("leader.replication.throttled.rate"))
        assertEquals("20", configChange.get("follower.replication.throttled.rate"))
      }
    }

    ConfigCommand.alterConfigWithZk(null, alterOpts, new TestAdminZkClient(zkClient))
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

  @Test(expected = classOf[IllegalArgumentException])
  def testDescribeAllBrokerConfigBootstrapServerRequired(): Unit = {
    val optsList = List("--zookeeper", zkConnect,
      "--entity-type", ConfigType.Broker,
      "--entity-name", "1",
      "--describe",
      "--all")

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEntityDefaultOptionWithDescribeBrokerLoggerIsNotAllowed(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigCommand.BrokerLoggerConfigType,
      "--entity-default",
      "--describe"
    )

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEntityDefaultOptionWithAlterBrokerLoggerIsNotAllowed(): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", ConfigCommand.BrokerLoggerConfigType,
      "--entity-default",
      "--alter",
      "--add-config", "kafka.log.LogCleaner=DEBUG"
    )

    new ConfigCommandOptions(optsList.toArray).checkArgs()
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def shouldRaiseInvalidConfigurationExceptionWhenAddingInvalidBrokerLoggerConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    // verifyAlterBrokerLoggerConfig tries to alter kafka.log.LogCleaner, kafka.server.ReplicaManager and kafka.server.KafkaApi
    // yet, we make it so DescribeConfigs returns only one logger, implying that kafka.server.ReplicaManager and kafka.log.LogCleaner are invalid
    verifyAlterBrokerLoggerConfig(node, "1", "1", List(
      new ConfigEntry("kafka.server.KafkaApi", "INFO")
    ))
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
      "--add-config", "message.max.bytes=10") ++ resourceOpts
    val alterOpts = new ConfigCommandOptions(optsList.toArray)
    val brokerConfigs = mutable.Map[String, String]("num.io.threads" -> "5")

    val resource = new ConfigResource(ConfigResource.Type.BROKER, resourceName)
    val configEntries = util.Collections.singletonList(new ConfigEntry("num.io.threads", "5"))
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    future.complete(util.Collections.singletonMap(resource, new Config(configEntries)))
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    EasyMock.expect(describeResult.all()).andReturn(future).once()

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = EasyMock.createNiceMock(classOf[AlterConfigsResult])
    EasyMock.expect(alterResult.all()).andReturn(alterFuture)

    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertFalse("Config synonyms requested unnecessarily", options.includeSynonyms())
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
    EasyMock.replay(alterResult, describeResult)
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertEquals(Map("message.max.bytes" -> "10", "num.io.threads" -> "5"), brokerConfigs.toMap)
    EasyMock.reset(alterResult, describeResult)
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
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    // make sure it will be called 2 times: (1) for broker "1" (2) for default broker ""
    EasyMock.expect(describeResult.all()).andReturn(future).times(2)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertTrue("Synonyms not requested", options.includeSynonyms())
        val resource = resources.iterator.next
        assertEquals(ConfigResource.Type.BROKER, resource.`type`)
        assertTrue(resourceCustom.name == resource.name || resourceDefault.name == resource.name)
        assertEquals(1, resources.size)
        describeResult
      }
    }
    EasyMock.replay(describeResult)
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    EasyMock.verify(describeResult)
    EasyMock.reset(describeResult)
  }

  def verifyAlterBrokerLoggerConfig(node: Node, resourceName: String, entityName: String,
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
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    EasyMock.expect(describeResult.all()).andReturn(future).once()

    val alterFuture = new KafkaFutureImpl[Void]
    alterFuture.complete(null)
    val alterResult: AlterConfigsResult = EasyMock.createNiceMock(classOf[AlterConfigsResult])
    EasyMock.expect(alterResult.all()).andReturn(alterFuture)

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
    EasyMock.replay(alterResult, describeResult)
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    assertTrue(alteredConfigs)
    EasyMock.reset(alterResult, describeResult)
  }

  @Test
  def shouldSupportCommaSeparatedValuesUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=[d,e ,f],g=[h,i]"))

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

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedEntityNameUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedEntityName(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10"))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test
  def testDynamicBrokerConfigUpdateUsingZooKeeper(): Unit = {
    val brokerId = "1"
    val adminZkClient = new AdminZkClient(zkClient)
    val alterOpts = Array("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter")

    def entityOpt(brokerId: Option[String]): Array[String] = {
      brokerId.map(id => Array("--entity-name", id)).getOrElse(Array("--entity-default"))
    }

    def alterConfigWithZk(configs: Map[String, String], brokerId: Option[String],
                          encoderConfigs: Map[String, String] = Map.empty): Unit = {
      val configStr = (configs ++ encoderConfigs).map { case (k, v) => s"$k=$v" }.mkString(",")
      val addOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++ Array("--add-config", configStr))
      ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient)
    }

    def verifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      val entityConfigs = zkClient.getEntityConfigs("brokers", brokerId.getOrElse(ConfigEntityName.Default))
      assertEquals(configs, entityConfigs.asScala)
    }

    def alterAndVerifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      alterConfigWithZk(configs, brokerId)
      verifyConfig(configs, brokerId)
    }

    def deleteAndVerifyConfig(configNames: Set[String], brokerId: Option[String]): Unit = {
      val deleteOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++
        Array("--delete-config", configNames.mkString(",")))
      ConfigCommand.alterConfigWithZk(zkClient, deleteOpts, adminZkClient)
      verifyConfig(Map.empty, brokerId)
    }

    // Add config
    alterAndVerifyConfig(Map("message.max.size" -> "110000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "120000"), None)

    // Change config
    alterAndVerifyConfig(Map("message.max.size" -> "130000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "140000"), None)

    // Delete config
    deleteAndVerifyConfig(Set("message.max.size"), Some(brokerId))
    deleteAndVerifyConfig(Set("message.max.size"), None)

    // Listener configs: should work only with listener name
    alterAndVerifyConfig(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId))
    intercept[ConfigException](alterConfigWithZk(Map("ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId)))

    // Per-broker config configured at default cluster-level should fail
    intercept[ConfigException](alterConfigWithZk(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), None))
    deleteAndVerifyConfig(Set("listener.name.external.ssl.keystore.location"), Some(brokerId))

    // Password config update without encoder secret should fail
    intercept[IllegalArgumentException](alterConfigWithZk(Map("listener.name.external.ssl.keystore.password" -> "secret"), Some(brokerId)))

    // Password config update with encoder secret should succeed and encoded password must be stored in ZK
    val configs = Map("listener.name.external.ssl.keystore.password" -> "secret", "log.cleaner.threads" -> "2")
    val encoderConfigs = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret")
    alterConfigWithZk(configs, Some(brokerId), encoderConfigs)
    val brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId)
    assertFalse("Encoder secret stored in ZooKeeper", brokerConfigs.contains(KafkaConfig.PasswordEncoderSecretProp))
    assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")) // not encoded
    val encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password")
    val passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs)
    assertEquals("secret", passwordEncoder.decode(encodedPassword).value)
    assertEquals(configs.size, brokerConfigs.size)

    // Password config update with overrides for encoder parameters
    val configs2 = Map("listener.name.internal.ssl.keystore.password" -> "secret2")
    val encoderConfigs2 = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret",
      KafkaConfig.PasswordEncoderCipherAlgorithmProp -> "DES/CBC/PKCS5Padding",
      KafkaConfig.PasswordEncoderIterationsProp -> "1024",
      KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp -> "PBKDF2WithHmacSHA1",
      KafkaConfig.PasswordEncoderKeyLengthProp -> "64")
    alterConfigWithZk(configs2, Some(brokerId), encoderConfigs2)
    val brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId)
    val encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password")
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs).decode(encodedPassword2).value)
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2).decode(encodedPassword2).value)


    // Password config update at default cluster-level should fail
    intercept[ConfigException](alterConfigWithZk(configs, None, encoderConfigs))

    // Dynamic config updates using ZK should fail if broker is running.
    registerBrokerInZk(brokerId.toInt)
    intercept[IllegalArgumentException](alterConfigWithZk(Map("message.max.size" -> "210000"), Some(brokerId)))
    intercept[IllegalArgumentException](alterConfigWithZk(Map("message.max.size" -> "220000"), None))

    // Dynamic config updates using ZK should for a different broker that is not running should succeed
    alterAndVerifyConfig(Map("message.max.size" -> "230000"), Some("2"))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=="))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=="))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfigUsingZookeeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    ConfigCommand.alterConfig(new DummyAdminClient(new Node(1, "localhost", 9092)), createOpts)
  }

  @Test (expected = classOf[InvalidConfigurationException])
  def shouldNotUpdateConfigIfNonExistingConfigIsDeletedUsingZookeper(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--delete-config", "missing_config1, missing_config2"))
    ConfigCommand.alterConfigWithZk(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[InvalidConfigurationException])
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
    val describeResult: DescribeConfigsResult = EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    EasyMock.expect(describeResult.all()).andReturn(future).once()

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

    EasyMock.replay(describeResult)
    ConfigCommand.alterConfig(mockAdminClient, createOpts)
    EasyMock.reset(describeResult)
  }

  @Test
  def shouldDeleteBrokerConfig(): Unit = {
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

    ConfigCommand.alterConfigWithZk(null, createOpts, new TestAdminZkClient(zkClient))
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
    case class CredentialChange(user: String, mechanisms: Set[String], iterations: Int) extends AdminZkClient(zkClient) {
      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {
        credentials.getOrElse(entityName, new Properties())
      }
      override def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configChange: Properties): Unit = {
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

    def checkInvalidEntity(entityType: String, entityName: Option[String], otherArgs: Array[String]): Unit = {
      val opts = createOpts(entityType, entityName, otherArgs)
      try {
        opts.checkArgs()
        ConfigCommand.parseEntity(opts)
        fail("Did not fail with invalid argument list")
      } catch {
        case _: IllegalArgumentException => // expected exception
      }
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
    checkInvalidEntity("clients", None, alterOpts)

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
    checkInvalidEntity("users", None, alterOpts)

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
    checkInvalidEntity("users", None, alterOpts ++ Array("--entity-type", "clients"))
  }

  @Test
  def testQuotaConfigEntityUsingZookeeper(): Unit = {
    doTestQuotaConfigEntity(zkConfig = true)
  }

  @Test
  def testQuotaConfigEntity(): Unit = {
    doTestQuotaConfigEntity(zkConfig = false)
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
  def testUserClientQuotaOptsUsingZookeeper(): Unit = {
    doTestUserClientQuotaOpts(zkConfig = true)
  }

  @Test
  def testUserClientQuotaOpts(): Unit = {
    doTestUserClientQuotaOpts(zkConfig = false)
  }

  @Test
  def testQuotaDescribeEntities(): Unit = {
    val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])

    def checkEntities(opts: Array[String], expectedFetches: Map[String, Seq[String]], expectedEntityNames: Seq[String]): Unit = {
      val entity = ConfigCommand.parseEntity(new ConfigCommandOptions(opts :+ "--describe"))
      expectedFetches.foreach {
        case (name, values) => EasyMock.expect(zkClient.getAllEntitiesWithConfig(name)).andReturn(values)
      }
      EasyMock.replay(zkClient)
      val entities = entity.getAllEntities(zkClient)
      assertEquals(expectedEntityNames, entities.map(e => e.fullSanitizedName))
      EasyMock.reset(zkClient)
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

  @Test
  def testRemoveDuplicateFromList(): Unit = {
    val brokerOpts = Array("--bootstrap-server", "localhost:9092")
    val zkOpts = Array("--zookeeper", "localhost:2181")
    val commonOpts = Array("--entity-type", "topics", "--entity-name", "test", "--alter")
    val configOpts = Array("--add-config", "cleanup.policy=[delete,delete,compact],segment.bytes=123456")
    val addedConfigs = Seq("segment.bytes=123456", "cleanup.policy=delete,delete,compact")
    val file = TestUtils.tempFile(addedConfigs.mkString("\n"))
    val fileOpts = Array("--add-config-file", file.getPath)

    for (config <- Array(
      brokerOpts ++ commonOpts ++ configOpts,
      brokerOpts ++ commonOpts ++ fileOpts,
      zkOpts ++ commonOpts ++ configOpts,
      zkOpts ++ commonOpts ++ fileOpts)) {
      val props = ConfigCommand.parseConfigsToBeAdded(new ConfigCommandOptions(config))
      ConfigCommand.preProcessTopicConfigs(props)
      assertEquals(2, props.values().size)
      assertEquals("delete,compact", props.get("cleanup.policy"))
    }
  }

  private def registerBrokerInZk(id: Int): Unit = {
    zkClient.createTopLevelPaths()
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val endpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    val brokerInfo = BrokerInfo(Broker(id, Seq(endpoint), rack = None), ApiVersion.latestVersion, jmxPort = 9192)
    zkClient.registerBroker(brokerInfo)
  }

  class DummyAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
    override def changeBrokerConfig(brokerIds: Seq[Int], configs: Properties): Unit = {}
    override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    override def changeClientIdConfig(clientId: String, configs: Properties): Unit = {}
    override def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configs: Properties): Unit = {}
    override def changeTopicConfig(topic: String, configs: Properties): Unit = {}
  }

  class DummyAdminClient(node: Node) extends MockAdminClient(util.Collections.singletonList(node), node) {
    override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult =
      EasyMock.createNiceMock(classOf[DescribeConfigsResult])
    override def incrementalAlterConfigs(configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]],
      options: AlterConfigsOptions): AlterConfigsResult = EasyMock.createNiceMock(classOf[AlterConfigsResult])
    override def alterConfigs(configs: util.Map[ConfigResource, Config], options: AlterConfigsOptions): AlterConfigsResult =
      EasyMock.createNiceMock(classOf[AlterConfigsResult])
    override def describeClientQuotas(filter: ClientQuotaFilter, options: DescribeClientQuotasOptions): DescribeClientQuotasResult =
      EasyMock.createNiceMock(classOf[DescribeClientQuotasResult])
    override def alterClientQuotas(entries: util.Collection[ClientQuotaAlteration],
      options: AlterClientQuotasOptions): AlterClientQuotasResult =
      EasyMock.createNiceMock(classOf[AlterClientQuotasResult])
  }
}
