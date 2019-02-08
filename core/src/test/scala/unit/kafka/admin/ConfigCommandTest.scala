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
import kafka.server.{ConfigEntityName, KafkaConfig}
import kafka.utils.{Exit, Logging}
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.Sanitizer
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.{Seq, mutable}
import scala.collection.JavaConverters._

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
  def shouldParseArgumentsForClientsEntityType() {
    testArgumentParse("clients")
  }

  @Test
  def shouldParseArgumentsForTopicsEntityType() {
    testArgumentParse("topics")
  }

  @Test
  def shouldParseArgumentsForBrokersEntityType() {
    testArgumentParse("brokers")
  }

  def testArgumentParse(entityType: String) = {
    // Should parse correctly
    var createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--describe"))
    createOpts.checkArgs()

    // For --alter and added config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--alter",
      "--add-config", "a=b,c=d"))
    createOpts.checkArgs()

    // For alter and deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
      "--alter",
      "--delete-config", "a,b,c"))
    createOpts.checkArgs()

    // For alter and both added, deleted config
    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
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

    createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "x",
      "--entity-type", entityType,
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
  def shouldFailIfUnrecognisedEntityType(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test
  def shouldAddClientConfig(): Unit = {
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

    ConfigCommand.alterConfig(null, createOpts, new TestAdminZkClient(zkClient))
  }

  @Test
  def shouldAddTopicConfig(): Unit = {
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

    ConfigCommand.alterConfig(null, createOpts, new TestAdminZkClient(zkClient))
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

    ConfigCommand.alterConfig(null, alterOpts, new TestAdminZkClient(zkClient))
  }

  @Test
  def shouldAddBrokerDynamicConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterBrokerConfig(node, "1", List("--entity-name", "1"))
  }

  @Test
  def shouldAddDefaultBrokerDynamicConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterBrokerConfig(node, "", List("--entity-default"))
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
        config.entries.asScala.foreach { e => brokerConfigs.put(e.name, e.value) }
        alterResult
      }
    }
    EasyMock.replay(alterResult, describeResult)
    ConfigCommand.alterBrokerConfig(mockAdminClient, alterOpts, resourceName)
    assertEquals(Map("message.max.bytes" -> "10", "num.io.threads" -> "5"), brokerConfigs.toMap)
    EasyMock.reset(alterResult, describeResult)
  }

  @Test
  def shouldSupportCommaSeparatedValues(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=[d,e ,f],g=[h,i]"))

    class TestAdminZkClient(zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeBrokerConfig(brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
        assertEquals("b", configChange.get("a"))
        assertEquals("d,e ,f", configChange.get("c"))
        assertEquals("h,i", configChange.get("g"))
      }

      override def changeTopicConfig(topic: String, configs: Properties): Unit = {}
    }

    ConfigCommand.alterConfig(null, createOpts, new TestAdminZkClient(zkClient))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedEntityName(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "leader.replication.throttled.rate=10"))
    ConfigCommand.alterConfig(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test
  def testDynamicBrokerConfigUpdateUsingZooKeeper(): Unit = {
    val brokerId = "1"
    val adminZkClient = new AdminZkClient(zkClient)
    val alterOpts = Array("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter")

    def entityOpt(brokerId: Option[String]): Array[String] = {
      brokerId.map(id => Array("--entity-name", id)).getOrElse(Array("--entity-default"))
    }

    def alterConfig(configs: Map[String, String], brokerId: Option[String],
                    encoderConfigs: Map[String, String] = Map.empty): Unit = {
      val configStr = (configs ++ encoderConfigs).map { case (k, v) => s"$k=$v" }.mkString(",")
      val addOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++ Array("--add-config", configStr))
      ConfigCommand.alterConfig(zkClient, addOpts, adminZkClient)
    }

    def verifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      val entityConfigs = zkClient.getEntityConfigs("brokers", brokerId.getOrElse(ConfigEntityName.Default))
      assertEquals(configs, entityConfigs.asScala)
    }

    def alterAndVerifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      alterConfig(configs, brokerId)
      verifyConfig(configs, brokerId)
    }

    def deleteAndVerifyConfig(configNames: Set[String], brokerId: Option[String]): Unit = {
      val deleteOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++
        Array("--delete-config", configNames.mkString(",")))
      ConfigCommand.alterConfig(zkClient, deleteOpts, adminZkClient)
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
    intercept[ConfigException](alterConfig(Map("ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId)))

    // Per-broker config configured at default cluster-level should fail
    intercept[ConfigException](alterConfig(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), None))
    deleteAndVerifyConfig(Set("listener.name.external.ssl.keystore.location"), Some(brokerId))

    // Password config update without encoder secret should fail
    intercept[IllegalArgumentException](alterConfig(Map("listener.name.external.ssl.keystore.password" -> "secret"), Some(brokerId)))

    // Password config update with encoder secret should succeed and encoded password must be stored in ZK
    val configs = Map("listener.name.external.ssl.keystore.password" -> "secret", "log.cleaner.threads" -> "2")
    val encoderConfigs = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret")
    alterConfig(configs, Some(brokerId), encoderConfigs)
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
    alterConfig(configs2, Some(brokerId), encoderConfigs2)
    val brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId)
    val encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password")
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs).decode(encodedPassword2).value)
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2).decode(encodedPassword2).value)


    // Password config update at default cluster-level should fail
    intercept[ConfigException](alterConfig(configs, None, encoderConfigs))

    // Dynamic config updates using ZK should fail if broker is running.
    registerBrokerInZk(brokerId.toInt)
    intercept[IllegalArgumentException](alterConfig(Map("message.max.size" -> "210000"), Some(brokerId)))
    intercept[IllegalArgumentException](alterConfig(Map("message.max.size" -> "220000"), None))

    // Dynamic config updates using ZK should for a different broker that is not running should succeed
    alterAndVerifyConfig(Map("message.max.size" -> "230000"), Some("2"))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=="))
    ConfigCommand.alterConfig(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    ConfigCommand.alterConfig(null, createOpts, new DummyAdminZkClient(zkClient))
  }

  @Test (expected = classOf[InvalidConfigurationException])
  def shouldNotUpdateBrokerConfigIfNonExistingConfigIsDeleted(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--delete-config", "missing_config1, missing_config2"))
    ConfigCommand.alterConfig(null, createOpts, new DummyAdminZkClient(zkClient))
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

    ConfigCommand.alterConfig(null, createOpts, new TestAdminZkClient(zkClient))
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
    ConfigCommand.alterConfig(null, optsA, CredentialChange("userA", Set("SCRAM-SHA-256"), 8192))
    val optsB = createOpts("userB", "SCRAM-SHA-256=[iterations=4096,password=abc, def],SCRAM-SHA-512=[password=1234=abc]")
    ConfigCommand.alterConfig(null, optsB, CredentialChange("userB", Set("SCRAM-SHA-256", "SCRAM-SHA-512"), 4096))

    val del256 = deleteOpts("userB", "SCRAM-SHA-256")
    ConfigCommand.alterConfig(null, del256, CredentialChange("userB", Set("SCRAM-SHA-512"), 4096))
    val del512 = deleteOpts("userB", "SCRAM-SHA-512")
    ConfigCommand.alterConfig(null, del512, CredentialChange("userB", Set(), 4096))
  }

  @Test
  def testQuotaConfigEntity() {

    def createOpts(entityType: String, entityName: Option[String], otherArgs: Array[String]) : ConfigCommandOptions = {
      val optArray = Array("--zookeeper", zkConnect,
                           "--entity-type", entityType)
      val nameArray = entityName match {
        case Some(name) => Array("--entity-name", name)
        case None => Array[String]()
      }
      new ConfigCommandOptions(optArray ++ nameArray ++ otherArgs)
    }

    def checkEntity(entityType: String, entityName: Option[String], expectedEntityName: String, otherArgs: Array[String]) {
      val opts = createOpts(entityType, entityName, otherArgs)
      opts.checkArgs()
      val entity = ConfigCommand.parseEntity(opts)
      assertEquals(entityType, entity.root.entityType)
      assertEquals(expectedEntityName, entity.fullSanitizedName)
    }

    def checkInvalidEntity(entityType: String, entityName: Option[String], otherArgs: Array[String]) {
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
  def testUserClientQuotaOpts() {
    def checkEntity(expectedEntityType: String, expectedEntityName: String, args: String*) {
      val opts = new ConfigCommandOptions(Array("--zookeeper", zkConnect) ++ args)
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
  def testQuotaDescribeEntities() {
    val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])

    def checkEntities(opts: Array[String], expectedFetches: Map[String, Seq[String]], expectedEntityNames: Seq[String]) {
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

}
