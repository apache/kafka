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

import java.util.Properties

import kafka.admin.ConfigCommand.ConfigCommandOptions
import kafka.common.InvalidConfigException
import kafka.server.{ConfigEntityName, QuotaId}
import kafka.utils.{Logging, ZkUtils}
import kafka.zk.ZooKeeperTestHarness

import org.apache.kafka.common.security.scram.{ScramCredential, ScramCredentialUtils, ScramMechanism}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import scala.collection.mutable
import scala.collection.JavaConverters._

class ConfigCommandTest extends ZooKeeperTestHarness with Logging {
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
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailIfUnrecognisedEntityType(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "client", "--entity-type", "not-recognised", "--alter", "--add-config", "a=b,c=d"))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test
  def shouldAddClientConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-client-id",
      "--entity-type", "clients",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configChange: Properties): Unit = {
        assertEquals("my-client-id", clientId)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldAddTopicConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("my-topic", topic)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldAddBrokerConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b,c=d"))

    val configChange = new TestAdminUtils {
      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
        assertEquals("b", configChange.get("a"))
        assertEquals("d", configChange.get("c"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test
  def shouldSupportCommaSeparatedValues(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--add-config", "a=b,c=[d,e ,f],g=[h,i]"))

    val configChange = new TestAdminUtils {
      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals(Seq(1), brokerIds)
        assertEquals("b", configChange.get("a"))
        assertEquals("d,e ,f", configChange.get("c"))
        assertEquals("h,i", configChange.get("g"))
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedEntityName(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1,2,3", //Don't support multiple brokers currently
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=b"))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a="))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def shouldNotUpdateBrokerConfigIfMalformedBracketConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "a=[b,c,d=e"))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test (expected = classOf[InvalidConfigException])
  def shouldNotUpdateBrokerConfigIfNonExistingConfigIsDeleted(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "my-topic",
      "--entity-type", "topics",
      "--alter",
      "--delete-config", "missing_config1, missing_config2"))
    ConfigCommand.alterConfig(null, createOpts, new TestAdminUtils)
  }

  @Test
  def shouldDeleteBrokerConfig(): Unit = {
    val createOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--delete-config", "a,c"))

    val configChange = new TestAdminUtils {
      override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {
        val properties: Properties = new Properties
        properties.put("a", "b")
        properties.put("c", "d")
        properties.put("e", "f")
        properties
      }

      override def changeBrokerConfig(zkUtils: ZkUtils, brokerIds: Seq[Int], configChange: Properties): Unit = {
        assertEquals("f", configChange.get("e"))
        assertEquals(1, configChange.size())
      }
    }
    ConfigCommand.alterConfig(null, createOpts, configChange)
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
    case class CredentialChange(val user: String, val mechanisms: Set[String], val iterations: Int) extends TestAdminUtils {
      override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {
        credentials.getOrElse(entityName, new Properties())
      }
      override def changeUserOrUserClientIdConfig(zkUtils: ZkUtils, sanitizedEntityName: String, configChange: Properties): Unit = {
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
    val sanitizedPrincipal = QuotaId.sanitize(principal)
    assertEquals(-1, sanitizedPrincipal.indexOf('='))
    assertEquals(principal, QuotaId.desanitize(sanitizedPrincipal))
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

    // <default> is a valid user principal (can be handled with URL-encoding),
    // but an invalid client-id (cannot be handled since client-ids are not encoded)
    checkEntity("users", QuotaId.sanitize("<default>"),
        "--entity-type", "users", "--entity-name", "<default>",
        "--alter", "--add-config", "a=b,c=d")
    try {
      checkEntity("clients", QuotaId.sanitize("<default>"),
          "--entity-type", "clients", "--entity-name", "<default>",
          "--alter", "--add-config", "a=b,c=d")
      fail("Did not fail with invalid client-id")
    } catch {
      case _: InvalidConfigException => // expected
    }

    checkEntity("users", QuotaId.sanitize("CN=user1") + "/clients/client1",
        "--entity-type", "users", "--entity-name", "CN=user1", "--entity-type", "clients", "--entity-name", "client1",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", QuotaId.sanitize("CN=user1") + "/clients/client1",
        "--entity-name", "CN=user1", "--entity-type", "users", "--entity-name", "client1", "--entity-type", "clients",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", QuotaId.sanitize("CN=user1") + "/clients/client1",
        "--entity-type", "clients", "--entity-name", "client1", "--entity-type", "users", "--entity-name", "CN=user1",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", QuotaId.sanitize("CN=user1") + "/clients/client1",
        "--entity-name", "client1", "--entity-type", "clients", "--entity-name", "CN=user1", "--entity-type", "users",
        "--alter", "--add-config", "a=b,c=d")
    checkEntity("users", QuotaId.sanitize("CN=user1") + "/clients",
        "--entity-type", "clients", "--entity-name", "CN=user1", "--entity-type", "users",
        "--describe")
    checkEntity("users", "/clients",
        "--entity-type", "clients", "--entity-type", "users",
        "--describe")
  }

  @Test
  def testQuotaDescribeEntities() {
    val zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])

    def checkEntities(opts: Array[String], expectedFetches: Map[String, Seq[String]], expectedEntityNames: Seq[String]) {
      val entity = ConfigCommand.parseEntity(new ConfigCommandOptions(opts :+ "--describe"))
      expectedFetches.foreach {
        case (name, values) => EasyMock.expect(zkUtils.getAllEntitiesWithConfig(name)).andReturn(values)
      }
      EasyMock.replay(zkUtils)
      val entities = entity.getAllEntities(zkUtils)
      assertEquals(expectedEntityNames, entities.map(e => e.fullSanitizedName))
      EasyMock.reset(zkUtils)
    }

    val clientId = "a-client"
    val principal = "CN=ConfigCommandTest.testQuotaDescribeEntities , O=Apache, L=<default>"
    val sanitizedPrincipal = QuotaId.sanitize(principal)
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
}
