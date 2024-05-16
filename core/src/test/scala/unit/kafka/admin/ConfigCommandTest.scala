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
import kafka.utils.Logging
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaFilter}
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.Sanitizer
import org.apache.kafka.server.config.ZooKeeperInternals
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, verify, when}

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class ConfigCommandTest extends Logging {

  private val zkConnect = "localhost:2181"
  private val dummyAdminZkClient = new DummyAdminZkClient(null)

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
      checkEntity("clients", Some(""), ZooKeeperInternals.DEFAULT_STRING, opts)
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
      checkEntity("users", Some(""), ZooKeeperInternals.DEFAULT_STRING, opts)
    }
    checkEntity("users", None, "", describeOpts)
    checkInvalidArgs("users", None, alterOpts)

    // <user, client-id> quota
    val userClient = sanitizedPrincipal + "/clients/" + clientId
    def clientIdOpts(name: String) = Array("--entity-type", "clients", "--entity-name", name)
    for (opts <- Seq(describeOpts, alterOpts)) {
      checkEntity("users", Some(principal), userClient, opts ++ clientIdOpts(clientId))
      checkEntity("users", Some(principal), sanitizedPrincipal + "/clients/" + ZooKeeperInternals.DEFAULT_STRING, opts ++ clientIdOpts(""))
      checkEntity("users", Some(""), ZooKeeperInternals.DEFAULT_STRING + "/clients/" + clientId, describeOpts ++ clientIdOpts(clientId))
      checkEntity("users", Some(""), ZooKeeperInternals.DEFAULT_STRING + "/clients/" + ZooKeeperInternals.DEFAULT_STRING, opts ++ clientIdOpts(""))
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

  @Test
  def shouldAlterClientMetricsConfig(): Unit = {
    val node = new Node(1, "localhost", 9092)
    verifyAlterClientMetricsConfig(node, "1", List("--entity-name", "1"))
  }

  private def verifyAlterClientMetricsConfig(node: Node, resourceName: String, resourceOpts: List[String]): Unit = {
    val optsList = List("--bootstrap-server", "localhost:9092",
      "--entity-type", "client-metrics",
      "--alter",
      "--delete-config", "interval.ms",
      "--add-config", "metrics=org.apache.kafka.consumer.," +
        "match=[client_software_name=kafka.python,client_software_version=1\\.2\\..*]") ++ resourceOpts
    val alterOpts = new ConfigCommandOptions(optsList.toArray)

    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, resourceName)
    val configEntries = util.Collections.singletonList(new ConfigEntry("interval.ms", "1000",
      ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG, false, false, util.Collections.emptyList[ConfigEntry.ConfigSynonym],
      ConfigEntry.ConfigType.UNKNOWN, null))
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
        assertEquals(ConfigResource.Type.CLIENT_METRICS, resource.`type`)
        assertEquals(resourceName, resource.name)
        describeResult
      }

      override def incrementalAlterConfigs(configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]], options: AlterConfigsOptions): AlterConfigsResult = {
        assertEquals(1, configs.size)
        val entry = configs.entrySet.iterator.next
        val resource = entry.getKey
        val alterConfigOps = entry.getValue
        assertEquals(ConfigResource.Type.CLIENT_METRICS, resource.`type`)
        assertEquals(3, alterConfigOps.size)

        val expectedConfigOps = List(
          new AlterConfigOp(new ConfigEntry("match", "client_software_name=kafka.python,client_software_version=1\\.2\\..*"), AlterConfigOp.OpType.SET),
          new AlterConfigOp(new ConfigEntry("metrics", "org.apache.kafka.consumer."), AlterConfigOp.OpType.SET),
          new AlterConfigOp(new ConfigEntry("interval.ms", ""), AlterConfigOp.OpType.DELETE)
        )
        assertEquals(expectedConfigOps, alterConfigOps.asScala.toList)
        alterResult
      }
    }
    ConfigCommand.alterConfig(mockAdminClient, alterOpts)
    verify(describeResult).all()
    verify(alterResult).all()
  }

  @Test
  def shouldDescribeClientMetricsConfigWithoutEntityName(): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-type", "client-metrics",
      "--describe"))

    val resourceCustom = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "1")
    val configEntry = new ConfigEntry("metrics", "*")
    val future = new KafkaFutureImpl[util.Map[ConfigResource, Config]]
    val describeResult: DescribeConfigsResult = mock(classOf[DescribeConfigsResult])
    when(describeResult.all()).thenReturn(future)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new MockAdminClient(util.Collections.singletonList(node), node) {
      override def describeConfigs(resources: util.Collection[ConfigResource], options: DescribeConfigsOptions): DescribeConfigsResult = {
        assertTrue(options.includeSynonyms())
        assertEquals(1, resources.size)
        val resource = resources.iterator.next
        assertEquals(ConfigResource.Type.CLIENT_METRICS, resource.`type`)
        assertTrue(resourceCustom.name == resource.name)
        future.complete(Map(resourceCustom -> new Config(util.Collections.singletonList(configEntry))).asJava)
        describeResult
      }
    }
    mockAdminClient.incrementalAlterConfigs(util.Collections.singletonMap(resourceCustom,
      util.Collections.singletonList(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET))), new AlterConfigsOptions())
    ConfigCommand.describeConfig(mockAdminClient, describeOpts)
    verify(describeResult).all()
  }

  @Test
  def shouldNotAlterClientMetricsConfigWithoutEntityName(): Unit = {
    val alterOpts = new ConfigCommandOptions(Array("--bootstrap-server", "localhost:9092",
      "--entity-type", "client-metrics",
      "--alter",
      "--add-config", "interval.ms=1000"))

    val exception = assertThrows(classOf[IllegalArgumentException], () => alterOpts.checkArgs())
    assertEquals("an entity name must be specified with --alter of client-metrics", exception.getMessage)
  }

  @Test
  def shouldNotSupportAlterClientMetricsWithZookeeperArg(): Unit = {
    val alterOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "sub",
      "--entity-type", "client-metrics",
      "--alter",
      "--add-config", "interval.ms=1000"))

    val exception = assertThrows(classOf[IllegalArgumentException], () => alterOpts.checkArgs())
    assertEquals("Invalid entity type client-metrics, the entity type must be one of users, brokers with a --zookeeper argument", exception.getMessage)
  }

  @Test
  def shouldNotSupportDescribeClientMetricsWithZookeeperArg(): Unit = {
    val describeOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "sub",
      "--entity-type", "client-metrics",
      "--describe"))

    val exception = assertThrows(classOf[IllegalArgumentException], () => describeOpts.checkArgs())
    assertEquals("Invalid entity type client-metrics, the entity type must be one of users, brokers with a --zookeeper argument", exception.getMessage)
  }

  @Test
  def shouldNotSupportAlterClientMetricsWithZookeeper(): Unit = {
    val alterOpts = new ConfigCommandOptions(Array("--zookeeper", zkConnect,
      "--entity-name", "sub",
      "--entity-type", "client-metrics",
      "--alter",
      "--add-config", "interval.ms=1000"))

    val exception = assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfigWithZk(null, alterOpts, dummyAdminZkClient))
    assertEquals("client-metrics is not a known entityType. Should be one of List(topics, clients, users, brokers, ips)", exception.getMessage)
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
