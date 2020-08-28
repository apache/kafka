package kafka.admin

import java.util.concurrent.TimeUnit
import kafka.server.{KafkaServer, KafkaConfig}
import java.util.{Collections, Properties}
import org.apache.kafka.common.config.ConfigResource
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClient => JAdminClient}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.junit.{After, Before, Test}
import org.apache.kafka.common.network.ListenerName

import org.junit.Assert._
import org.apache.kafka.clients.admin.{AlterConfigOp, AlterConfigsOptions, ConfigEntry, DescribeConfigsOptions, AdminClientConfig}
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import scala.collection.Map
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class DynamicClientConfigTest extends ZooKeeperTestHarness {

  var servers = Seq.empty[KafkaServer]

  val timeoutConfig = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG
  val heartbeatConfig = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG
  val acksConfig = ProducerConfig.ACKS_CONFIG

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val brokerConfigs = TestUtils.createBrokerConfigs(4, zkConnect, enableControlledShutdown = true)
    servers = brokerConfigs.map { config =>
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  /**
   * Test altering and describing client configs with admin client
   */
  @Test
  def testAlterAndDescribeWithAdminClient(): Unit = {
    TestUtils.resource(JAdminClient.create(createAdminConfig(servers).asJava)) { client =>

      val userEntity = "alice"
      val userClientIdEntity = "alice:client-1"

      val configResource = new ConfigResource(ConfigResource.Type.USER_CLIENT, userClientIdEntity)
      val defaultConfigResource = new ConfigResource(ConfigResource.Type.USER_CLIENT, userEntity)
      val configProps = new Properties()
      val defaultConfigProps = new Properties()

      configProps.setProperty(acksConfig, "-1")
      configProps.setProperty(timeoutConfig, "8000")

      defaultConfigProps.setProperty(acksConfig, "0")
      defaultConfigProps.setProperty(timeoutConfig, "10000")
      defaultConfigProps.setProperty(heartbeatConfig, "4000")

      // Set configs with admin client for <user, default> and <user, client-id>
      alterConfig(client, configResource, configProps.asScala, Seq.empty)
      alterConfig(client, defaultConfigResource, defaultConfigProps.asScala, Seq.empty)

      // Describe configs with admin client
      var configsDescription = describeConfig(client, configResource, true)
      var defaultConfigsDescription = describeConfig(client, defaultConfigResource, true)

      // Verify <user, client-id> config description
      verifyConfigs(configsDescription, configProps, defaultConfigProps)

      // Verify <user, default> config description
      verifyConfigs(defaultConfigsDescription, new Properties, defaultConfigProps)

      // Delete and set multiple configs
      configProps.remove(timeoutConfig)
      configProps.setProperty(acksConfig, "1")

      defaultConfigProps.remove(heartbeatConfig)
      defaultConfigProps.remove(timeoutConfig)
      defaultConfigProps.setProperty(acksConfig, "0")

      // Make changes using admin client
      alterConfig(client, configResource, configProps.asScala, Seq(timeoutConfig))
      alterConfig(client, defaultConfigResource, defaultConfigProps.asScala, Seq(heartbeatConfig, timeoutConfig))

      // Describe again
      configsDescription = describeConfig(client, configResource, true)
      defaultConfigsDescription = describeConfig(client, defaultConfigResource, true)

      // Verify <user, client-id> config description
      verifyConfigs(configsDescription, configProps, defaultConfigProps)

      // Verify <user, default> config description
      verifyConfigs(defaultConfigsDescription, new Properties, defaultConfigProps)
    }
  }

  def verifyConfigs(entries: Seq[ConfigEntry], expectedConfigs: Properties, defaultExpectedConfigs: Properties): Unit = {
    for (entry <- entries) {
      if (entry.source == ConfigSource.DYNAMIC_CLIENT_CONFIG) {
        assertTrue(expectedConfigs.containsKey(entry.name) && (expectedConfigs.getProperty(entry.name) == entry.value))
      } else {
        assertTrue(defaultExpectedConfigs.containsKey(entry.name) && (defaultExpectedConfigs.getProperty(entry.name) == entry.value))
      }
      verifySynonyms(entry.synonyms.asScala.toList, expectedConfigs, defaultExpectedConfigs)
    }
  }

  def verifySynonyms(synonyms: List[ConfigSynonym], expectedConfigs: Properties, defaultExpectedConfigs: Properties): Unit = {
    for (synonym <- synonyms) {
      if (synonym.source == ConfigSource.DYNAMIC_CLIENT_CONFIG) {
        verifySynonym(synonym, expectedConfigs)
      } else {
        verifySynonym(synonym, defaultExpectedConfigs)
      }
    }
  }

  def verifySynonym(synonym: ConfigSynonym, configProps: Properties): Unit = {
    assertTrue(configProps.getProperty(synonym.name) == synonym.value)
  }

  def describeConfig(client: Admin, configResource: ConfigResource, includeSynonyms: Boolean = true): Seq[ConfigEntry] = {
    val describeOptions = new DescribeConfigsOptions().includeSynonyms(includeSynonyms)
    val configsDesc = client.describeConfigs(Collections.singleton(configResource), describeOptions)
      .all.get(30, TimeUnit.SECONDS)
    configsDesc.get(configResource).entries.asScala.toSeq
  }

  def alterConfig(client: Admin, configResource: ConfigResource, configsToBeAddedMap: Map[String, String], configsToBeDeleted: Seq[String]): Unit = {
      val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
      val configsToBeAdded = configsToBeAddedMap.map { case (k, v) => (k, new ConfigEntry(k, v)) }
      val alterEntries = (configsToBeAdded.values.map(new AlterConfigOp(_, AlterConfigOp.OpType.SET))
        ++ configsToBeDeleted.map { k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE) }
      ).asJavaCollection
      client.incrementalAlterConfigs(Map(configResource -> alterEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)
  }

  def createAdminConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def bootstrapServers(servers: Seq[KafkaServer]): String = {
    servers.map { server =>
      val port = server.socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
      s"localhost:$port"
    }.headOption.mkString(",")
  }
}
