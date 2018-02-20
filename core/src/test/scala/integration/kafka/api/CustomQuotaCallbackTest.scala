/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import java.io.File
import java.{lang, util}
import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Properties}

import kafka.api.GroupedUserPrincipalBuilder._
import kafka.server._
import kafka.utils.JaasTestUtils.ScramLoginModule
import kafka.utils.{JaasTestUtils, Logging, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.{Cluster, Reconfigurable}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth._
import org.apache.kafka.server.quota._
import org.junit.{Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class CustomQuotaCallbackTest extends IntegrationTestHarness with SaslSetup {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected def listenerName = new ListenerName("CLIENT")
  override protected def interBrokerListenerName: ListenerName = new ListenerName("BROKER")

  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override val consumerCount: Int = 0
  override val producerCount: Int = 0
  override val serverCount: Int = 2

  private val kafkaServerSaslMechanisms = Seq("SCRAM-SHA-256")
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  private val adminClients = new ArrayBuffer[AdminClient]()

  val defaultRequestQuota = 1000
  val defaultProduceQuota = 2000 * 1000 * 1000
  val defaultConsumeQuota = 1000 * 1000 * 1000

  @Before
  override def setUp() {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some("SCRAM-SHA-256"), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    this.serverConfig.setProperty(KafkaConfig.ClientQuotaCallbackClassProp, classOf[GroupedUserQuotaCallback].getName)
    this.serverConfig.setProperty(s"${listenerName.configPrefix}${KafkaConfig.PrincipalBuilderClassProp}",
      classOf[GroupedUserPrincipalBuilder].getName)
    this.serverConfig.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
    super.setUp()
    brokerList = TestUtils.bootstrapServers(servers, listenerName)
  }

  override def configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)
  }

  @Test
  def testCustomQuotaCallback() {
    // Large quota override, should not throttle
    var brokerId = 0
    var user = createGroupWithOneUser("group0_user1", brokerId)
    user.configureAndWaitForQuota(1000000, 2000000)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)

    // Large quota updated to small quota, should throttle
    user.configureAndWaitForQuota(9000, 3000)
    user.produceConsume(expectProduceThrottle = true, expectConsumeThrottle = true)

    // Quota override deletion - verify default quota applied (large quota, no throttling)
    user = addUser("group0_user2", brokerId)
    user.removeQuotaOverrides()
    user.waitForQuotaUpdate(defaultProduceQuota, defaultConsumeQuota, defaultRequestQuota)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)

    // Make default quota smaller, should throttle
    user.configureAndWaitForQuota(8000, 2500, divisor = 1, group = None)
    user.produceConsume(expectProduceThrottle = true, expectConsumeThrottle = true)

    // Configure large quota override, should not throttle
    user = addUser("group0_user3", brokerId)
    user.configureAndWaitForQuota(2000000, 2000000)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)

    // Quota large enough for one partition, should not throttle
    brokerId = 1
    user = createGroupWithOneUser("group1_user1", brokerId)
    user.configureAndWaitForQuota(8000 * 100, 2500 * 100)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)

    // Create large number of partitions on another broker, should result in throttling on first partition
    val largeTopic = "group1_largeTopic"
    createTopic(largeTopic, numPartitions = 99, leader = 0)
    user.waitForQuotaUpdate(8000, 2500, defaultRequestQuota)
    user.produceConsume(expectProduceThrottle = true, expectConsumeThrottle = true)

    // Remove quota override and test default quota applied with scaling based on partitions
    user = addUser("group1_user2", brokerId)
    user.removeQuotaOverrides()
    user.waitForQuotaUpdate(defaultProduceQuota / 100, defaultConsumeQuota / 100, defaultRequestQuota)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)
    user.configureAndWaitForQuota(8000 * 100, 2500 * 100, divisor=100, group = None)
    user.produceConsume(expectProduceThrottle = true, expectConsumeThrottle = true)

    // Remove the second topic with large number of partitions, verify no longer throttled
    adminZkClient.deleteTopic(largeTopic)
    user = addUser("group1_user3", brokerId)
    user.waitForQuotaUpdate(8000 * 100, 2500 * 100, defaultRequestQuota)
    user.produceConsume(expectProduceThrottle = false, expectConsumeThrottle = false)

    // Alter configs of custom callback dynamically
    val adminClient = createAdminClient()
    val newProps = new Properties
    newProps.put(GroupedUserQuotaCallback.DefaultProduceQuotaProp, "8000")
    newProps.put(GroupedUserQuotaCallback.DefaultFetchQuotaProp, "2500")
    TestUtils.alterConfigs(servers, adminClient, newProps, perBrokerConfig = false)
    user.waitForQuotaUpdate(8000, 2500, defaultRequestQuota)
    user.produceConsume(expectProduceThrottle = true, expectConsumeThrottle = true)
  }

  private def createGroupWithOneUser(firstUser: String, brokerId: Int): GroupedUser = {
    val user = addUser(firstUser, brokerId)
    createTopic(user.topic, numPartitions = 1, brokerId)
    user.configureAndWaitForQuota(defaultProduceQuota, defaultConsumeQuota, divisor = 1, group = None)
    user
  }

  private def createTopic(topic: String, numPartitions: Int, leader: Int): Unit = {
    val assignment = (0 until numPartitions).map { i => i -> Seq(leader) }.toMap
    TestUtils.createTopic(zkClient, topic, assignment, servers)
  }

  private def createAdminClient(): AdminClient = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      TestUtils.bootstrapServers(servers, new ListenerName("BROKER")))
    clientSecurityProps("admin-client").asInstanceOf[util.Map[Object, Object]].asScala.foreach { case (key, value) =>
      config.put(key.toString, value)
    }
    config.put(SaslConfigs.SASL_JAAS_CONFIG,
      ScramLoginModule(JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword).toString)
    val adminClient = AdminClient.create(config)
    adminClients += adminClient
    adminClient
  }

  private def addUser(user: String, leader: Int): GroupedUser = {

    val password = s"$user:secret"
    createScramCredentials(zkConnect, user, password)

    val userGroup = group(user)
    val topic = s"${userGroup}_topic"
    val producerClientId = s"$user:producer-client-id"
    val consumerClientId = s"$user:producer-client-id"

    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, ScramLoginModule(user, password).toString)
    val producer = createNewProducer
    producers += producer

    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, s"$user-group")
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, ScramLoginModule(user, password).toString)
    val consumer = createNewConsumer
    consumers += consumer

    GroupedUser(user, userGroup, topic, servers(leader), producerClientId, consumerClientId, producer, consumer)
  }

  case class GroupedUser(user: String, userGroup: String, topic: String, leaderNode: KafkaServer,
                         producerClientId: String, consumerClientId: String,
                         producer: KafkaProducer[Array[Byte], Array[Byte]],
                         consumer: KafkaConsumer[Array[Byte], Array[Byte]]) extends
    QuotaTestClients(topic, leaderNode, producerClientId, consumerClientId, producer, consumer) {

    override def userPrincipal: KafkaPrincipal = GroupedUserPrincipal(user, userGroup)

    override def quotaMetricTags(clientId: String): Map[String, String] = {
      Map(GroupedUserQuotaCallback.QuotaGroupTag -> userGroup)
    }

    override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit = {
      configureQuota(userGroup, producerQuota, consumerQuota, requestQuota)
    }

    override def removeQuotaOverrides(): Unit = {
      adminZkClient.changeUserOrUserClientIdConfig(quotaEntityName(userGroup), new Properties)
    }

    def configureQuota(userGroup: String, producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit = {
      val quotaProps = quotaProperties(producerQuota, consumerQuota, requestQuota)
      adminZkClient.changeUserOrUserClientIdConfig(quotaEntityName(userGroup), quotaProps)
    }

    def configureAndWaitForQuota(produceQuota: Long, fetchQuota: Long, divisor: Int = 1,
                                 group: Option[String] = Some(userGroup)): Unit = {
      configureQuota(group.getOrElse(""), produceQuota, fetchQuota, defaultRequestQuota)
      waitForQuotaUpdate(produceQuota / divisor, fetchQuota / divisor, defaultRequestQuota)
    }

    def produceConsume(expectProduceThrottle: Boolean, expectConsumeThrottle: Boolean): Unit = {
      removeThrottleMetrics()
      val numRecords = 1000
      val produced = produceUntilThrottled(numRecords)
      verifyProduceThrottle(expectProduceThrottle)
      consumeUntilThrottled(produced)
      verifyConsumeThrottle(expectConsumeThrottle)
    }

    def removeThrottleMetrics(): Unit = {
      def removeSensors(quotaType: QuotaType, clientId: String): Unit = {
        val sensorSuffix = quotaMetricTags(clientId).values.mkString(":")
        leaderNode.metrics.removeSensor(s"${quotaType}ThrottleTime-$sensorSuffix")
        leaderNode.metrics.removeSensor(s"$quotaType-$sensorSuffix")
      }
      removeSensors(QuotaType.Produce, producerClientId)
      removeSensors(QuotaType.Fetch, consumerClientId)
      removeSensors(QuotaType.Request, producerClientId)
      removeSensors(QuotaType.Request, consumerClientId)
    }

    private def quotaEntityName(userGroup: String): String = s"${userGroup}_"
  }
}

object GroupedUserPrincipalBuilder {
  def group(str: String): String = {
    if (str.indexOf("_") <= 0)
      ""
    else
      str.substring(0, str.indexOf("_"))
  }
}

class GroupedUserPrincipalBuilder extends KafkaPrincipalBuilder {
  override def build(context: AuthenticationContext): KafkaPrincipal = {
    val securityProtocol = context.securityProtocol
    if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
      val user = context.asInstanceOf[SaslAuthenticationContext].server().getAuthorizationID
      val userGroup = group(user)
      if (userGroup.isEmpty)
        throw new SaslAuthenticationException(s"Unknown user $user")
      else
        GroupedUserPrincipal(user, userGroup)
    } else
      throw new IllegalStateException(s"Unexpected security protocol $securityProtocol")
  }
}

case class GroupedUserPrincipal(user: String, userGroup: String) extends KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)

object GroupedUserQuotaCallback {
  val QuotaGroupTag = "group"
  val DefaultProduceQuotaProp = "default.produce.quota"
  val DefaultFetchQuotaProp = "default.fetch.quota"
  val UnlimitedQuota = new ClientQuota(Long.MaxValue, Collections.emptyMap[String, String])
}

class GroupedUserQuotaCallback extends ClientQuotaCallback with Reconfigurable with Logging {
  import kafka.api.GroupedUserQuotaCallback._

  var brokerId: Int = -1

  val quotas = Map[ClientQuotaType, ConcurrentHashMap[String, Double]](
    ClientQuotaType.FETCH -> new ConcurrentHashMap[String, Double](),
    ClientQuotaType.PRODUCE -> new ConcurrentHashMap[String, Double](),
    ClientQuotaType.REQUEST -> new ConcurrentHashMap[String, Double]()
  )

  val partitionRatio = new ConcurrentHashMap[String, Double]()

  override def configure(configs: util.Map[String, _]): Unit = {
    brokerId = configs.get(KafkaConfig.BrokerIdProp).toString.toInt
  }

  override def reconfigurableConfigs(): util.Set[String] = {
    Set(DefaultProduceQuotaProp, DefaultFetchQuotaProp).asJava
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    reconfigurableConfigs.asScala.foreach(configValue(configs, _))
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    configValue(configs, DefaultProduceQuotaProp).foreach(value => quotas(ClientQuotaType.PRODUCE).put("", value))
    configValue(configs, DefaultFetchQuotaProp).foreach(value => quotas(ClientQuotaType.FETCH).put("", value))
  }

  private def configValue(configs: util.Map[String, _], key: String): Option[Long] = {
    val value = configs.get(key)
    if (value != null) Some(value.toString.toLong) else None
  }

  override def quota(principal: KafkaPrincipal, clientId: String, quotaType: ClientQuotaType): ClientQuota = {
    principal match {
      case groupPrincipal: GroupedUserPrincipal =>
        val userGroup = groupPrincipal.userGroup
        val quotaLimit = quotaOrDefault(userGroup, quotaType)
        if (quotaLimit != null)
          new ClientQuota(quotaLimit, Map(QuotaGroupTag -> userGroup).asJava)
        else
          UnlimitedQuota
      case _ =>
        UnlimitedQuota
    }
  }

  override def quotaLimit(metricTags: util.Map[String, String], quotaType: ClientQuotaType): lang.Double = {
    val group = metricTags.get(QuotaGroupTag)
    if (group != null) quotaOrDefault(group, quotaType) else null
  }

  override def updateClusterMetadata(cluster: Cluster): Boolean = {
    val topicsByGroup = cluster.topics.asScala.groupBy(group)

    !topicsByGroup.forall { case (group, groupTopics) =>
      val groupPartitions = groupTopics.flatMap(topic => cluster.partitionsForTopic(topic).asScala)
      val totalPartitions = groupPartitions.size
      val partitionsOnThisBroker = groupPartitions.count { p => p.leader != null && p.leader.id == brokerId }
      val multiplier = if (totalPartitions == 0)
        1
      else if (partitionsOnThisBroker == 0)
        1.0 / totalPartitions
      else
        partitionsOnThisBroker.toDouble / totalPartitions
      partitionRatio.put(group, multiplier) != multiplier
    }
  }

  override def updateQuota(quotaEntity: ClientQuotaEntity, quotaType: ClientQuotaType, newValue: Double): Boolean = {
    quotas(quotaType).put(userGroup(quotaEntity), newValue) != newValue
  }

  override def removeQuota(quotaEntity: ClientQuotaEntity, quotaType: ClientQuotaType): Boolean = {
    quotas(quotaType).remove(userGroup(quotaEntity)) != 0
  }

  def close(): Unit = {}

  private def userGroup(quotaEntity: ClientQuotaEntity): String = {
    val configEntity = quotaEntity.configEntities.get(0)
    if (configEntity.entityType == ClientQuotaEntity.ConfigEntityType.USER)
      group(configEntity.name)
    else
      throw new IllegalArgumentException(s"Config entity type ${configEntity.entityType} is not supported")
  }

  private def quotaOrDefault(group: String, quotaType: ClientQuotaType): lang.Double = {
    val quotaMap = quotas(quotaType)
    var quotaLimit: Any = quotaMap.get(group)
    if (quotaLimit == null)
      quotaLimit = quotaMap.get("")
    if (quotaLimit != null) scaledQuota(quotaType, group, quotaLimit.asInstanceOf[Double]) else null
  }

  private def scaledQuota(quotaType: ClientQuotaType, group: String, configuredQuota: Double): Double = {
    if (quotaType == ClientQuotaType.REQUEST)
      configuredQuota
    else {
      val multiplier = partitionRatio.get(group)
      if (multiplier <= 0.0) configuredQuota else configuredQuota * multiplier
    }
  }
}


