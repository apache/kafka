package unit.kafka.server

import org.apache.kafka.common.metrics.Metrics
import kafka.coordinator.group.GroupCoordinator
import kafka.controller.KafkaController
import kafka.network.RequestChannel
import org.easymock.EasyMock._
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.api.ApiVersion
import org.apache.kafka.server.authorizer.Authorizer
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{AdminManager, BrokerTopicStats, ClientQuotaManager, ClientRequestQuotaManager, ControllerMutationQuotaManager, FetchManager, KafkaApis, KafkaConfig, MetadataCache, ReplicaManager, ReplicationQuotaManager}
import org.apache.kafka.server.auditor.Auditor
import org.junit.{After, Test}

class KafkaApisAuditorTest {

  private val requestChannel: RequestChannel = createNiceMock(classOf[RequestChannel])
//  private val requestChannelMetrics: RequestChannel.Metrics = createNiceMock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = createNiceMock(classOf[GroupCoordinator])
  private val adminManager: AdminManager = createNiceMock(classOf[AdminManager])
  private val txnCoordinator: TransactionCoordinator = createNiceMock(classOf[TransactionCoordinator])
  private val controller: KafkaController = createNiceMock(classOf[KafkaController])
  private val zkClient: KafkaZkClient = createNiceMock(classOf[KafkaZkClient])
  private val auditor: Auditor = createNiceMock(classOf[Auditor])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val metadataCache = new MetadataCache(brokerId)
  private val clientQuotaManager: ClientQuotaManager = createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = createNiceMock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
//  private val clientId = ""

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion,
                      authorizer: Option[Authorizer] = None): KafkaApis = {
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocolVersion.toString)
    properties.put(KafkaConfig.LogMessageFormatVersionProp, interBrokerProtocolVersion.toString)
    new KafkaApis(requestChannel,
      replicaManager,
      adminManager,
      groupCoordinator,
      txnCoordinator,
      controller,
      zkClient,
      brokerId,
      new KafkaConfig(properties),
      metadataCache,
      metrics,
      authorizer,
      quotas,
      fetchManager,
      brokerTopicStats,
      clusterId,
      time,
      null,
      List(auditor)
    )
  }

  @Test
  def testHandleCreateTopicsRequest(): Unit = {
    expect(controller.isActive).andReturn(false)
//    expect(auditor.audit(anyObject(), anyObject(), anyObject())).andStubAnswer()
  }
}
