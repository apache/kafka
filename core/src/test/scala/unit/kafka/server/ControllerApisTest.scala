package unit.kafka.server

import java.net.InetAddress
import java.util.{Properties, UUID}

import kafka.network.RequestChannel
import kafka.server.{ClientQuotaManager, ClientRequestQuotaManager, ControllerApis, ControllerMutationQuotaManager, KafkaConfig, MetaProperties, ReplicationQuotaManager}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.MockTime
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BrokerRegistrationRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.VersionRange
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, AuthorizationResult, Authorizer}
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Test}
import org.scalatest.Matchers.intercept

class ControllerApisTest {
  // Mocks
  private val brokerID = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val time = new MockTime
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = EasyMock.createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    clientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    None)
  private val controller: Controller = EasyMock.createNiceMock(classOf[Controller])

  private def createControllerApis(authorizer: Option[Authorizer],
                                   supportedFeatures: Map[String, VersionRange] = Map.empty): ControllerApis = {
    new ControllerApis(
      requestChannel,
      authorizer,
      quotas,
      time,
      supportedFeatures,
      controller,
      new KafkaConfig(new Properties()),
      new MetaProperties(UUID.fromString("a2c197eb-0789-442d-8e53-3478e58d0070"),
        UUID.fromString("7ace6e94-d39e-4a2f-a722-167eab2d0d9d"))
    )
  }

  /**
   * Build a RequestChannel.Request from the AbstractRequest
   *
   * @param request - AbstractRequest
   * @param listenerName - Default listener for the RequestChannel
   * @tparam T - Type of AbstractRequest
   * @return
   */
  private def buildRequest[T <: AbstractRequest](request: AbstractRequest,
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): RequestChannel.Request = {
    val buffer = request.serialize(new RequestHeader(request.api, request.version, clientID, 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics)
  }

  @Test
  def testBrokerRegistration(): Unit = {
    val brokerRegistrationRequest = new BrokerRegistrationRequest.Builder(
      new BrokerRegistrationRequestData()
        .setBrokerId(brokerID)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)

    val capturedResponse: Capture[Option[AbstractResponse]] = EasyMock.newCapture()
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.capture(capturedResponse),
      EasyMock.anyObject[Option[Send => Unit]]()
    ))

    val authorizer = Some[Authorizer](EasyMock.createNiceMock(classOf[Authorizer]))
    EasyMock.expect(authorizer.get.authorize(EasyMock.anyObject[AuthorizableRequestContext](), EasyMock.anyObject())).andAnswer(
      new IAnswer[java.util.List[AuthorizationResult]]() {
        override def answer(): java.util.List[AuthorizationResult] = {
          new java.util.ArrayList[AuthorizationResult](){
            add(AuthorizationResult.DENIED)
          }
        }
      }
    )
    EasyMock.replay(requestChannel, authorizer.get)

    val assertion = intercept[ClusterAuthorizationException] {
      createControllerApis(authorizer = authorizer).handleBrokerRegistration(request)
    }
    assert(Errors.forException(assertion) == Errors.CLUSTER_AUTHORIZATION_FAILED)
  }

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
