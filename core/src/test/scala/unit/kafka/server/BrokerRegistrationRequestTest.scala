package unit.kafka.server

import kafka.server.{BrokerToControllerChannelManager, ControllerNodeProvider, ControllerRequestCompletionHandler}
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.message.{BrokerRegistrationRequestData, BrokerRegistrationResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.concurrent.{CompletableFuture, TimeUnit, TimeoutException}

/**
 * This test simulates a broker registering with the KRaft quorum running with different MetadataVersions.
 */
@Timeout(120)
@Tag("integration")
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class BrokerRegistrationRequestTest {

  def brokerToControllerChannelManager(clusterInstance: ClusterInstance): BrokerToControllerChannelManager = {
    BrokerToControllerChannelManager(
      new ControllerNodeProvider() {
        override def get(): Option[Node] = Some(new Node(
          clusterInstance.anyControllerSocketServer().config.nodeId,
          "127.0.0.1",
          clusterInstance.anyControllerSocketServer().boundPort(clusterInstance.controllerListenerName().get()),
        ))

        override def listenerName: ListenerName = clusterInstance.controllerListenerName().get()

        override def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

        override def saslMechanism: String = ""
      },
      Time.SYSTEM,
      new Metrics(),
      clusterInstance.anyControllerSocketServer().config,
      "heartbeat",
      Some("heartbeat"),
      10000
    )
  }

  def sendAndRecieve(
    channelManager: BrokerToControllerChannelManager,
    req: BrokerRegistrationRequestData
  ): BrokerRegistrationResponseData = {
    val responseFuture = new CompletableFuture[BrokerRegistrationResponseData]()
    channelManager.sendRequest(new BrokerRegistrationRequest.Builder(req), new ControllerRequestCompletionHandler() {
      override def onTimeout(): Unit = responseFuture.completeExceptionally(new TimeoutException())

      override def onComplete(response: ClientResponse): Unit =
        responseFuture.complete(response.responseBody().asInstanceOf[BrokerRegistrationResponse].data())
    })
    responseFuture.get(30, TimeUnit.SECONDS)
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_4_IV0)
  def testRegisterZkWithKRaft34(clusterInstance: ClusterInstance): Unit = {
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()

      val resp1 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
        )
        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(true)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.NONE, Errors.forCode(resp1.errorCode()))

      val resp2 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
        )
        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(false)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.NONE, Errors.forCode(resp2.errorCode()))

      val resp3 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
        )
        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(true)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.UNSUPPORTED_VERSION, Errors.forCode(resp3.errorCode()))

      val resp4 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
        )
        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(false)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.UNSUPPORTED_VERSION, Errors.forCode(resp4.errorCode()))
    } finally {
      channelManager.shutdown()
    }
  }

  @ClusterTest(clusterType = Type.KRAFT, brokers = 0, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
  def testRegisterZkWithKRaft33(clusterInstance: ClusterInstance): Unit = {
    val channelManager = brokerToControllerChannelManager(clusterInstance)
    try {
      channelManager.start()
      val resp1 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
        )

        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(true)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.BROKER_ID_NOT_REGISTERED, Errors.forCode(resp1.errorCode()))

      val resp2 = {
        val features = new BrokerRegistrationRequestData.FeatureCollection()
        features.add(new BrokerRegistrationRequestData.Feature()
          .setName(MetadataVersion.FEATURE_NAME)
          .setMinSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
          .setMaxSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
        )

        val req1 = new BrokerRegistrationRequestData()
          .setBrokerId(100)
          .setClusterId(clusterInstance.clusterId())
          .setIncarnationId(Uuid.randomUuid())
          .setIsMigratingZkBroker(true)
          .setFeatures(features)

        sendAndRecieve(channelManager, req1)
      }
      assertEquals(Errors.BROKER_ID_NOT_REGISTERED, Errors.forCode(resp2.errorCode()))
    } finally {
      channelManager.shutdown()
    }
  }
}
