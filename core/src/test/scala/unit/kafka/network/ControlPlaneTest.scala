package kafka.network

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class ControlPlaneTest extends KafkaServerTestHarness {
  val numNodes = 2
  val overridingProps = new Properties()
  val testedMetrics = List("ControlPlaneRequestQueueSize", "ControlPlaneNetworkProcessorIdlePercent", "ControlPlaneRequestHandlerIdlePercent")

  overridingProps.put(KafkaConfig.ControlPlaneListenerNameProp, "CONTROLLER")
  overridingProps.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:" + TestUtils.RandomPort + ",CONTROLLER://localhost:" + TestUtils.RandomPort)
  overridingProps.put(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic=true).map(KafkaConfig.fromProps(_, overridingProps))

  @Test
  def testCreatingTopicThroughControlPlane() {
    val topic = "test"
    createTopic(topic, 3, numNodes)

    for (s <- servers) {
      val controlPlaneProcessors = s.socketServer.controlPlaneProcessors.asScala
      assertTrue(s"There should be exactly one control plane network processor thread when ${KafkaConfig.ControlPlaneListenerNameProp} is set",
        controlPlaneProcessors.size == 1)
      controlPlaneProcessors.foreach {
        case (id, processor) =>
          assertTrue("The control plane network processor should have processed some request", processor.receivesProcessed > 0)
      }

      assertTrue(s"There should be exactly one control plane request handler thread when ${KafkaConfig.ControlPlaneListenerNameProp} is set",
        s.controlPlaneRequestHandlerPool != null && s.controlPlaneRequestHandlerPool.runnables.size == 1)
      s.controlPlaneRequestHandlerPool.runnables.foreach { requestHandler =>
        assertTrue("The control plane request handler thread should have processed some request", requestHandler.requestsHandled > 0)
      }
    }

    testedMetrics.foreach { metric => TestUtils.verifyMetricExistence(metric, true)}
  }
}
