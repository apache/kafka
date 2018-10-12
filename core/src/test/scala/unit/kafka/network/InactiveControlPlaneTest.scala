package kafka.network

import com.yammer.metrics.{Metrics => YammerMetrics}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class InactiveControlPlaneTest extends KafkaServerTestHarness {
  val numNodes = 2
  val testedMetrics = List("ControlPlaneRequestQueueSize", "ControlPlaneNetworkProcessorIdlePercent", "ControlPlaneRequestHandlerIdlePercent")

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic=true).map(KafkaConfig.fromProps(_))

  @Before
  override def setUp(): Unit = {
    TestUtils.cleanMetricsRegistry()
    super.setUp()
  }

  /**
    * the control plane metrics should not exist when the control.plane.listener.name is not set
    */
  @Test
  def testInactiveControlPlane(): Unit = {
    for (s <- servers) {
      val controlPlaneProcessors = s.socketServer.controlPlaneProcessors.asScala
      assertTrue(s"There should be no control plane network processor thread when ${KafkaConfig.ControlPlaneListenerNameProp} is not set",
        controlPlaneProcessors.isEmpty)

      assertTrue(s"There should be no control plane request handler thread when ${KafkaConfig.ControlPlaneListenerNameProp} is not set",
        s.controlPlaneRequestHandlerPool == null)
    }

    testedMetrics.foreach { metric => TestUtils.verifyMetricExistence(metric, false)}
  }

}
