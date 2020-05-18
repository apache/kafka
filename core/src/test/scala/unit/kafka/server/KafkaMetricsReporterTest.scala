package kafka.server

import java.util

import java.util.concurrent.atomic.AtomicReference

import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsContext, MetricsReporter}
import org.junit.Assert.{assertEquals}
import org.junit.{After, Before, Test}
import org.junit.Assert._


object KafkaMetricsReporterTest {
  val setupError = new AtomicReference[String]("")

  class MockMetricsReporter extends MetricsReporter {
    def init(metrics: util.List[KafkaMetric]): Unit = {}

    def metricChange(metric: KafkaMetric): Unit = {}

    def metricRemoval(metric: KafkaMetric): Unit = {}

    override def close(): Unit = {}

    override def contextChange(metricsContext: MetricsContext): Unit = {
      //read jmxPrefix

      MockMetricsReporter.JMXPREFIX.set(metricsContext.metadata().get("_namespace").toString)
      MockMetricsReporter.CLUSTERID.set(metricsContext.metadata().get("kafka.cluster.id").toString)
      MockMetricsReporter.BROKERID.set(metricsContext.metadata().get("kafka.broker.id").toString)
      MockMetricsReporter.testProp.set(metricsContext.metadata().get("foo.bar"))
    }

    override def configure(configs: util.Map[String, _]): Unit = {}

  }

  object MockMetricsReporter {
    val JMXPREFIX: AtomicReference[String] = new AtomicReference[String]
    val BROKERID : AtomicReference[String] = new AtomicReference[String]
    val CLUSTERID : AtomicReference[String] = new AtomicReference[String]
    val testProp :  AtomicReference[String] = new AtomicReference[String]
  }
}

class KafkaMetricsReporterTest extends ZooKeeperTestHarness {
  var server: KafkaServerStartable = null
  var config: KafkaConfig = null

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty(KafkaConfig.MetricReporterClassesProp, "kafka.server.KafkaMetricsReporterTest$MockMetricsReporter")
    props.setProperty("metrics.context.foo.bar", "test")
    props.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "true")
    props.setProperty(KafkaConfig.BrokerIdProp, "-1")
    config = KafkaConfig.fromProps(props)
    server = KafkaServerStartable.fromProps(props, threadNamePrefix = Option(this.getClass.getName))
    server.startup()
  }

  @Test
  def testMetricsContextNamespacePresent(): Unit = {
    assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.CLUSTERID)
    assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.BROKERID)
    assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.JMXPREFIX)
    assertEquals("kafka.server", KafkaMetricsReporterTest.MockMetricsReporter.JMXPREFIX.get())
    assertEquals("test", KafkaMetricsReporterTest.MockMetricsReporter.testProp.get())

    server.shutdown()
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @After
  override def tearDown(): Unit = {
    server.shutdown()
    CoreUtils.delete(config.logDirs)
    super.tearDown()
  }
}
