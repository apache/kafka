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
package kafka.server

import java.util
import java.util.concurrent.atomic.AtomicReference
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsContext, MetricsReporter}
import org.apache.kafka.server.metrics.MetricConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


object KafkaMetricsReporterTest {
  val setupError = new AtomicReference[String]("")

  class MockMetricsReporter extends MetricsReporter {
    def init(metrics: util.List[KafkaMetric]): Unit = {}

    def metricChange(metric: KafkaMetric): Unit = {}

    def metricRemoval(metric: KafkaMetric): Unit = {}

    override def close(): Unit = {}

    override def contextChange(metricsContext: MetricsContext): Unit = {
      //read jmxPrefix

      MockMetricsReporter.JMXPREFIX.set(contextLabelOrNull("_namespace", metricsContext))
      MockMetricsReporter.CLUSTERID.set(contextLabelOrNull("kafka.cluster.id", metricsContext))
      MockMetricsReporter.BROKERID.set(contextLabelOrNull("kafka.broker.id", metricsContext))
      MockMetricsReporter.NODEID.set(contextLabelOrNull("kafka.node.id", metricsContext))
    }

    private def contextLabelOrNull(name: String, metricsContext: MetricsContext): String = {
      Option(metricsContext.contextLabels().get(name)).flatMap(v => Option(v)).orNull
    }

    override def configure(configs: util.Map[String, _]): Unit = {}
  }

  object MockMetricsReporter {
    val JMXPREFIX: AtomicReference[String] = new AtomicReference[String]
    val BROKERID : AtomicReference[String] = new AtomicReference[String]
    val NODEID : AtomicReference[String] = new AtomicReference[String]
    val CLUSTERID : AtomicReference[String] = new AtomicReference[String]
  }
}

class KafkaMetricsReporterTest extends QuorumTestHarness {
  var broker: KafkaBroker = _
  var config: KafkaConfig = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val props = TestUtils.createBrokerConfig(1, zkConnectOrNull)
    props.setProperty(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, "kafka.server.KafkaMetricsReporterTest$MockMetricsReporter")
    props.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "true")
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    config = KafkaConfig.fromProps(props)
    broker = createBroker(config, threadNamePrefix = Option(this.getClass.getName))
    broker.startup()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testMetricsContextNamespacePresent(quorum: String): Unit = {
    assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.CLUSTERID.get())
    if (isKRaftTest()) {
      assertNull(KafkaMetricsReporterTest.MockMetricsReporter.BROKERID.get())
      assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.NODEID.get())
    } else {
      assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.BROKERID.get())
      assertNull(KafkaMetricsReporterTest.MockMetricsReporter.NODEID.get())
    }
    assertNotNull(KafkaMetricsReporterTest.MockMetricsReporter.JMXPREFIX.get())

    broker.shutdown()
    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

  @AfterEach
  override def tearDown(): Unit = {
    broker.shutdown()
    CoreUtils.delete(config.logDirs)
    super.tearDown()
  }
}
