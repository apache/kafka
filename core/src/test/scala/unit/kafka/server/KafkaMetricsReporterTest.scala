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

      MockMetricsReporter.JMXPREFIX.set(metricsContext.contextLabels().get("_namespace").toString)
      MockMetricsReporter.CLUSTERID.set(metricsContext.contextLabels().get("kafka.cluster.id").toString)
      MockMetricsReporter.BROKERID.set(metricsContext.contextLabels().get("kafka.broker.id").toString)
    }

    override def configure(configs: util.Map[String, _]): Unit = {}

  }

  object MockMetricsReporter {
    val JMXPREFIX: AtomicReference[String] = new AtomicReference[String]
    val BROKERID : AtomicReference[String] = new AtomicReference[String]
    val CLUSTERID : AtomicReference[String] = new AtomicReference[String]
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
