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

import java.util.concurrent.atomic.AtomicReference

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{CoreUtils, TestUtils, VerifiableProperties}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener}
import org.apache.kafka.test.MockMetricsReporter
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.apache.kafka.test.TestUtils.isValidClusterId

object KafkaMetricReporterClusterIdTest {

  class MockKafkaMetricsReporter extends KafkaMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockKafkaMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }

    override def init(props: VerifiableProperties): Unit = {
    }
  }

  object MockKafkaMetricsReporter {
    val CLUSTER_META = new AtomicReference[ClusterResource]
  }

  object MockBrokerMetricsReporter {
    val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
  }

  class MockBrokerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource) {
      MockBrokerMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }
  }

}

class KafkaMetricReporterClusterIdTest extends ZooKeeperTestHarness {
  var server: KafkaServerStartable = null
  var config: KafkaConfig = null

  @Before
  override def setUp() {
    super.setUp()
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty("kafka.metrics.reporters", "kafka.server.KafkaMetricReporterClusterIdTest$MockKafkaMetricsReporter")
    props.setProperty(KafkaConfig.MetricReporterClassesProp, "kafka.server.KafkaMetricReporterClusterIdTest$MockBrokerMetricsReporter")
    config = KafkaConfig.fromProps(props)
    server = KafkaServerStartable.fromProps(props)
    server.startup()
  }

  @Test
  def testClusterIdPresent() {
    assertNotNull(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META)
    isValidClusterId(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META.get().clusterId())

    assertNotNull(KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META)
    isValidClusterId(KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META.get().clusterId())

    assertEquals(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META.get().clusterId(),
      KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META.get().clusterId())
  }

  @After
  override def tearDown() {
    server.shutdown()
    CoreUtils.delete(config.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
    super.tearDown()
  }
}
