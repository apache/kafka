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

import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{CoreUtils, TestUtils, VerifiableProperties}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener}
import org.junit.Assert._
import org.junit.{Before, Test}

class MockKafkaMetricsReporter extends KafkaMetricsReporter with ClusterResourceListener {

  override def onUpdate(clusterMetadata: ClusterResource): Unit = {
    MockKafkaMetricsReporter.CLUSTER_META.set(clusterMetadata)
  }

  override def init(props: VerifiableProperties): Unit = {
  }
}

object MockKafkaMetricsReporter {
  val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
}

class KafkaMetricReporterClusterIdTest extends ZooKeeperTestHarness {
  var props1: Properties = null
  var config1: KafkaConfig = null

  @Before
  override def setUp() {
    super.setUp()
    props1 = TestUtils.createBrokerConfig(1, zkConnect)
    props1.setProperty("kafka.metrics.reporters", "kafka.server.MockKafkaMetricsReporter")
    config1 = KafkaConfig.fromProps(props1)
  }

  @Test
  def testClusterIdPresent() {
    val server1 = KafkaServerStartable.fromProps(props1)
    server1.startup()

    // Make sure the cluster id is 48 characters long (base64 of UUID.randomUUID() )
    assertNotNull(MockKafkaMetricsReporter.CLUSTER_META)
    assertEquals(48, MockKafkaMetricsReporter.CLUSTER_META.get().clusterId().length())

    server1.shutdown()

    CoreUtils.delete(config1.logDirs)
    TestUtils.verifyNonDaemonThreadsStatus(this.getClass.getName)
  }
}
