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

import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.ClusterTest
import org.apache.kafka.common.test.api.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class BrokerMetricNamesTest(cluster: ClusterInstance) {
  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @ClusterTest
  def testMetrics(): Unit = {
    checkReplicaManagerMetrics()
  }

  def checkReplicaManagerMetrics(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    val expectedPrefix = "kafka.server:type=ReplicaManager,name"
    val expectedMetricNames = Set(
      "LeaderCount", "PartitionCount", "OfflineReplicaCount", "UnderReplicatedPartitions",
      "UnderMinIsrPartitionCount", "AtMinIsrPartitionCount", "ReassigningPartitions",
      "IsrExpandsPerSec", "IsrShrinksPerSec", "FailedIsrUpdatesPerSec",
      "ProducerIdCount",
    )
    expectedMetricNames.foreach { metricName =>
      assertEquals(1, metrics.keySet.asScala.count(_.getMBeanName == s"$expectedPrefix=$metricName"))
    }
  }
}
