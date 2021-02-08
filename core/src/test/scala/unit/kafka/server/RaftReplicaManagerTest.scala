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

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{CachedConfigRepository, RaftMetadataCache}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.mock

class RaftReplicaManagerTest {
  var alterIsrManager: AlterIsrManager = _
  var config: KafkaConfig = _
  val configRepository = new CachedConfigRepository()
  val metrics = new Metrics
  var quotaManager: QuotaManagers = _
  val time = new MockTime

  @BeforeEach
  def setUp(): Unit = {
    alterIsrManager = mock(classOf[AlterIsrManager])
    config = KafkaConfig.fromProps({
      val nodeId = 1
      val props = TestUtils.createBrokerConfig(nodeId, "")
      props.put(KafkaConfig.ProcessRolesProp, "broker")
      props.put(KafkaConfig.NodeIdProp, nodeId.toString)
      props
    })
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
  }

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  def createRaftReplicaManager(): ReplicaManager = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    new RaftReplicaManager(config, metrics, time, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      new RaftMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository)
  }

  @Test
  def testRejectsZkConfig(): Unit = {
    assertThrows(classOf[IllegalStateException], () => {
      val zkConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, ""))
      val mockLogMgr = TestUtils.createLogManager(zkConfig.logDirs.map(new File(_)))
      new RaftReplicaManager(zkConfig, metrics, time, new MockScheduler(time), mockLogMgr,
        new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
        new RaftMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager, configRepository)
    })
  }
}
