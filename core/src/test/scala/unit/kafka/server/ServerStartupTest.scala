/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils.ZkUtils
import kafka.utils.CoreUtils
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

class ServerStartupTest extends ZooKeeperTestHarness {

  @Test
  def testBrokerCreatesZKChroot {
    val brokerId = 0
    val zookeeperChroot = "/kafka-chroot-for-unittest"
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val zooKeeperConnect = props.get("zookeeper.connect")
    props.put("zookeeper.connect", zooKeeperConnect + zookeeperChroot)
    val server = TestUtils.createServer(KafkaConfig.fromProps(props))

    val pathExists = zkUtils.pathExists(zookeeperChroot)
    assertTrue(pathExists)

    server.shutdown()
    CoreUtils.delete(server.config.logDirs)
  }

  @Test
  def testConflictBrokerRegistration {
    // Try starting a broker with the a conflicting broker id.
    // This shouldn't affect the existing broker registration.

    val brokerId = 0
    val props1 = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(props1))
    val brokerRegistration = zkUtils.readData(ZkUtils.BrokerIdsPath + "/" + brokerId)._1

    val props2 = TestUtils.createBrokerConfig(brokerId, zkConnect)
    try {
      TestUtils.createServer(KafkaConfig.fromProps(props2))
      fail("Registering a broker with a conflicting id should fail")
    } catch {
      case e : RuntimeException =>
      // this is expected
    }

    // broker registration shouldn't change
    assertEquals(brokerRegistration, zkUtils.readData(ZkUtils.BrokerIdsPath + "/" + brokerId)._1)

    server1.shutdown()
    CoreUtils.delete(server1.config.logDirs)
  }

  @Test
  def testBrokerSelfAware {
    val brokerId = 0
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val server = TestUtils.createServer(KafkaConfig.fromProps(props))

    TestUtils.waitUntilTrue(() => server.metadataCache.getAliveBrokers.nonEmpty, "Wait for cache to update")
    assertEquals(1, server.metadataCache.getAliveBrokers.size)
    assertEquals(brokerId, server.metadataCache.getAliveBrokers.head.id)

    server.shutdown()
    CoreUtils.delete(server.config.logDirs)
  }

  @Test
  def testBrokerStateRunningAfterZK {
    val brokerId = 0
    val mockBrokerState = EasyMock.niceMock(classOf[kafka.server.BrokerState])

    class BrokerStateInterceptor() extends BrokerState {
      override def newState(newState: BrokerStates): Unit = {
        val brokers = zkUtils.getAllBrokersInCluster()
        assertEquals(1, brokers.size)
        assertEquals(brokerId, brokers.head.id)
      }
    }

    class MockKafkaServer(override val config: KafkaConfig, override val brokerState: BrokerState = mockBrokerState) extends KafkaServer(config) {}

    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val server = new MockKafkaServer(KafkaConfig.fromProps(props))

    EasyMock.expect(mockBrokerState.newState(RunningAsBroker)).andDelegateTo(new BrokerStateInterceptor).once()
    EasyMock.replay(mockBrokerState)

    server.startup()
    server.shutdown()
    CoreUtils.delete(server.config.logDirs)
  }
}
