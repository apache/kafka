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

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import junit.framework.Assert._
import kafka.utils.{ZkUtils, Utils, TestUtils}

class AdvertiseBrokerTest extends JUnit3Suite with ZooKeeperTestHarness {
  var server : KafkaServer = null
  val brokerId = 0
  val advertisedHostName = "routable-host"
  val advertisedPort = 1234

  override def setUp() {
    super.setUp()
    val props = TestUtils.createBrokerConfig(brokerId, TestUtils.choosePort())
    props.put("advertised.host.name", advertisedHostName)
    props.put("advertised.port", advertisedPort.toString)
    
    server = TestUtils.createServer(new KafkaConfig(props))
  }

  override def tearDown() {
    server.shutdown()
    Utils.rm(server.config.logDirs)
    super.tearDown()
  }
  
  def testBrokerAdvertiseToZK {
    val brokerInfo = ZkUtils.getBrokerInfo(zkClient, brokerId)
    assertEquals(advertisedHostName, brokerInfo.get.host)
    assertEquals(advertisedPort, brokerInfo.get.port)
  }
  
}