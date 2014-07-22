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

package kafka.integration

import kafka.server._
import kafka.utils.{Utils, TestUtils}
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.common.KafkaException

/**
 * A test harness that brings up some number of broker nodes
 */
trait KafkaServerTestHarness extends JUnit3Suite with ZooKeeperTestHarness {

  val configs: List[KafkaConfig]
  var servers: List[KafkaServer] = null
  var brokerList: String = null

  override def setUp() {
    super.setUp
    if(configs.size <= 0)
      throw new KafkaException("Must suply at least one server config.")
    brokerList = TestUtils.getBrokerListStrFromConfigs(configs)
    servers = configs.map(TestUtils.createServer(_))
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => server.config.logDirs.map(Utils.rm(_)))
    super.tearDown
  }
}
