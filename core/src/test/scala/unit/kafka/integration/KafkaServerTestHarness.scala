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

import java.util.Arrays
import scala.collection.mutable.Buffer
import kafka.server._
import kafka.utils.{Utils, TestUtils}
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.common.KafkaException
import kafka.utils.TestUtils

/**
 * A test harness that brings up some number of broker nodes
 */
trait KafkaServerTestHarness extends JUnit3Suite with ZooKeeperTestHarness {

  val configs: List[KafkaConfig]
  var servers: Buffer[KafkaServer] = null
  var brokerList: String = null
  var alive: Array[Boolean] = null
  
  def serverForId(id: Int) = servers.find(s => s.config.brokerId == id)
  
  def bootstrapUrl = configs.map(c => c.hostName + ":" + c.port).mkString(",")
  
  override def setUp() {
    super.setUp
    if(configs.size <= 0)
      throw new KafkaException("Must suply at least one server config.")
    brokerList = TestUtils.getBrokerListStrFromConfigs(configs)
    servers = configs.map(TestUtils.createServer(_)).toBuffer
    alive = new Array[Boolean](servers.length)
    Arrays.fill(alive, true)
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => server.config.logDirs.map(Utils.rm(_)))
    super.tearDown
  }
  
  /**
   * Pick a broker at random and kill it if it isn't already dead
   * Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(servers.length)
    if(alive(index)) {
      servers(index).shutdown()
      servers(index).awaitShutdown()
      alive(index) = false
    }
    index
  }
  
  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers() {
    for(i <- 0 until servers.length if !alive(i)) {
      servers(i).startup()
      alive(i) = true
    }
  }
}
