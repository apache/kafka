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

package kafka.zk

import javax.security.auth.login.Configuration
import kafka.utils.{ZkUtils, Logging, CoreUtils}
import org.junit.{After, Before}
import org.scalatest.junit.JUnitSuite
import org.apache.kafka.common.security.JaasUtils

trait ZooKeeperTestHarness extends JUnitSuite with Logging {
  var zookeeper: EmbeddedZookeeper = null
  var zkPort: Int = -1
  var zkUtils: ZkUtils = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000
  def zkConnect: String = "127.0.0.1:" + zkPort
  def confFile: String = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "")
  
  @Before
  def setUp() {
    zookeeper = new EmbeddedZookeeper()
    zkPort = zookeeper.port
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled())
  }

  @After
  def tearDown() {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())

    def isDown(): Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", zkPort, 3000)
        false
      } catch { case _: Throwable =>
        debug("Server is down")
        true
      }
    }

    Iterator.continually(isDown()).exists(identity)

    Configuration.setConfiguration(null)
  }

}
