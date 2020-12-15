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

import java.util.Properties

import kafka.utils.MockTime
import org.junit.rules.Timeout
import org.junit.{Rule, Test}

class BrokerLifecycleManagerTest {
  private def configProperties = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    properties.setProperty(KafkaConfig.BrokerIdProp, "1")
    properties
  }

  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Test
  def testCreateAndClose(): Unit = {
    val config = new KafkaConfig(configProperties)
    val time = new MockTime(0, 0)
    val manager = new BrokerLifecycleManager(config, time, None)
    manager.close()
  }
}