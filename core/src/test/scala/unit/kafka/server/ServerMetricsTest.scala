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

import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.metrics.Sensor
import org.junit.Assert._
import org.junit.{Before, Test}

class ServerMetricsTest extends ZooKeeperTestHarness {

  @Before
  override def setUp() {
    super.setUp()
  }

  @Test
  def testMetricsConfig(){
    val names = List(Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.INFO.toString())
    val illegalNames = List("IllegalName", "")
    val props = TestUtils.createBrokerConfig(0, zkConnect)

    for (name <- names) {
      props.put(KafkaConfig.MetricRecordingLevelProp, name)
      val config = KafkaConfig.fromProps(props)
      val server = new KafkaServer(config)
      server.startup()
      assertEquals(server.metrics.config().recordLevel(), Sensor.RecordingLevel.forName(name))
      server.shutdown()
      CoreUtils.delete(server.config.logDirs)
    }

    for (illegalName <- illegalNames) {
      intercept[IllegalArgumentException] {
        props.put(KafkaConfig.MetricRecordingLevelProp, illegalName)
        val config = KafkaConfig.fromProps(props)
        val server = new KafkaServer(config)
      }
    }
  }

}