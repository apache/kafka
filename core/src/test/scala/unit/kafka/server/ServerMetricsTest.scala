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

import kafka.utils.TestUtils
import org.apache.kafka.common.metrics.Sensor
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class ServerMetricsTest {

  @Test
  def testMetricsConfig(): Unit = {
    val recordingLevels = List(Sensor.RecordingLevel.DEBUG, Sensor.RecordingLevel.INFO)
    val illegalNames = List("IllegalName", "")
    val props = TestUtils.createBrokerConfig(0, "localhost:2818")

    for (recordingLevel <- recordingLevels) {
      props.put(KafkaConfig.MetricRecordingLevelProp, recordingLevel.name)
      val config = KafkaConfig.fromProps(props)
      val metricConfig = Server.buildMetricsConfig(config)
      assertEquals(recordingLevel, metricConfig.recordLevel)
    }

    for (illegalName <- illegalNames) {
      props.put(KafkaConfig.MetricRecordingLevelProp, illegalName)
      val config = KafkaConfig.fromProps(props)
      assertThrows(classOf[IllegalArgumentException], () => Server.buildMetricsConfig(config))
    }

  }

}
