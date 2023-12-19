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
package kafka.docker

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class KafkaDockerWrapperTest {

  @Test
  def testGetToolsLog4jConfigsFromEnv(): Unit = {
    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")
    val expected = "\n" + "log4j.rootLogger=TRACE, stderr"
    val actual = KafkaDockerWrapper.getToolsLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testGetServerConfigsFromEnv(): Unit = {
    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE",
                      "KAFKA_VALID_PROPERTY" -> "Value",
                      "SOME_VARIABLE" -> "Some Value",
                      "KAFKA_VALID___PROPERTY__ALL_CASES" -> "All Cases Value")
    val expected = List("valid.property=Value", "valid-property_all.cases=All Cases Value")
    val actual = KafkaDockerWrapper.getServerConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }
}
