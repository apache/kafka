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
package kafka.metrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import kafka.metrics.clientmetrics.CmClientInformation
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}

import java.util.regex.PatternSyntaxException

@Timeout(120)
class ClientInstanceSelectorTest {

  def createClientInstanceSelector(): CmClientInformation = {
    val client_instance_id = Uuid.randomUuid().toString
    val clientId = "testclient1"
    val softwareName = "Apache.Java"
    val softwareVersion = "89.2.0"
    val clientHostAddress = "1.2.3.4"
    val clientPort = "9092"
    CmClientInformation(client_instance_id, clientId, softwareName, softwareVersion, clientHostAddress, clientPort)
  }

  @Test
  def testClientMatchingPattern(): Unit = {
    val selector = createClientInstanceSelector()

    // Since we consider empty/missing client matching patterns is valid, make sure that they pass the check.
    assertTrue(selector.isMatched(Map.empty))
    assertTrue(selector.isMatched(null))

    // '*' is considered as invalid regex pattern
    assertFalse(selector.isMatched(Map("*" -> "*")))
    assertFalse(selector.isMatched(Map("*" -> "abc")))
    assertThrows(classOf[PatternSyntaxException],() =>
      selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "*")))
    assertThrows(classOf[PatternSyntaxException],() =>
      selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "******")))
    assertThrows(classOf[PatternSyntaxException],() =>
      selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "*Something")))

    // Make sure that pattern matching is anchored regex.
    assertFalse(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "O.Apache.Java", CLIENT_SOFTWARE_VERSION -> "8.1.*")))
    assertFalse(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apache.Java.D", CLIENT_SOFTWARE_VERSION -> "8.1.*")))

    // Negative tests -  unknown matching entries.
    assertFalse(selector.isMatched(Map("a" -> "b")))
    assertFalse(selector.isMatched(Map("software" -> "Java")))

    // Negative tests -- Wrong matching patterns
    assertFalse(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apache.Java", CLIENT_SOFTWARE_VERSION -> "8.1.*")))
    assertFalse(selector.isMatched(Map("AAA" -> "BBB", CLIENT_SOFTWARE_VERSION -> "89.2.0")))
    assertFalse(selector.isMatched(Map(CLIENT_SOFTWARE_VERSION -> "89.2.0", "AAA" -> "fff")))

    // Positive tests
    assertTrue(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apache.Java")))
    assertTrue(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apache.*", CLIENT_SOFTWARE_VERSION -> "89.2.0")))
    assertTrue(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apa.he.*a", CLIENT_SOFTWARE_VERSION -> "^8.*0")))
    assertTrue(selector.isMatched(Map(CLIENT_SOFTWARE_NAME -> "Apache.Java$", CLIENT_SOFTWARE_VERSION -> "8..2.*")))
  }

  @Test
  def testMatchingPatternParser(): Unit = {
    var res = CmClientInformation.parseMatchingPatterns(ClientMetricsTestUtils.defaultClientMatchPatters)
    assertTrue(res.size == 2)

    var patterns = List(s"${CLIENT_SOFTWARE_NAME}=*")
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns))

    patterns = List(s"${CLIENT_SOFTWARE_NAME}=*****")
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns))

    patterns = List("ABC=something")
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns))

    patterns = List(s"${CLIENT_SOFTWARE_NAME}=Java=1.4")
    res = CmClientInformation.parseMatchingPatterns(patterns)
    assertTrue(res.size == 1)
    val value = res.get(CLIENT_SOFTWARE_NAME).get
    assertTrue(value.equals("Java=1.4"))

    val patterns2 = ClientMetricsTestUtils.defaultClientMatchPatters ++ List("ABC=something")
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns2))
  }

}
