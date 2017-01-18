/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.integration

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Properties

import kafka.admin.BrokerApiVersionCommand
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

class BrokerApiVersionCommandTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.MinInSyncReplicasProp, "5")
  def generateConfigs() = TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  @Test
  def checkBrokerApiVersionCommandOutput() {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val printStream = new PrintStream(byteArrayOutputStream)
    BrokerApiVersionCommand.execute(Array("--bootstrap-server", brokerList), printStream)
    val content = new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8)
    val lineIter = content.split("\n").iterator
    assertTrue(lineIter.hasNext)
    assertEquals("%s (id: 0 rack: null) -> (".format(brokerList), lineIter.next)
    val nodeApiVersions = new NodeApiVersions(ApiVersionsResponse.API_VERSIONS_RESPONSE.apiVersions())
    for (i <- 0 to ApiKeys.MAX_API_KEY) {
      val apiKey = ApiKeys.forId(i)
      val apiVersion = nodeApiVersions.apiVersion(apiKey)
      val versionRangeStr = if (apiVersion.minVersion == apiVersion.maxVersion)
        "%d".format(apiVersion.minVersion: Int)
      else
        "%d to %d".format(apiVersion.minVersion: Int, apiVersion.maxVersion: Int)
      val terminator = if (i == ApiKeys.MAX_API_KEY) "" else ","
      val line = "\t%s(%d): %s [usable: %d]%s".format(apiKey.name, apiKey.id: Int, versionRangeStr,
          nodeApiVersions.usableVersion(apiKey), terminator)
      assertTrue(lineIter.hasNext)
      assertEquals(line, lineIter.next)
    }
    assertTrue(lineIter.hasNext)
    assertEquals(")", lineIter.next)
    assertFalse(lineIter.hasNext)
  }
}
