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

package kafka.admin

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsResponse
import org.apache.kafka.network.SocketServerConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class BrokerApiVersionsCommandTest extends KafkaServerTestHarness {

  def generateConfigs: Seq[KafkaConfig] =
    if (isKRaftTest()) {
      TestUtils.createBrokerConfigs(1, null).map(props => {
        // Enable unstable api versions to be compatible with the new APIs under development,
        // maybe we can remove this after the new APIs is complete.
        props.setProperty(KafkaConfig.UnstableApiVersionsEnableProp, "true")
        props
      }).map(KafkaConfig.fromProps)
    } else {
      TestUtils.createBrokerConfigs(1, zkConnect).map(props => {
        // Configure control plane listener to make sure we have separate listeners from client,
        // in order to avoid returning Envelope API version.
        props.setProperty(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG, "CONTROLLER")
        props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        props.setProperty("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
        props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
        props.setProperty(KafkaConfig.UnstableApiVersionsEnableProp, "true")
        props
      }).map(KafkaConfig.fromProps)
    }

  @Timeout(120)
  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def checkBrokerApiVersionCommandOutput(quorum: String): Unit = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val printStream = new PrintStream(byteArrayOutputStream, false, StandardCharsets.UTF_8.name())
    BrokerApiVersionsCommand.execute(Array("--bootstrap-server", bootstrapServers()), printStream)
    val content = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val lineIter = content.split("\n").iterator
    assertTrue(lineIter.hasNext)
    assertEquals(s"${bootstrapServers()} (id: 0 rack: null) -> (", lineIter.next())
    val listenerType = if (isKRaftTest()) {
      ApiMessageType.ListenerType.BROKER
    } else {
      ApiMessageType.ListenerType.ZK_BROKER
    }
    val clientApis = ApiKeys.clientApis().asScala
    val nodeApiVersions = new NodeApiVersions(clientApis.map(ApiVersionsResponse.toApiVersion).asJava, Collections.emptyList(), false)
    for (apiKey <- clientApis) {
      val terminator = if (apiKey == clientApis.last) "" else ","
      if (apiKey.inScope(listenerType)) {
        val apiVersion = nodeApiVersions.apiVersion(apiKey)
        assertNotNull(apiVersion)

        val versionRangeStr =
          if (apiVersion.minVersion == apiVersion.maxVersion) apiVersion.minVersion.toString
          else s"${apiVersion.minVersion} to ${apiVersion.maxVersion}"
        val usableVersion = nodeApiVersions.latestUsableVersion(apiKey)

        val line =
          if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY) s"\t${apiKey.name}(${apiKey.id}): UNSUPPORTED$terminator"
          else s"\t${apiKey.name}(${apiKey.id}): $versionRangeStr [usable: $usableVersion]$terminator"
        assertTrue(lineIter.hasNext)
        assertEquals(line, lineIter.next())
      } else {
        val line = s"\t${apiKey.name}(${apiKey.id}): UNSUPPORTED$terminator"
        assertTrue(lineIter.hasNext)
        assertEquals(line, lineIter.next())
      }
    }
    assertTrue(lineIter.hasNext)
    assertEquals(")", lineIter.next())
    assertFalse(lineIter.hasNext)
  }
}
