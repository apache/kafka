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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import scala.collection.Seq
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

import scala.jdk.CollectionConverters._

class BrokerApiVersionsCommandTest extends KafkaServerTestHarness {

  def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(1, zkConnect).map(props => {
      // Configure control plane listener to make sure we have separate listeners from client,
      // in order to avoid returning Envelope API version.
      props.setProperty(KafkaConfig.ControlPlaneListenerNameProp, "CONTROLLER")
      props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      props.setProperty("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
      props.setProperty(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
      props
    }).map(KafkaConfig.fromProps)

  @Timeout(120)
  @Test
  def checkBrokerApiVersionCommandOutput(): Unit = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val printStream = new PrintStream(byteArrayOutputStream, false, StandardCharsets.UTF_8.name())
    BrokerApiVersionsCommand.execute(Array("--bootstrap-server", bootstrapServers()), printStream)
    val content = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val lineIter = content.split("\n").iterator
    assertTrue(lineIter.hasNext)
    assertEquals(s"${bootstrapServers()} (id: 0 rack: null) -> (", lineIter.next())
    val nodeApiVersions = NodeApiVersions.create
    val enabledApis = ApiKeys.zkBrokerApis.asScala
    for (apiKey <- enabledApis) {
      val apiVersion = nodeApiVersions.apiVersion(apiKey)
      assertNotNull(apiVersion)

      val versionRangeStr =
        if (apiVersion.minVersion == apiVersion.maxVersion) apiVersion.minVersion.toString
        else s"${apiVersion.minVersion} to ${apiVersion.maxVersion}"
      val usableVersion = nodeApiVersions.latestUsableVersion(apiKey)

      val terminator = if (apiKey == enabledApis.last) "" else ","

      val line = s"\t${apiKey.name}(${apiKey.id}): $versionRangeStr [usable: $usableVersion]$terminator"
      assertTrue(lineIter.hasNext)
      assertEquals(line, lineIter.next())
    }
    assertTrue(lineIter.hasNext)
    assertEquals(")", lineIter.next())
    assertFalse(lineIter.hasNext)
  }
}
