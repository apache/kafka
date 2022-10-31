/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.network

import kafka.server.{BaseRequestTest, Defaults, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.network.ListenerName
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.util.Properties
import scala.jdk.CollectionConverters._

class DynamicNumNetworkThreadsTest extends BaseRequestTest {

  override def brokerCount = 1

  val internal = "PLAINTEXT"
  val external = "EXTERNAL"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ListenersProp, s"$internal://localhost:0, $external://localhost:0")
    properties.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$internal:PLAINTEXT, $external:PLAINTEXT")
    properties.put(s"listener.name.${internal.toLowerCase}.${KafkaConfig.NumNetworkThreadsProp}", "2")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    TestUtils.createTopic(zkClient, "test", brokerCount, brokerCount, servers)
    assertEquals(2, getNumNetworkThreads(internal))
    assertEquals(Defaults.NumNetworkThreads, getNumNetworkThreads(external))
  }

  def getNumNetworkThreads(listener: String): Int = {
    brokers.head.metrics.metrics().keySet().asScala
      .filter(_.name() == "request-rate")
      .count(listener == _.tags().get("listener"))
  }

  @Test
  def testDynamicNumNetworkThreads(): Unit = {
    // Increase the base network thread count
    val newBaseNetworkThreadsCount = Defaults.NumNetworkThreads + 1
    var props = new Properties
    props.put(KafkaConfig.NumNetworkThreadsProp, newBaseNetworkThreadsCount.toString)
    reconfigureServers(props, (KafkaConfig.NumNetworkThreadsProp, newBaseNetworkThreadsCount.toString))

    // Only the external listener is changed
    assertEquals(2, getNumNetworkThreads(internal))
    assertEquals(newBaseNetworkThreadsCount, getNumNetworkThreads(external))

    // Increase the network thread count for internal
    val newInternalNetworkThreadsCount = 3
    props = new Properties
    props.put(s"listener.name.${internal.toLowerCase}.${KafkaConfig.NumNetworkThreadsProp}", newInternalNetworkThreadsCount.toString)
    reconfigureServers(props, (s"listener.name.${internal.toLowerCase}.${KafkaConfig.NumNetworkThreadsProp}", newInternalNetworkThreadsCount.toString))

    // The internal listener is changed
    assertEquals(newInternalNetworkThreadsCount, getNumNetworkThreads(internal))
    assertEquals(newBaseNetworkThreadsCount, getNumNetworkThreads(external))
  }

  private def reconfigureServers(newProps: Properties, aPropToVerify: (String, String)): Unit = {
    val adminClient = createAdminClient()
    TestUtils.incrementalAlterConfigs(servers, adminClient, newProps, perBrokerConfig = false).all.get()
    waitForConfigOnServer(aPropToVerify._1, aPropToVerify._2)
    adminClient.close()
  }

  private def createAdminClient(): Admin = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(securityProtocol.name))
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = Admin.create(config)
    adminClient
  }

  private def waitForConfigOnServer(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, servers.head.config.originals.get(propName))
    }
  }

}
