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

package integration.kafka.server

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import kafka.api.{Both, IntegrationTestHarness, SaslSetup}
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.network._
import org.apache.kafka.common.security.{JaasContext, TestSecurityConfig}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{After, Before, Test}

class GssapiAuthenticationTest extends IntegrationTestHarness with SaslSetup {

  override val producerCount = 0
  override val consumerCount = 0
  override val serverCount = 1
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT

  private val kafkaClientSaslMechanism = "GSSAPI"
  private val kafkaServerSaslMechanisms = List("GSSAPI")

  private val numThreads = 10
  private val executor = Executors.newFixedThreadPool(numThreads)
  private val clientConfig: Properties = new Properties

  @Before
  override def setUp() {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), Both))
    super.setUp()

    clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
    clientConfig.put(SaslConfigs.SASL_MECHANISM, kafkaClientSaslMechanism)
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    clientConfig.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, "5000")
  }

  @After
  override def tearDown(): Unit = {
    executor.shutdownNow()
    super.tearDown()
    closeSasl()
  }

  @Test
  def testReplay(): Unit = {
    val numIterations = 10
    val futures = (0 until numThreads).map(_ => executor.submit(new Runnable {
      override def run(): Unit = authenticate(numIterations)
    }))
    futures.foreach(_.get(60, TimeUnit.SECONDS))
    assertEquals(0, TestUtils.totalMetricCount(servers.head, "failed-authentication-total"))
    val successfulAuths = TestUtils.totalMetricCount(servers.head, "successful-authentication-total")
    assertTrue("Too few authentications: " + successfulAuths, successfulAuths > numIterations * numThreads)
  }

  private def authenticate(numIterations: Int): Unit = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol,
      JaasContext.Type.CLIENT, new TestSecurityConfig(clientConfig), null, kafkaClientSaslMechanism, true)
    val serverAddr = new InetSocketAddress("localhost",
      servers.head.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)))

    val selector = NetworkTestUtils.createSelector(channelBuilder)
    try {
      var successfulAuths = 0
      while (successfulAuths < numIterations) {
        val nodeId = successfulAuths.toString
        selector.connect(nodeId, serverAddr, 1024, 1024)
        TestUtils.waitUntilTrue(() => {
          selector.poll(100)
          val disconnectState = selector.disconnected().get(nodeId)
          // Verify that disconnect state is not AUTHENTICATION_FAILED
          if (disconnectState != null)
            assertEquals(ChannelState.State.AUTHENTICATE, disconnectState.state())
          selector.isChannelReady(nodeId) || disconnectState != null
        }, "Client not ready or disconnected within timeout")
        if (selector.isChannelReady(nodeId))
          successfulAuths += 1
        selector.close(nodeId)
      }
    } finally {
      selector.close()
    }
  }
}
