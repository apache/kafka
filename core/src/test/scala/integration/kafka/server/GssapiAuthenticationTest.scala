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

package kafka.server

import java.net.InetSocketAddress
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import kafka.api.{Both, IntegrationTestHarness, SaslSetup}
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.network._
import org.apache.kafka.common.security.{JaasContext, TestSecurityConfig}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class GssapiAuthenticationTest extends IntegrationTestHarness with SaslSetup {
  override val brokerCount = 1
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT

  private val kafkaClientSaslMechanism = "GSSAPI"
  private val kafkaServerSaslMechanisms = List("GSSAPI")

  private val numThreads = 10
  private val executor = Executors.newFixedThreadPool(numThreads)
  private val clientConfig: Properties = new Properties
  private var serverAddr: InetSocketAddress = _
  private val time = new MockTime(10)
  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  private val failedAuthenticationDelayMs = 2000

  @Before
  override def setUp() {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), Both))
    serverConfig.put(KafkaConfig.SslClientAuthProp, "required")
    serverConfig.put(KafkaConfig.FailedAuthenticationDelayMsProp, failedAuthenticationDelayMs.toString)
    super.setUp()
    serverAddr = new InetSocketAddress("localhost",
      servers.head.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)))

    clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
    clientConfig.put(SaslConfigs.SASL_MECHANISM, kafkaClientSaslMechanism)
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(kafkaClientSaslMechanism))
    clientConfig.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, "5000")

    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, brokerCount)
  }

  @After
  override def tearDown(): Unit = {
    executor.shutdownNow()
    super.tearDown()
    closeSasl()
  }

  /**
   * Tests that Kerberos replay error `Request is a replay (34)` is not handled as an authentication exception
   * since replay detection used to detect DoS attacks may occasionally reject valid concurrent requests.
   */
  @Test
  def testRequestIsAReplay(): Unit = {
    val successfulAuthsPerThread = 10
    val futures = (0 until numThreads).map(_ => executor.submit(new Runnable {
      override def run(): Unit = verifyRetriableFailuresDuringAuthentication(successfulAuthsPerThread)
    }))
    futures.foreach(_.get(60, TimeUnit.SECONDS))
    assertEquals(0, TestUtils.totalMetricValue(servers.head, "failed-authentication-total"))
    val successfulAuths = TestUtils.totalMetricValue(servers.head, "successful-authentication-total")
    assertTrue("Too few authentications: " + successfulAuths, successfulAuths > successfulAuthsPerThread * numThreads)
  }

  /**
   * Tests that Kerberos error `Server not found in Kerberos database (7)` is handled
   * as a fatal authentication failure.
   */
  @Test
  def testServerNotFoundInKerberosDatabase(): Unit = {
    val jaasConfig = clientConfig.getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    val invalidServiceConfig = jaasConfig.replace("serviceName=\"kafka\"", "serviceName=\"invalid-service\"")
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, invalidServiceConfig)
    clientConfig.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "invalid-service")
    verifyNonRetriableAuthenticationFailure()
  }

  /**
   * Test that when client fails to verify authenticity of the server, the resulting failed authentication exception
   * is thrown immediately, and is not affected by <code>connection.failed.authentication.delay.ms</code>.
   */
  @Test
  def testServerAuthenticationFailure(): Unit = {
    // Setup client with a non-existent service principal, so that server authentication fails on the client
    val clientLoginContext = jaasClientLoginModule(kafkaClientSaslMechanism, Some("another-kafka-service"))
    val configOverrides = new Properties()
    configOverrides.setProperty(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    val consumer = createConsumer(configOverrides = configOverrides)
    consumer.assign(List(tp).asJava)

    val startMs = System.currentTimeMillis()
    try {
      consumer.poll(Duration.ofMillis(50))
      fail()
    } catch {
      case _: SaslAuthenticationException =>
    }
    val endMs = System.currentTimeMillis()
    require(endMs - startMs < failedAuthenticationDelayMs, "Failed authentication must not be delayed on the client")
    consumer.close()
  }

  /**
   * Verifies that any exceptions during authentication with the current `clientConfig` are
   * notified with disconnect state `AUTHENTICATE` (and not `AUTHENTICATION_FAILED`). This
   * is to ensure that NetworkClient doesn't handle this as a fatal authentication failure,
   * but as a transient I/O exception. So Producer/Consumer/AdminClient will retry
   * any operation based on their configuration until timeout and will not propagate
   * the exception to the application.
   */
  private def verifyRetriableFailuresDuringAuthentication(numSuccessfulAuths: Int): Unit = {
    val selector = createSelector()
    try {
      var actualSuccessfulAuths = 0
      while (actualSuccessfulAuths < numSuccessfulAuths) {
        val nodeId = actualSuccessfulAuths.toString
        selector.connect(nodeId, serverAddr, 1024, 1024)
        TestUtils.waitUntilTrue(() => {
          selector.poll(100)
          val disconnectState = selector.disconnected().get(nodeId)
          // Verify that disconnect state is not AUTHENTICATION_FAILED
          if (disconnectState != null)
            assertEquals(s"Authentication failed with exception ${disconnectState.exception()}",
              ChannelState.State.AUTHENTICATE, disconnectState.state())
          selector.isChannelReady(nodeId) || disconnectState != null
        }, "Client not ready or disconnected within timeout")
        if (selector.isChannelReady(nodeId))
          actualSuccessfulAuths += 1
        selector.close(nodeId)
      }
    } finally {
      selector.close()
    }
  }

  /**
   * Verifies that authentication with the current `clientConfig` results in disconnection and that
   * the disconnection is notified with disconnect state `AUTHENTICATION_FAILED`. This is to ensure
   * that NetworkClient handles this as a fatal authentication failure that is propagated to
   * applications by Producer/Consumer/AdminClient without retrying and waiting for timeout.
   */
  private def verifyNonRetriableAuthenticationFailure(): Unit = {
    val selector = createSelector()
    val nodeId = "1"
    selector.connect(nodeId, serverAddr, 1024, 1024)
    TestUtils.waitUntilTrue(() => {
      selector.poll(100)
      val disconnectState = selector.disconnected().get(nodeId)
      if (disconnectState != null)
        assertEquals(ChannelState.State.AUTHENTICATION_FAILED, disconnectState.state())
      disconnectState != null
    }, "Client not disconnected within timeout")
  }

  private def createSelector(): Selector = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol,
      JaasContext.Type.CLIENT, new TestSecurityConfig(clientConfig), null, kafkaClientSaslMechanism, time, true)
    NetworkTestUtils.createSelector(channelBuilder, time)
  }
}
