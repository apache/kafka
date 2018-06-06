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
package integration.kafka.api

import java.io.File
import java.time.Duration

import kafka.api.{Both, IntegrationTestHarness, SaslSetup}
import kafka.server.KafkaConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class SaslGssapiSslServerAuthenticationFailureTest extends IntegrationTestHarness with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  def kafkaClientSaslMechanism = "GSSAPI"
  def kafkaServerSaslMechanisms = List("GSSAPI")

  override val producerCount = 0
  override val consumerCount = 1
  override val serverCount = 1

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val failedAuthenticationDelayMs = 2000

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), Both))
    val clientLoginContext = jaasClientLoginModule(kafkaClientSaslMechanism, Some("another-kafka-service"))
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    serverConfig.put(KafkaConfig.SslClientAuthProp, "required")
    serverConfig.put(KafkaConfig.FailedAuthenticationDelayMsProp, failedAuthenticationDelayMs.toString)

    super.setUp()
    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, serverCount)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @Test
  def testServerAuthenticationFailure(): Unit = {
    val consumer = consumers.head
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
  }
}
