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

import java.util.Collections

import kafka.api.{IntegrationTestHarness, KafkaSasl, SaslSetup}
import kafka.utils._
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Tests that there are no failed authentications during broker startup. This is to verify
 * that SCRAM credentials are loaded by brokers before client connections can be made.
 * For simplicity of testing, this test verifies authentications of controller connections.
 */
class ScramServerStartupTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 1

  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = Collections.singletonList("SCRAM-SHA-256").asScala

  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  override def configureSecurityBeforeServersStart(): Unit = {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    // Create credentials before starting brokers
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)

    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), KafkaSasl))
  }

  @Test
  def testAuthentications(): Unit = {
    val successfulAuths = TestUtils.totalMetricValue(servers.head, "successful-authentication-total")
    assertTrue(successfulAuths > 0, "No successful authentications")
    val failedAuths = TestUtils.totalMetricValue(servers.head, "failed-authentication-total")
    assertEquals(0, failedAuths)
  }
}
