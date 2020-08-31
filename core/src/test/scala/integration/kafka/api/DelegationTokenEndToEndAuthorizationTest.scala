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
package kafka.api

import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.admin.{Admin, ScramCredentialInfo, UserScramCredentialAlteration, UserScramCredentialUpsertion, ScramMechanism => PublicScramMechanism}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

class DelegationTokenEndToEndAuthorizationTest extends EndToEndAuthorizationTest {

  val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  val kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramUser)
  private val clientPassword = JaasTestUtils.KafkaScramPassword

  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramAdmin)
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  this.serverConfig.setProperty(KafkaConfig.DelegationTokenMasterKeyProp, "testKey")

  override def configureSecurityBeforeServersStart(): Unit = {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    // Create broker admin credentials before starting brokers
    createScramCredentials(zkConnect, kafkaPrincipal.getName, kafkaPassword)
  }

  override def createPrivilegedAdminClient() = createScramAdminClient(kafkaClientSaslMechanism, kafkaPrincipal.getName, kafkaPassword)

  override def configureSecurityAfterServersStart(): Unit = {
    super.configureSecurityAfterServersStart()

    // create scram credential for user "scram-user"
    createScramCredentialsViaPrivilegedAdminClient(clientPrincipal.getName, clientPassword)
    waitForUserScramCredentialToAppearOnAllBrokers(clientPrincipal.getName, kafkaClientSaslMechanism)

    //create a token with "scram-user" credentials
    val token = createDelegationToken()

    // pass token to client jaas config
    val clientLoginContext = JaasTestUtils.tokenClientLoginModule(token.tokenInfo().tokenId(), token.hmacAsBase64String())
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    adminClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
  }

  @Test
  def testCannotCreateUserWithDelegationToken(): Unit = {
    val adminClient = Admin.create(adminClientConfig)
    try {
      val user = "user"
      val results = adminClient.alterUserScramCredentials(List[UserScramCredentialAlteration](
        new UserScramCredentialUpsertion(user, new ScramCredentialInfo(PublicScramMechanism.SCRAM_SHA_256, 4096), "password")).asJava)
      assertEquals(1, results.values.size)
      val future = results.values.get(user)
      future.get
      fail("Should not be able to alter SCRAM user credentials when authenticated with a delegation token")
    } catch {
      case e: Exception => {
        // expected
        val cause = e.getCause
        assertTrue(cause.isInstanceOf[UnsupportedByAuthenticationException])
        assertEquals("Altering User SCRAM credentials is not allowed when authenticating with a delegation token", cause.getMessage)
      }
    } finally {
      adminClient.close()
    }
  }

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), Both))
    super.setUp()
  }

  private def createDelegationToken(): DelegationToken = {
    val adminClient = createScramAdminClient(kafkaClientSaslMechanism, clientPrincipal.getName, clientPassword)
    try {
      val token = adminClient.createDelegationToken().delegationToken().get()
      //wait for token to reach all the brokers
      TestUtils.waitUntilTrue(() => servers.forall(server => !server.tokenCache.tokens().isEmpty),
        "Timed out waiting for token to propagate to all servers")
      token
    } finally {
      adminClient.close()
    }
  }
}
