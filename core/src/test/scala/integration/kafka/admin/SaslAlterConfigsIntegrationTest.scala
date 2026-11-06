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

package integration.kafka.admin

import kafka.api.{KafkaSasl, SaslSetup}
import kafka.security.authorizer.AclAuthorizer
import kafka.utils.{JaasTestUtils, TestInfoUtils, TestUtils}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

@Tag("integration")
class SaslAlterConfigsIntegrationTest extends BaseAlterConfigsIntegrationTest with SaslSetup {
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaPlainAdmin)

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // Static JAAS config: brokers authenticate as 'plain-admin', clients default to 'plain-user2'
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), KafkaSasl))
    superuserClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, jaasAdminLoginModule(kafkaClientSaslMechanism))

    if (TestInfoUtils.isKRaft(testInfo)) {
      this.serverConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString)
      this.controllerConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString + ";" + "User:ANONYMOUS")
      this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)
      this.controllerConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[StandardAuthorizer].getName)
    } else {
      this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[AclAuthorizer].getName)
      this.serverConfig.setProperty(AclAuthorizer.SuperUsersProp, kafkaPrincipal.toString)
    }

    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersWithoutAcls(quorum: String): Unit = {
    val adminConfig: Map[String, AnyRef] = Map(
      SaslConfigs.SASL_JAAS_CONFIG -> s"""${classOf[PlainLoginModule].getName} required username="${JaasTestUtils.KafkaPlainUser2}" password="${JaasTestUtils.KafkaPlainPassword2}";"""
    )
    checkAlterConfigs(adminConfig, classOf[ClusterAuthorizationException], "token", placeholder = true)
  }

}
