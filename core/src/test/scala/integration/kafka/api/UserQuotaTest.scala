/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import java.io.File
import java.util.Properties

import kafka.server.{ConfigEntityName, KafkaConfig, KafkaServer}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Sanitizer
import org.junit.{After, Before}

class UserQuotaTest extends BaseQuotaTest with SaslSetup {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  private val kafkaServerSaslMechanisms = Seq("GSSAPI")
  private val kafkaClientSaslMechanism = "GSSAPI"
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @Before
  override def setUp() {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some("GSSAPI"), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    super.setUp()
    val defaultProps = quotaTestClients.quotaProperties(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
    adminZkClient.changeUserOrUserClientIdConfig(ConfigEntityName.Default, defaultProps)
    quotaTestClients.waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  override def createQuotaTestClients(topic: String, leaderNode: KafkaServer): QuotaTestClients = {
    val producer = createProducer()
    val consumer = createConsumer()

    new QuotaTestClients(topic, leaderNode, producerClientId, consumerClientId, producer, consumer) {
      override val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaClientPrincipalUnqualifiedName2)
      override def quotaMetricTags(clientId: String): Map[String, String] = {
        Map("user" -> userPrincipal.getName, "client-id" -> "")
      }

      override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double) {
        val props = quotaProperties(producerQuota, consumerQuota, requestQuota)
        updateQuotaOverride(props)
      }

      override def removeQuotaOverrides() {
        val emptyProps = new Properties
        updateQuotaOverride(emptyProps)
        updateQuotaOverride(emptyProps)
      }

      private def updateQuotaOverride(properties: Properties) {
        adminZkClient.changeUserOrUserClientIdConfig(Sanitizer.sanitize(userPrincipal.getName), properties)
      }
    }
  }
}
