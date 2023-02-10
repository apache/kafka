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

import kafka.server._
import kafka.utils.TestUtils
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Sanitizer
import org.junit.jupiter.api.{BeforeEach, TestInfo}

class UserClientIdQuotaTest extends BaseQuotaTest {

  override protected def securityProtocol = SecurityProtocol.SSL
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))

  override def producerClientId = "QuotasTestProducer-!@#$%^&*()"
  override def consumerClientId = "QuotasTestConsumer-!@#$%^&*()"

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.serverConfig.setProperty(KafkaConfig.SslClientAuthProp, "required")
    super.setUp(testInfo)
    quotaTestClients.alterClientQuotas(
      quotaTestClients.clientQuotaAlteration(
        quotaTestClients.clientQuotaEntity(Some(QuotaTestClients.DefaultEntity), Some(QuotaTestClients.DefaultEntity)),
        Some(defaultProducerQuota), Some(defaultConsumerQuota), Some(defaultRequestQuota)
      )
    )
    quotaTestClients.waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
  }

  override def createQuotaTestClients(topic: String, leaderNode: KafkaBroker): QuotaTestClients = {
    val producer = createProducer()
    val consumer = createConsumer()
    val adminClient = createAdminClient()

    new QuotaTestClients(topic, leaderNode, producerClientId, consumerClientId, producer, consumer, adminClient) {
      override def userPrincipal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "O=A client,CN=localhost")

      override def quotaMetricTags(clientId: String): Map[String, String] = {
        Map("user" -> Sanitizer.sanitize(userPrincipal.getName), "client-id" -> clientId)
      }

      override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), Some(producerClientId)),
            Some(producerQuota), None, Some(requestQuota)
          ),
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), Some(consumerClientId)),
            None, Some(consumerQuota), Some(requestQuota)
          )
        )
      }

      override def removeQuotaOverrides(): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), Some(producerClientId)),
            None, None, None
          ),
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), Some(consumerClientId)),
            None, None, None
          )
        )
      }
    }
  }
}
