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

import kafka.server.KafkaServer
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.jupiter.api.BeforeEach

class ClientIdQuotaTest extends BaseQuotaTest {

  override def producerClientId = "QuotasTestProducer-!@#$%^&*()"
  override def consumerClientId = "QuotasTestConsumer-!@#$%^&*()"

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    quotaTestClients.alterClientQuotas(
      quotaTestClients.clientQuotaAlteration(
        quotaTestClients.clientQuotaEntity(None, Some(QuotaTestClients.DefaultEntity)),
        Some(defaultProducerQuota), Some(defaultConsumerQuota), Some(defaultRequestQuota)
      )
    )
    quotaTestClients.waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
  }

  override def createQuotaTestClients(topic: String, leaderNode: KafkaServer): QuotaTestClients = {
    val producer = createProducer()
    val consumer = createConsumer()
    val adminClient = createAdminClient()

    new QuotaTestClients(topic, leaderNode, producerClientId, consumerClientId, producer, consumer, adminClient) {
      override def userPrincipal: KafkaPrincipal = KafkaPrincipal.ANONYMOUS

      override def quotaMetricTags(clientId: String): Map[String, String] = {
        Map("user" -> "", "client-id" -> clientId)
      }

      override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(None, Some(producerClientId)),
            Some(producerQuota), None, Some(requestQuota)
          ),
          clientQuotaAlteration(
            clientQuotaEntity(None, Some(consumerClientId)),
            None, Some(consumerQuota), Some(requestQuota)
          )
        )
      }

      override def removeQuotaOverrides(): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(None, Some(producerClientId)),
            None, None, None
          ),
          clientQuotaAlteration(
            clientQuotaEntity(None, Some(consumerClientId)),
            None, None, None
          )
        )
      }
    }
  }
}
