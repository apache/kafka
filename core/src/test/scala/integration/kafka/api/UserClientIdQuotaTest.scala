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

import kafka.admin.AdminUtils
import kafka.server._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Sanitizer
import org.junit.Before

class UserClientIdQuotaTest extends BaseQuotaTest {

  override protected def securityProtocol = SecurityProtocol.SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override val userPrincipal = "O=A client,CN=localhost"
  override def producerClientId = "QuotasTestProducer-!@#$%^&*()"
  override def consumerClientId = "QuotasTestConsumer-!@#$%^&*()"
  override def producerQuotaId = QuotaId(Some(Sanitizer.sanitize(userPrincipal)), Some(producerClientId), Some(Sanitizer.sanitize(producerClientId)))
  override def consumerQuotaId = QuotaId(Some(Sanitizer.sanitize(userPrincipal)), Some(consumerClientId), Some(Sanitizer.sanitize(consumerClientId)))

  @Before
  override def setUp() {
    this.serverConfig.setProperty(KafkaConfig.SslClientAuthProp, "required")
    this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    super.setUp()
    val defaultProps = quotaProperties(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, ConfigEntityName.Default + "/clients/" + ConfigEntityName.Default, defaultProps)
    waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
  }

  override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double) {
    val producerProps = new Properties()
    producerProps.setProperty(DynamicConfig.Client.ProducerByteRateOverrideProp, producerQuota.toString)
    producerProps.setProperty(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    updateQuotaOverride(userPrincipal, producerClientId, producerProps)

    val consumerProps = new Properties()
    consumerProps.setProperty(DynamicConfig.Client.ConsumerByteRateOverrideProp, consumerQuota.toString)
    consumerProps.setProperty(DynamicConfig.Client.RequestPercentageOverrideProp, requestQuota.toString)
    updateQuotaOverride(userPrincipal, consumerClientId, consumerProps)
  }

  override def removeQuotaOverrides() {
    val emptyProps = new Properties
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, Sanitizer.sanitize(userPrincipal) + "/clients/" + Sanitizer.sanitize(producerClientId), emptyProps)
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, Sanitizer.sanitize(userPrincipal) + "/clients/" + Sanitizer.sanitize(consumerClientId), emptyProps)
  }

  private def updateQuotaOverride(userPrincipal: String, clientId: String, properties: Properties) {
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, Sanitizer.sanitize(userPrincipal) + "/clients/" + Sanitizer.sanitize(clientId), properties)
  }
}
