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
import kafka.server.{KafkaConfig, ConfigEntityName, QuotaId}
import kafka.utils.JaasTestUtils

import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Before

class UserQuotaTest extends BaseQuotaTest with SaslTestHarness {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override protected val zkSaslEnabled = false
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  override val userPrincipal = JaasTestUtils.KafkaClientPrincipalUnqualifiedName2
  override val producerQuotaId = QuotaId(Some(userPrincipal), None)
  override val consumerQuotaId = QuotaId(Some(userPrincipal), None)

  @Before
  override def setUp() {
    this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
    super.setUp()
    val defaultProps = quotaProperties(defaultProducerQuota, defaultConsumerQuota)
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, ConfigEntityName.Default, defaultProps)
    waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota)
  }

  override def overrideQuotas(producerQuota: Long, consumerQuota: Long) {
    val props = quotaProperties(producerQuota, consumerQuota)
    updateQuotaOverride(props)
  }

  override def removeQuotaOverrides() {
    val emptyProps = new Properties
    updateQuotaOverride(emptyProps)
    updateQuotaOverride(emptyProps)
  }

  private def updateQuotaOverride(properties: Properties) {
    AdminUtils.changeUserOrUserClientIdConfig(zkUtils, QuotaId.sanitize(userPrincipal), properties)
  }
}
