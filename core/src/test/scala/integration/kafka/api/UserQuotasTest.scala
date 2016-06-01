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

import kafka.server.{KafkaConfig, ClientQuotaManagerConfig}

import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.server.QuotaId

class UserQuotasTest extends BaseQuotasTest with SaslTestHarness {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override protected val zkSaslEnabled = false
  override protected val saslProperties = Some(kafkaSaslProperties(kafkaClientSaslMechanism, Some(kafkaServerSaslMechanisms)))

  override def quotaType: String = ClientQuotaManagerConfig.User
  override val clientPrincipal = "client"
  override val producerQuotaId = clientPrincipal
  override val consumerQuotaId = clientPrincipal
  override def changeQuota(quotaId: String, props: Properties) {
    AdminUtils.changeUserConfig(zkUtils, QuotaId.sanitize(quotaType, quotaId), props)
  }
}
