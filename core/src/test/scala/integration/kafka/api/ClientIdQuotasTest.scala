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

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, ClientQuotaManagerConfig}
import org.apache.kafka.common.security.auth.KafkaPrincipal

class ClientIdQuotasTest extends BaseQuotasTest {

  override def generateConfigs() = {
    FixedPortTestUtils.createBrokerConfigs(serverCount,
                                           zkConnect,
                                           enableControlledShutdown = false)
            .map(KafkaConfig.fromProps(_, this.serverConfig))
  }

  override def quotaType = ClientQuotaManagerConfig.ClientId
  override val clientPrincipal = KafkaPrincipal.ANONYMOUS.getName
  override val producerQuotaId = producerClientId
  override val consumerQuotaId = consumerClientId
  override def changeQuota(quotaId: String, props: Properties) {
    AdminUtils.changeClientIdConfig(zkUtils, quotaId, props)
  }
}
