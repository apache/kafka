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

import kafka.utils.JaasTestUtils
import org.apache.kafka.common.security.auth._
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs

object SaslOAuthBearerSslEndToEndAuthorizationTest {
  class TestControllerPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaOAuthBearerAdmin)
    }
  }
}

class SaslOAuthBearerSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  import SaslOAuthBearerSslEndToEndAuthorizationTest._

  this.controllerConfig.setProperty("listener.name.controller." + BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
    classOf[TestControllerPrincipalBuilder].getName)

  override protected def kafkaClientSaslMechanism = "OAUTHBEARER"
  override protected def kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaOAuthBearerUser)
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaOAuthBearerAdmin)
}
