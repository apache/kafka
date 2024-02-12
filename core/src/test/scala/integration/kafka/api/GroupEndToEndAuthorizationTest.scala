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

import kafka.api.GroupEndToEndAuthorizationTest._
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, SaslAuthenticationContext}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder

object GroupEndToEndAuthorizationTest {
  val GroupPrincipalType = "Group"
  val ClientGroup = "testGroup"
  class GroupPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context match {
        case ctx: SaslAuthenticationContext =>
          if (ctx.server.getAuthorizationID == JaasTestUtils.KafkaScramUser)
            new KafkaPrincipal(GroupPrincipalType, ClientGroup)
          else
            new KafkaPrincipal(GroupPrincipalType, ctx.server.getAuthorizationID)
        case _ =>
          KafkaPrincipal.ANONYMOUS
      }
    }
  }
}

class GroupEndToEndAuthorizationTest extends SaslScramSslEndToEndAuthorizationTest {
  override val clientPrincipal = new KafkaPrincipal(GroupPrincipalType, ClientGroup)
  override val kafkaPrincipal = new KafkaPrincipal(GroupPrincipalType, JaasTestUtils.KafkaScramAdmin)
  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[GroupPrincipalBuilder].getName)
}
