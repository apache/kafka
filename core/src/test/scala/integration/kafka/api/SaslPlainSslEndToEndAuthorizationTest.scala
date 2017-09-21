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

import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SaslAuthenticationContext}
import org.junit.Test

object SaslPlainSslEndToEndAuthorizationTest {
  class TestPrincipalBuilder extends KafkaPrincipalBuilder {

    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context match {
        case ctx: SaslAuthenticationContext =>
          ctx.server.getAuthorizationID match {
            case JaasTestUtils.KafkaPlainAdmin =>
              new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin")
            case JaasTestUtils.KafkaPlainUser =>
              new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user")
            case _ =>
              KafkaPrincipal.ANONYMOUS
          }
      }
    }
  }
}

class SaslPlainSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  import SaslPlainSslEndToEndAuthorizationTest.TestPrincipalBuilder

  this.serverConfig.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[TestPrincipalBuilder].getName)

  override protected def kafkaClientSaslMechanism = "PLAIN"
  override protected def kafkaServerSaslMechanisms = List("PLAIN")
  override val clientPrincipal = "user"
  override val kafkaPrincipal = "admin"

  /**
   * Checks that secure paths created by broker and acl paths created by AclCommand
   * have expected ACLs.
   */
  @Test
  def testAcls() {
    TestUtils.verifySecureZkAcls(zkUtils, 1)
  }
}
