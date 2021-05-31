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

import kafka.admin.AclCommand
import kafka.utils.JaasTestUtils
import org.apache.kafka.clients.admin.{Admin, CreateDelegationTokenOptions}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken

class DelegationTokenEndToEndAuthorizationWithOwnerTest extends DelegationTokenEndToEndAuthorizationTest {

  def createTokenArgs: Array[String] = Array("--authorizer-properties",
    s"zookeeper.connect=$zkConnect",
    s"--add",
    s"--user-principal=$clientPrincipal",
    s"--operation=CreateTokens",
    s"--allow-principal=$tokenRequesterPrincipal")

  override def createDelegationTokenOptions(): CreateDelegationTokenOptions = new CreateDelegationTokenOptions().owner(clientPrincipal)

  private val tokenRequesterPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramUser2)
  private val tokenRequesterPassword = JaasTestUtils.KafkaScramPassword2

  override def configureTokenAclsBeforeServersStart(): Unit = {
    super.configureTokenAclsBeforeServersStart()
    AclCommand.main(createTokenArgs)
  }

  override def createAdditionalCredentialsAfterServersStarted(): Unit = {
    super.createAdditionalCredentialsAfterServersStarted()
    createScramCredentialsViaPrivilegedAdminClient(tokenRequesterPrincipal.getName, tokenRequesterPassword)
  }

  override def assertToken(token: DelegationToken): Unit = {
    assertTokenOwner(clientPrincipal, token)
    assertTokenRequester(tokenRequesterPrincipal, token)
  }

  override def createTokenRequesterAdminClient(): Admin = {
    createScramAdminClient(kafkaClientSaslMechanism, tokenRequesterPrincipal.getName, tokenRequesterPassword)
  }
}
