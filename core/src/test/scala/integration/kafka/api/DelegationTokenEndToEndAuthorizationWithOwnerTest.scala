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
import org.apache.kafka.clients.admin.{Admin, CreateDelegationTokenOptions, DescribeDelegationTokenOptions}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Collections
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class DelegationTokenEndToEndAuthorizationWithOwnerTest extends DelegationTokenEndToEndAuthorizationTest {

  def createTokenForValidUserArgs: Array[String] = Array("--authorizer-properties",
    s"zookeeper.connect=$zkConnect",
    s"--add",
    s"--user-principal=$clientPrincipal",
    s"--operation=CreateTokens",
    s"--allow-principal=$tokenRequesterPrincipal")

  // tests the naive positive case for token requesting for others
  def describeTokenForValidUserArgs: Array[String] = Array("--authorizer-properties",
    s"zookeeper.connect=$zkConnect",
    s"--add",
    s"--user-principal=$clientPrincipal",
    s"--operation=DescribeTokens",
    s"--allow-principal=$tokenRequesterPrincipal")

  // This permission is just there so that otherClientPrincipal shows up among the resources
  def describeTokenForAdminArgs: Array[String] = Array("--authorizer-properties",
    s"zookeeper.connect=$zkConnect",
    s"--add",
    s"--user-principal=$otherClientPrincipal",
    s"--operation=DescribeTokens",
    s"--allow-principal=$otherClientRequesterPrincipal")

  override def createDelegationTokenOptions(): CreateDelegationTokenOptions = new CreateDelegationTokenOptions().owner(clientPrincipal)

  private val tokenRequesterPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramUser2)
  private val tokenRequesterPassword = JaasTestUtils.KafkaScramPassword2

  private val otherClientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "other-client-principal")
  private val otherClientPassword = "other-client-password"

  private val otherClientRequesterPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "other-client-requester-principal")
  private val otherClientRequesterPassword = "other-client-requester-password"

  private val describeTokenFailPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "describe-token-fail-principal")
  private val describeTokenFailPassword = "describe-token-fail-password"

  override def configureTokenAclsBeforeServersStart(): Unit = {
    super.configureTokenAclsBeforeServersStart()
    AclCommand.main(createTokenForValidUserArgs)
    AclCommand.main(describeTokenForValidUserArgs)
    AclCommand.main(describeTokenForAdminArgs)
  }

  override def createAdditionalCredentialsAfterServersStarted(): Unit = {
    super.createAdditionalCredentialsAfterServersStarted()
    createScramCredentialsViaPrivilegedAdminClient(tokenRequesterPrincipal.getName, tokenRequesterPassword)
    createScramCredentialsViaPrivilegedAdminClient(otherClientPrincipal.getName, otherClientPassword)
    createScramCredentialsViaPrivilegedAdminClient(otherClientRequesterPrincipal.getName, otherClientRequesterPassword)
    createScramCredentialsViaPrivilegedAdminClient(describeTokenFailPrincipal.getName, describeTokenFailPassword)
  }

  override def assertToken(token: DelegationToken): Unit = {
    assertTokenOwner(clientPrincipal, token)
    assertTokenRequester(tokenRequesterPrincipal, token)
  }

  override def createTokenRequesterAdminClient(): Admin = {
    createScramAdminClient(kafkaClientSaslMechanism, tokenRequesterPrincipal.getName, tokenRequesterPassword)
  }

  @Test
  def testCreateTokenForOtherUserFails(): Unit = {
    val thrown = assertThrows(classOf[ExecutionException], () => {
      createDelegationTokens(() => new CreateDelegationTokenOptions().owner(otherClientPrincipal), assert = false)
    })
    assertTrue(thrown.getMessage.contains("Delegation Token authorization failed"))
  }

  @Test
  def testDescribeTokenForOtherUserFails(): Unit = {
    val describeTokenFailAdminClient = createScramAdminClient(kafkaClientSaslMechanism, describeTokenFailPrincipal.getName, describeTokenFailPassword)
    val otherClientAdminClient = createScramAdminClient(kafkaClientSaslMechanism, otherClientPrincipal.getName, otherClientPassword)
    try {
      otherClientAdminClient.createDelegationToken().delegationToken().get()
      val tokens = describeTokenFailAdminClient.describeDelegationToken(
        new DescribeDelegationTokenOptions().owners(Collections.singletonList(otherClientPrincipal)))
        .delegationTokens.get.asScala
      assertTrue(tokens.isEmpty)
    } finally {
      describeTokenFailAdminClient.close()
      otherClientAdminClient.close()
    }
  }

  @Test
  def testDescribeTokenForOtherUserPasses(): Unit = {
    val adminClient = createTokenRequesterAdminClient()
    try {
      val tokens = adminClient.describeDelegationToken(
        new DescribeDelegationTokenOptions().owners(Collections.singletonList(clientPrincipal)))
        .delegationTokens.get.asScala
      assertTrue(tokens.nonEmpty)
      tokens.foreach(t => {
        assertTrue(t.tokenInfo.owner.equals(clientPrincipal))
        assertTrue(t.tokenInfo.tokenRequester.equals(tokenRequesterPrincipal))
      })
    } finally {
      adminClient.close()
    }
  }
}
