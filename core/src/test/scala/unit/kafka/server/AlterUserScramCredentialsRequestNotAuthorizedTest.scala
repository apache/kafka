/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.network.SocketServer
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * see AlterUserScramCredentialsRequestTest
 */
class AlterUserScramCredentialsRequestNotAuthorizedTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
    properties.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[AlterCredentialsTest.TestStandardAuthorizer].getName)
    properties.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[AlterCredentialsTest.TestPrincipalBuilderReturningUnauthorized].getName)
  }

  override def kraftControllerConfigs(testInfo: TestInfo): collection.Seq[Properties] = {
    val controllerConfigs = super.kraftControllerConfigs(testInfo)
    controllerConfigs.head.put(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, classOf[AlterCredentialsTest.TestStandardAuthorizer].getName)
    controllerConfigs.head.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[AlterCredentialsTest.TestPrincipalBuilderReturningUnauthorized].getName)
    controllerConfigs
  }

  private val user1 = "user1"
  private val user2 = "user2"

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAlterNothingNotAuthorized(quorum: String): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(0, results.size)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAlterSomethingNotAuthorized(quorum: String): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)))).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(2, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.CLUSTER_AUTHORIZATION_FAILED, "when not authorized")
  }

  private def sendAlterUserScramCredentialsRequest(request: AlterUserScramCredentialsRequest, socketServer: SocketServer = adminSocketServer): AlterUserScramCredentialsResponse = {
    connectAndReceive[AlterUserScramCredentialsResponse](request, destination = socketServer)
  }

  private def checkAllErrorsAlteringCredentials(resultsToCheck: util.List[AlterUserScramCredentialsResult], expectedError: Errors, contextMsg: String): Unit = {
    assertEquals(0, resultsToCheck.asScala.filterNot(_.errorCode == expectedError.code).size,
      s"Expected all '${expectedError.name}' errors when altering credentials $contextMsg")
  }
}