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


import java.nio.charset.StandardCharsets
import java.util
import kafka.utils.TestUtils
import kafka.utils.TestInfoUtils
import kafka.network.SocketServer
import kafka.security.authorizer.AclAuthorizer
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult
import org.apache.kafka.common.message.{AlterUserScramCredentialsRequestData, DescribeUserScramCredentialsRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.{Test, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._

/**
 * Test AlterUserScramCredentialsRequest/Response API for the cases where either no credentials are altered
 * or failure is expected due to lack of authorization, sending the request to a non-controller broker, or some other issue.
 * Also tests the Alter and Describe APIs for the case where credentials are successfully altered/described.
 */
class AlterUserScramCredentialsRequestTest extends BaseRequestTest {

  protected var testMetadataVersion = MetadataVersion.latestTesting
  override protected def metadataVersion = testMetadataVersion

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    if (TestInfoUtils.isKRaft(testInfo)) {
      this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[StandardAuthorizer].getName)
      if (testInfo.getDisplayName().contains("quorum=kraft-IBP_3_4")) {
        testMetadataVersion = MetadataVersion.IBP_3_4_IV0
      }
    } else {
      this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[AlterCredentialsTest.TestAuthorizer].getName)

    }
    this.serverConfig.setProperty(KafkaConfig.PrincipalBuilderClassProp, classOf[AlterCredentialsTest.TestPrincipalBuilderReturningAuthorized].getName)
    this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")

    super.setUp(testInfo)
  }

  private val saltedPasswordBytes = "saltedPassword".getBytes(StandardCharsets.UTF_8)
  private val saltBytes = "salt".getBytes(StandardCharsets.UTF_8)
  private val user1 = "user1"
  private val user2 = "user2"
  private val user3 = "user3@user3.com"
  private val unknownUser = "unknownUser"

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterNothing(quorum: String): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(0, results.size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterSameThingTwice(quorum: String): Unit = {
    val deletion1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val deletion2 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val upsertion1 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val upsertion2 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)

    // Create user1 and user2 so delete returns duplicate instead of no resource
    val init_requests = List (
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Collections.emptyList())
          .setUpsertions(util.Arrays.asList(upsertion1, upsertion2))).build(),
    )
    init_requests.foreach(request => {
      val response = sendAlterUserScramCredentialsRequest(request)
      val results = response.data.results
      checkNoErrorsAlteringCredentials(results)
    })

    val requests = List (
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletion1, deletion1))
          .setUpsertions(util.Arrays.asList(upsertion2, upsertion2))).build(),
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletion1, deletion2))
          .setUpsertions(util.Arrays.asList(upsertion1, upsertion2))).build(),
    )
    requests.foreach(request => {
      val response = sendAlterUserScramCredentialsRequest(request)
      val results = response.data.results
      assertEquals(2, results.size)
      checkAllErrorsAlteringCredentials(results, Errors.DUPLICATE_RESOURCE, "when altering the same credential twice in a single request")
    })
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterEmptyUser(quorum: String): Unit = {
    val deletionEmpty = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val upsertionEmpty = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val requests = List (
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletionEmpty))
          .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build(),
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
          .setUpsertions(util.Arrays.asList(upsertionEmpty))).build(),
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletionEmpty, deletionEmpty))
          .setUpsertions(util.Arrays.asList(upsertionEmpty))).build(),
    )
    requests.foreach(request => {
      val response = sendAlterUserScramCredentialsRequest(request)
      val results = response.data.results
      assertEquals(1, results.size)
      checkAllErrorsAlteringCredentials(results, Errors.UNACCEPTABLE_CREDENTIAL, "when altering an empty user")
      assertEquals("Username must not be empty", results.get(0).errorMessage)
    })
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterUnknownMechanism(quorum: String): Unit = {
    val deletionUnknown1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.UNKNOWN.`type`)
    val deletionValid1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val deletionUnknown2 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user2).setMechanism(10.toByte)
    val user4 = "user4"
    val upsertionUnknown4 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user4).setMechanism(ScramMechanism.UNKNOWN.`type`)
      .setIterations(8192).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val upsertionValid4 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user4).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(8192).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val user5 = "user5"
    val upsertionUnknown5 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user5).setMechanism(10.toByte)
      .setIterations(8192).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val user6 = "user6"
    val upsertionUnknown6 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user6).setMechanism(ScramMechanism.UNKNOWN.`type`)
      .setIterations(8192).setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletionUnknown1, deletionValid1, deletionUnknown2))
          .setUpsertions(util.Arrays.asList(upsertionUnknown4, upsertionValid4, upsertionUnknown5, upsertionUnknown6))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(5, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.UNSUPPORTED_SASL_MECHANISM, "when altering the credentials with unknown SCRAM mechanisms")
    results.asScala.foreach(result => assertEquals("Unknown SCRAM mechanism", result.errorMessage))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterTooFewIterations(quorum: String): Unit = {
    val upsertionTooFewIterations = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1)
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`).setIterations(1)
      .setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Collections.emptyList())
        .setUpsertions(util.Arrays.asList(upsertionTooFewIterations))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(1, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.UNACCEPTABLE_CREDENTIAL, "when altering the credentials with too few iterations")
    assertEquals("Too few iterations", results.get(0).errorMessage)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterTooManyIterations(quorum: String): Unit = {
    val upsertionTooFewIterations = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1)
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`).setIterations(Integer.MAX_VALUE)
      .setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Collections.emptyList())
        .setUpsertions(util.Arrays.asList(upsertionTooFewIterations))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(1, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.UNACCEPTABLE_CREDENTIAL, "when altering the credentials with too many iterations")
    assertEquals("Too many iterations", results.get(0).errorMessage)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testDeleteSomethingThatDoesNotExist(quorum: String): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(1, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.RESOURCE_NOT_FOUND, "when deleting a non-existing credential")
  }

  @Test
  def testAlterNotController(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)))).build()
    val response = sendAlterUserScramCredentialsRequest(request, notControllerSocketServer)

    val results = response.data.results
    assertEquals(2, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.NOT_CONTROLLER, "when routed incorrectly to a non-Controller broker")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft", "zk"))
  def testAlterAndDescribe(quorum: String): Unit = {
    // create a bunch of credentials
    val request1_0 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setUpsertions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
            .setIterations(4096)
            .setSalt(saltBytes)
            .setSaltedPassword(saltedPasswordBytes),
        ))).build()
    val results1_0 = sendAlterUserScramCredentialsRequest(request1_0).data.results
    assertEquals(1, results1_0.size)
    checkNoErrorsAlteringCredentials(results1_0)
    checkUserAppearsInAlterResults(results1_0, user1)

    // When creating credentials, do not update the same user more than once per request
    val request1_1 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setUpsertions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)
            .setIterations(8192)
            .setSalt(saltBytes)
            .setSaltedPassword(saltedPasswordBytes),
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)
            .setIterations(8192)
            .setSalt(saltBytes)
            .setSaltedPassword(saltedPasswordBytes),
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user3).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)
            .setIterations(8192)
            .setSalt(saltBytes)
            .setSaltedPassword(saltedPasswordBytes),
        ))).build()
    val results1_1 = sendAlterUserScramCredentialsRequest(request1_1).data.results
    assertEquals(3, results1_1.size)
    checkNoErrorsAlteringCredentials(results1_1)
    checkUserAppearsInAlterResults(results1_1, user1)
    checkUserAppearsInAlterResults(results1_1, user2)
    checkUserAppearsInAlterResults(results1_1, user3)

    // KRaft is eventually consistent so it is possible to call describe before
    // the credential is propagated from the controller to the broker.
    TestUtils.waitUntilTrue(() => describeAllWithNoTopLevelErrorConfirmed().data.results.size == 3,
                               "describeAllWithNoTopLevelErrorConfirmed does not see 3 users");

    // now describe them all
    val results2 = describeAllWithNoTopLevelErrorConfirmed().data.results
    assertEquals(3, results2.size)
    checkUserHasTwoCredentials(results2, user1)
    checkForSingleSha512Iterations8192Credential(results2, user2)
    checkForSingleSha512Iterations8192Credential(results2, user3)

    // now describe just one
    val request3 = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(util.Arrays.asList(
        new DescribeUserScramCredentialsRequestData.UserName().setName(user1)))).build()
    val response3 = sendDescribeUserScramCredentialsRequest(request3)
    checkNoTopLevelErrorDescribingCredentials(response3)
    val results3 = response3.data.results
    assertEquals(1, results3.size)
    checkUserHasTwoCredentials(results3, user1)

    // now test per-user errors by describing user1 and an unknown
    val requestUnknown = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(util.Arrays.asList(
        new DescribeUserScramCredentialsRequestData.UserName().setName(user1),
        new DescribeUserScramCredentialsRequestData.UserName().setName(unknownUser)))).build()
    val responseUnknown = sendDescribeUserScramCredentialsRequest(requestUnknown)
    checkNoTopLevelErrorDescribingCredentials(responseUnknown)
    val resultsUnknown = responseUnknown.data.results
    assertEquals(2, resultsUnknown.size)
    checkUserHasTwoCredentials(resultsUnknown, user1)
    checkDescribeForError(resultsUnknown, unknownUser, Errors.RESOURCE_NOT_FOUND)

    // now test per-user errors again by describing user1 along with user2 twice
    val requestDuplicateUser = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(util.Arrays.asList(
        new DescribeUserScramCredentialsRequestData.UserName().setName(user1),
        new DescribeUserScramCredentialsRequestData.UserName().setName(user2),
        new DescribeUserScramCredentialsRequestData.UserName().setName(user2)))).build()
    val responseDuplicateUser = sendDescribeUserScramCredentialsRequest(requestDuplicateUser)
    checkNoTopLevelErrorDescribingCredentials(responseDuplicateUser)
    val resultsDuplicateUser = responseDuplicateUser.data.results
    assertEquals(2, resultsDuplicateUser.size)
    checkUserHasTwoCredentials(resultsDuplicateUser, user1)
    checkDescribeForError(resultsDuplicateUser, user2, Errors.DUPLICATE_RESOURCE)

    // now delete a couple of credentials
    val request4 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`),
          new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
            .setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`),
        ))).build()
    val response4 = sendAlterUserScramCredentialsRequest(request4)
    val results4 = response4.data.results
    assertEquals(2, results4.size)
    checkNoErrorsAlteringCredentials(results4)
    checkUserAppearsInAlterResults(results4, user1)
    checkUserAppearsInAlterResults(results4, user2)

    TestUtils.waitUntilTrue(() => describeAllWithNoTopLevelErrorConfirmed().data.results.size == 2,
                               "describeAllWithNoTopLevelErrorConfirmed does not see only 2 users");

    // now describe them all, which should just yield 2 credentials
    val results5 = describeAllWithNoTopLevelErrorConfirmed().data.results
    assertEquals(2, results5.size)
    checkForSingleSha512Iterations8192Credential(results5, user1)
    checkForSingleSha512Iterations8192Credential(results5, user3)

    // now delete user1 and user3
    val request6 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`),
          new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
            .setName(user3).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`),
        ))).build()
    val results6 = sendAlterUserScramCredentialsRequest(request6).data.results
    assertEquals(2, results6.size)
    checkNoErrorsAlteringCredentials(results6)
    checkUserAppearsInAlterResults(results6, user1)
    checkUserAppearsInAlterResults(results6, user3)

    TestUtils.waitUntilTrue(() => describeAllWithNoTopLevelErrorConfirmed().data.results.size == 0,
                               "describeAllWithNoTopLevelErrorConfirmed does not see empty user");

    // now describe them all, which should yield 0 credentials
    val results7 = describeAllWithNoTopLevelErrorConfirmed().data.results
    assertEquals(0, results7.size)
  }

  /*
   * Test that SCRAM alter command on KRaft cluster with IBP version less that IBP_3_5 fails
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft-IBP_3_4"))
  def testMetadataVersionTooLow(quorum: String): Unit = {
    val upsertionMetadataVersionTooLow = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1)
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`).setIterations(8192)
      .setSalt(saltBytes).setSaltedPassword(saltedPasswordBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Collections.emptyList())
        .setUpsertions(util.Arrays.asList(upsertionMetadataVersionTooLow))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(1, results.size)
    checkAllErrorsAlteringCredentials(results, Errors.UNSUPPORTED_VERSION,
                                      "when altering the credentials on unsupported IBP version")
    assertEquals("The current metadata version does not support SCRAM", results.get(0).errorMessage)
  }

  private def sendAlterUserScramCredentialsRequest(request: AlterUserScramCredentialsRequest, socketServer: SocketServer = adminSocketServer): AlterUserScramCredentialsResponse = {
    connectAndReceive[AlterUserScramCredentialsResponse](request, destination = socketServer)
  }

  private def sendDescribeUserScramCredentialsRequest(request: DescribeUserScramCredentialsRequest, socketServer: SocketServer = adminSocketServer): DescribeUserScramCredentialsResponse = {
    connectAndReceive[DescribeUserScramCredentialsResponse](request, destination = socketServer)
  }

  private def checkAllErrorsAlteringCredentials(resultsToCheck: util.List[AlterUserScramCredentialsResult], expectedError: Errors, contextMsg: String) = {
    assertEquals(0, resultsToCheck.asScala.filterNot(_.errorCode == expectedError.code).size,
      s"Expected all '${expectedError.name}' errors when altering credentials $contextMsg")
  }

  private def checkNoErrorsAlteringCredentials(resultsToCheck: util.List[AlterUserScramCredentialsResult]) = {
    assertEquals(0, resultsToCheck.asScala.filterNot(_.errorCode == Errors.NONE.code).size,
      "Expected no error when altering credentials")
  }

  private def checkUserAppearsInAlterResults(resultsToCheck: util.List[AlterUserScramCredentialsResult], user: String) = {
    assertTrue(resultsToCheck.asScala.exists(_.user == user), s"Expected result to contain '$user'")
  }

  private def describeAllWithNoTopLevelErrorConfirmed() = {
    val response = sendDescribeUserScramCredentialsRequest(
      new DescribeUserScramCredentialsRequest.Builder(new DescribeUserScramCredentialsRequestData()).build())
    checkNoTopLevelErrorDescribingCredentials(response)
    response
  }

  private def checkNoTopLevelErrorDescribingCredentials(responseToCheck: DescribeUserScramCredentialsResponse) = {
    assertEquals(Errors.NONE.code, responseToCheck.data.errorCode, "Expected no top-level error when describing the credentials")
  }

  private def checkUserHasTwoCredentials(resultsToCheck: util.List[DescribeUserScramCredentialsResult], user: String) = {
    assertTrue(resultsToCheck.asScala.exists(result => result.user == user && result.credentialInfos.size == 2 && result.errorCode == Errors.NONE.code),
      s"Expected result to contain '$user' with 2 credentials: $resultsToCheck")
    assertTrue(resultsToCheck.asScala.exists(result => result.user == user && result.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_256.`type` && info.iterations == 4096)
        && result.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)),
      s"Expected result to contain '$user' with SCRAM_SHA_256/4096 and SCRAM_SHA_512/8192 credentials: $resultsToCheck")
  }

  private def checkForSingleSha512Iterations8192Credential(resultsToCheck: util.List[DescribeUserScramCredentialsResult], user: String) = {
    assertTrue(resultsToCheck.asScala.exists(result => result.user == user && result.credentialInfos.size == 1 && result.errorCode == Errors.NONE.code),
      s"Expected result to contain '$user' with 1 credential: $resultsToCheck")
    assertTrue(resultsToCheck.asScala.exists(result => result.user == user && result.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)),
      s"Expected result to contain '$user' with SCRAM_SHA_512/8192 credential: $resultsToCheck")
  }

  private def checkDescribeForError(resultsToCheck: util.List[DescribeUserScramCredentialsResult], user: String, expectedError: Errors) = {
    assertTrue(resultsToCheck.asScala.exists(result => result.user == user && result.credentialInfos.size == 0 && result.errorCode == expectedError.code),
      s"Expected result to contain '$user' with a ${expectedError.name} error: $resultsToCheck")
  }
}

object AlterCredentialsTest {
  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  val AuthorizedPrincipal = KafkaPrincipal.ANONYMOUS

  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      actions.asScala.map { _ =>
        if (requestContext.requestType == ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.id && requestContext.principal == UnauthorizedPrincipal)
          AuthorizationResult.DENIED
        else
          AuthorizationResult.ALLOWED
      }.asJava
    }
  }

  class TestPrincipalBuilderReturningAuthorized extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      AuthorizedPrincipal
    }
  }

  class TestPrincipalBuilderReturningUnauthorized extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      UnauthorizedPrincipal
    }
  }
}
