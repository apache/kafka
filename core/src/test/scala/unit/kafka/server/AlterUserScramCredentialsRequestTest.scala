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
import java.util.Properties

import kafka.network.SocketServer
import kafka.security.authorizer.AclAuthorizer
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.{AlterUserScramCredentialsRequestData, DescribeUserScramCredentialsRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

/**
 * Test AlterUserScramCredentialsRequest/Response API for the cases where either no credentials are altered
 * or failure is expected due to lack of authorization, sending the request to a non-controller broker, or some other issue.
 * Also tests the Alter and Describe APIs for the case where credentials are successfully altered/described.
 */
class AlterUserScramCredentialsRequestTest extends BaseRequestTest {
  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[AlterCredentialsTest.TestAuthorizer].getName)
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[AlterCredentialsTest.TestPrincipalBuilder].getName)
  }

  @Before
  override def setUp(): Unit = {
    AlterCredentialsTest.principal = KafkaPrincipal.ANONYMOUS // default is to be authorized
    super.setUp()
  }

  @Test
  def testAlterNothing(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(0, results.size)
  }

  @Test
  def testAlterNothingNotAuthorized(): Unit = {
    AlterCredentialsTest.principal = AlterCredentialsTest.UnauthorizedPrincipal

    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(0, results.size)
  }

  @Test
  def testAlterSomethingNotAuthorized(): Unit = {
    AlterCredentialsTest.principal = AlterCredentialsTest.UnauthorizedPrincipal

    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)))).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(2, results.size)
    val msg = "Expected not authorized"
    assertEquals(msg, Errors.CLUSTER_AUTHORIZATION_FAILED.code, results.get(0).errorCode)
    assertEquals(msg, Errors.CLUSTER_AUTHORIZATION_FAILED.code, results.get(1).errorCode)
  }

  @Test
  def testAlterSameThingTwice(): Unit = {
    val deletion1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val deletion2 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val upsertion1 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val upsertion2 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
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
      val msg = "Expected error when altering the same credential twice in a single request"
      assertEquals(msg, Errors.INVALID_REQUEST.code, results.get(0).errorCode)
      assertEquals(msg, Errors.INVALID_REQUEST.code, results.get(1).errorCode)
    })
  }

  @Test
  def testAlterEmptyUser(): Unit = {
    val deletionEmpty = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val upsertionEmpty = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(4096).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
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
      assertEquals("Expected error when altering an empty user", Errors.INVALID_REQUEST.code, results.get(0).errorCode)
      assertEquals("\"\" is an illegal user name", results.get(0).errorMessage)
    })
  }

  private val user1 = "user1"
  private val user2 = "user2"

  @Test
  def testAlterUnknownMechanism(): Unit = {
    val deletionUnknown1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.UNKNOWN.`type`)
    val deletionValid1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
    val deletionUnknown2 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(user2).setMechanism(10.toByte)
    val user3 = "user3"
    val upsertionUnknown3 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user3).setMechanism(ScramMechanism.UNKNOWN.`type`)
      .setIterations(8192).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val upsertionValid3 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user3).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setIterations(8192).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val user4 = "user4"
    val upsertionUnknown4 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user4).setMechanism(10.toByte)
      .setIterations(8192).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val user5 = "user5"
    val upsertionUnknown5 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user5).setMechanism(ScramMechanism.UNKNOWN.`type`)
      .setIterations(8192).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletionUnknown1, deletionValid1, deletionUnknown2))
          .setUpsertions(util.Arrays.asList(upsertionUnknown3, upsertionValid3, upsertionUnknown4, upsertionUnknown5))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(5, results.size)
    assertEquals("Expected error when altering the credentials with unknown SCRAM mechanisms",
      0, results.asScala.filterNot(_.errorCode == Errors.INVALID_REQUEST.code).size)
    results.asScala.foreach(result => assertEquals("Unknown SCRAM mechanism", result.errorMessage))
  }

  @Test
  def testAlterTooFewIterations(): Unit = {
    val upsertionTooFewIterations = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1)
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`).setIterations(1)
      .setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Collections.emptyList())
        .setUpsertions(util.Arrays.asList(upsertionTooFewIterations))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(1, results.size)
    assertEquals("Expected error when altering the credentials with too few iterations",
      0, results.asScala.filterNot(_.errorCode == Errors.INVALID_REQUEST.code).size)
    assertEquals("Too few iterations", results.get(0).errorMessage)
  }

  @Test
  def testAlterTooManyIterations(): Unit = {
    val upsertionTooFewIterations = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName(user1)
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`).setIterations(Integer.MAX_VALUE)
      .setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Collections.emptyList())
        .setUpsertions(util.Arrays.asList(upsertionTooFewIterations))).build()
    val response = sendAlterUserScramCredentialsRequest(request)
    val results = response.data.results
    assertEquals(1, results.size)
    assertEquals("Expected error when altering the credentials with too many iterations",
      0, results.asScala.filterNot(_.errorCode == Errors.INVALID_REQUEST.code).size)
    assertEquals("Too many iterations", results.get(0).errorMessage)
  }

  @Test
  def testDeleteSomethingThatDoesNotExist(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(1, results.size)
    assertEquals("Expected error when deleting a non-existing credential", Errors.RESOURCE_NOT_FOUND.code, results.get(0).errorCode)
  }

  @Test
  def testAlterNotController(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)))).build()
    val response = sendAlterUserScramCredentialsRequest(request, notControllerSocketServer)

    val results = response.data.results
    assertEquals(2, results.size)
    val msg = "Expected controller error when routed incorrectly"
    assertEquals(msg, Errors.NOT_CONTROLLER.code, results.get(0).errorCode)
    assertEquals(msg, Errors.NOT_CONTROLLER.code, results.get(1).errorCode)
  }

  @Test
  def testAlterAndDescribe(): Unit = {
    // create a bunch of credentials
    val request1 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setUpsertions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
            .setIterations(4096)
            .setSalt("salt".getBytes(StandardCharsets.UTF_8))
            .setSaltedPassword("saltedPassword".getBytes(StandardCharsets.UTF_8)),
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)
            .setIterations(8192)
            .setSalt("salt".getBytes(StandardCharsets.UTF_8))
            .setSaltedPassword("saltedPassword".getBytes(StandardCharsets.UTF_8)),
          new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
            .setName(user2).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`)
            .setIterations(8192)
            .setSalt("salt".getBytes(StandardCharsets.UTF_8))
            .setSaltedPassword("saltedPassword".getBytes(StandardCharsets.UTF_8)),
        ))).build()
    val response1 = sendAlterUserScramCredentialsRequest(request1)
    val results1 = response1.data.results
    assertEquals(2, results1.size)
    assertEquals("Expected no error when creating the credentials",
      0, results1.asScala.filterNot(_.errorCode == Errors.NONE.code).size)
    assertTrue(results1.asScala.exists(_.user == user1))
    assertTrue(results1.asScala.exists(_.user == user2))
    // now describe them all
    val request2 = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response2 = sendDescribeUserScramCredentialsRequest(request2)
    assertEquals("Expected no error when describing the credentials",
      Errors.NONE.code, response2.data.error)
    val results2 = response2.data.userScramCredentials
    assertEquals(2, results2.size)
    assertTrue(s"Expected result to contain '$user1' with 2 credentials: $results2",
      results2.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.size == 2))
    assertTrue(s"Expected result to contain '$user2' with 1 credential: $results2",
      results2.asScala.exists(usc => usc.name == user2 && usc.credentialInfos.size == 1))
    assertTrue(s"Expected result to contain '$user1' with SCRAM_SHA_256/4096 and SCRAM_SHA_512/8192 credentials: $results2",
      results2.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_256.`type` && info.iterations == 4096)
        && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)))
    assertTrue(s"Expected result to contain '$user2' with SCRAM_SHA_512/8192 credential: $results2",
      results2.asScala.exists(usc => usc.name == user2 && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)))
    // now describe just one
    val request3 = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(util.Arrays.asList(
        new DescribeUserScramCredentialsRequestData.UserName().setName(user1)))).build()
    val response3 = sendDescribeUserScramCredentialsRequest(request3)
    assertEquals("Expected no error when describing the credentials",Errors.NONE.code, response3.data.error)
    val results3 = response3.data.userScramCredentials
    assertEquals(1, results3.size)
    assertTrue(s"Expected result to contain '$user1' with 2 credentials: $results3",
      results3.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.size == 2))
    assertTrue(s"Expected result to contain '$user1' with SCRAM_SHA_256/4096 and SCRAM_SHA_512/8192 credentials: $results3",
      results3.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_256.`type` && info.iterations == 4096)
        && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)))
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
    assertEquals("Expected no error when deleting the credentials",
      0, results4.asScala.filterNot(_.errorCode == Errors.NONE.code).size)
    assertTrue(s"Expected result to contain '$user1'", results4.asScala.exists(_.user == user1))
    assertTrue(s"Expected result to contain '$user2'", results4.asScala.exists(_.user == user2))
    // now describe them all, which should just yield 1 credential
    val request5 = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response5 = sendDescribeUserScramCredentialsRequest(request5)
    assertEquals("Expected no error when describing the credentials", Errors.NONE.code, response5.data.error)
    val results5 = response5.data.userScramCredentials
    assertEquals(1, results5.size)
    assertTrue(s"Expected result to contain '$user1' with 1 credential: $results5",
      results5.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.size == 1))
    assertTrue(s"Expected result to contain '$user1' with SCRAM_SHA_512/8192 credential: $results5",
      results5.asScala.exists(usc => usc.name == user1 && usc.credentialInfos.asScala.exists(info =>
        info.mechanism == ScramMechanism.SCRAM_SHA_512.`type` && info.iterations == 8192)))
    // now delete the last one
    val request6 = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(
          new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
            .setName(user1).setMechanism(ScramMechanism.SCRAM_SHA_512.`type`),
        ))).build()
    val response6 = sendAlterUserScramCredentialsRequest(request6)
    val results6 = response6.data.results
    assertEquals(1, results6.size)
    assertEquals("Expected no error when deleting the credentials",
      0, results4.asScala.filterNot(_.errorCode == Errors.NONE.code).size)
    assertTrue(s"Expected result to contain '$user1'", results6.asScala.exists(_.user == user1))
    // now describe them all, which should yield 0 credentials
    val request7 = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build()
    val response7 = sendDescribeUserScramCredentialsRequest(request7)
    assertEquals("Expected no error when describing the credentials",
      Errors.NONE.code, response7.data.error)
    val results7 = response7.data.userScramCredentials
    assertEquals(0, results7.size)
  }

  private def sendAlterUserScramCredentialsRequest(request: AlterUserScramCredentialsRequest, socketServer: SocketServer = controllerSocketServer): AlterUserScramCredentialsResponse = {
    connectAndReceive[AlterUserScramCredentialsResponse](request, destination = socketServer)
  }

  private def sendDescribeUserScramCredentialsRequest(request: DescribeUserScramCredentialsRequest, socketServer: SocketServer = controllerSocketServer): DescribeUserScramCredentialsResponse = {
    connectAndReceive[DescribeUserScramCredentialsResponse](request, destination = socketServer)
  }
}

object AlterCredentialsTest {
  val UnauthorizedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Unauthorized")
  // Principal used for all client connections. This is modified by tests which
  // check unauthorized code path
  var principal = KafkaPrincipal.ANONYMOUS

  class TestAuthorizer extends AclAuthorizer {
    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
      // UnauthorizedPrincipal is not authorized for ALTER permission on CLUSTER resource
      actions.asScala.map { action =>
        if (requestContext.principal == UnauthorizedPrincipal && action.operation == AclOperation.ALTER && action.resourcePattern.resourceType == ResourceType.CLUSTER)
          AuthorizationResult.DENIED
        else
          AuthorizationResult.ALLOWED
      }.asJava
    }
  }

  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      principal
    }
  }
}
