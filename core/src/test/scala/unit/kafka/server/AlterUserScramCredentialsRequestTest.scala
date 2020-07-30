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


import java.util
import java.util.Properties

import kafka.network.SocketServer
import kafka.security.authorizer.AclAuthorizer
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

/**
 * Test AlterUserScramCredentialsRequest/Response API for the cases where either no credentials are altered
 * or failure is expected due to lack of authorization, sending the request to a non-controller broker, or some other issue.
 * Testing the API for the case where credentials are successfully altered is performed elsewhere.
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
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_512.ordinal().toByte)))).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(2, results.size)
    assertTrue("Expected not authorized",
      results.get(0).errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code && results.get(1).errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code)
  }

  @Test
  def testAlterSameThingTwice(): Unit = {
    val deletion1 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)
    val deletion2 = new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)
    val upsertion1 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)
      .setIterations(-1).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val upsertion2 = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)
      .setIterations(-1).setSalt("salt".getBytes).setSaltedPassword("saltedPassword".getBytes)
    val requests = List (
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletion1, deletion1, deletion2))
          .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build(),
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion])
          .setUpsertions(util.Arrays.asList(upsertion1, upsertion1, upsertion2))).build(),
      new AlterUserScramCredentialsRequest.Builder(
        new AlterUserScramCredentialsRequestData()
          .setDeletions(util.Arrays.asList(deletion1, deletion2))
          .setUpsertions(util.Arrays.asList(upsertion1))).build(),
    )
    requests.foreach(request => {
      val response = sendAlterUserScramCredentialsRequest(request)
      val results = response.data.results
      assertEquals(2, results.size)
      assertTrue("Expected error when altering the same credential twice in a single request",
        results.get(0).errorCode == Errors.INVALID_REQUEST.code && results.get(1).errorCode == Errors.INVALID_REQUEST.code)
    })
  }

  @Test
  def testDeleteSomethingThatDoesNotExist(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)))
        .setUpsertions(new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion])).build()
    val response = sendAlterUserScramCredentialsRequest(request)

    val results = response.data.results
    assertEquals(1, results.size)
    assertTrue("Expected error when deleting a non-existing credential",
      results.get(0).errorCode == Errors.RESOURCE_NOT_FOUND.code)
  }

  @Test
  def testAlterNotController(): Unit = {
    val request = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData()
        .setDeletions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("name1").setMechanism(ScramMechanism.SCRAM_SHA_256.ordinal().toByte)))
        .setUpsertions(util.Arrays.asList(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion().setName("name2").setMechanism(ScramMechanism.SCRAM_SHA_512.ordinal().toByte)))).build()
    val response = sendAlterUserScramCredentialsRequest(request, notControllerSocketServer)

    val results = response.data.results
    assertEquals(2, results.size)
    assertTrue("Expected controller error when routed incorrectly",
      results.get(0).errorCode == Errors.NOT_CONTROLLER.code && results.get(1).errorCode == Errors.NOT_CONTROLLER.code)
  }

  private def sendAlterUserScramCredentialsRequest(request: AlterUserScramCredentialsRequest, socketServer: SocketServer = controllerSocketServer): AlterUserScramCredentialsResponse = {
    connectAndReceive[AlterUserScramCredentialsResponse](request, destination = socketServer)
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
