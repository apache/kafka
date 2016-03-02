/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
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

import java.nio.ByteBuffer

import kafka.security.auth.{SimpleAclAuthorizer, Deny, Describe, Read, Resource, All, Allow, Acl, Topic}
import kafka.utils._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AlterAclsResponse, AlterAclsRequest, ListAclsResponse, ListAclsRequest, RequestHeader,
ResponseHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.{Resource => JResource, ResourceType => JResourceType}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class AclsRequestTest extends BaseRequestTest {

  @Before
  override def setUp() {
    super.setUp()
    Logger.getLogger(classOf[SimpleAclAuthorizer]).setLevel(Level.DEBUG)
    Logger.getLogger(classOf[KafkaApis]).setLevel(Level.DEBUG)
  }

  @Test
  def testListAclsRequests() {
    val acls1 = Set(Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, All))
    val resource1 = Resource.ClusterResource
    val jResource1 = resource1.asJava
    servers.head.apis.authorizer.get.addAcls(acls1, resource1)

    val acls2 = Set(Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read))
    val resource2 = new Resource(Topic, "topic")
    val jResource2 = resource2.asJava
    servers.head.apis.authorizer.get.addAcls(acls2, resource2)

    val unusedJResource = new JResource(JResourceType.TOPIC, "unused")
    val unusedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unused")

    TestUtils.waitAndVerifyAcls(acls1, servers.head.apis.authorizer.get, resource1)

    validateListAclsRequests(new ListAclsRequest(), Map(resource1 -> acls1, resource2 -> acls2))
    validateListAclsRequests(new ListAclsRequest(KafkaPrincipal.ANONYMOUS), Map(resource1 -> acls1, resource2 -> acls2))
    validateListAclsRequests(new ListAclsRequest(jResource2), Map(resource2 -> acls2))
    validateListAclsRequests(new ListAclsRequest(KafkaPrincipal.ANONYMOUS, jResource1), Map(resource1 -> acls1))
    validateListAclsRequests(new ListAclsRequest(unusedJResource), Map())
    validateListAclsRequests(new ListAclsRequest(unusedPrincipal), Map())
    validateListAclsRequests(new ListAclsRequest(unusedPrincipal, unusedJResource), Map())
  }

  private def validateListAclsRequests(request: ListAclsRequest, expectedAcls: Map[Resource, Set[Acl]]): Unit = {
    val response = sendListAclsRequest(request)

    assertEquals(s"There should be no errors", Errors.NONE, response.error)

    response.acls.asScala.foreach { case (resource, acls) =>
      val sResource = Resource.fromJava(resource)


      assertTrue("The resource should have acls", expectedAcls.contains(sResource))
      assertEquals("The resource should have the correct number of acls", expectedAcls.get(sResource).size, acls.size)

      val expectedAclsForResource = expectedAcls.get(sResource).get
      acls.asScala.foreach { jAcl =>
        val sAcl = Acl.fromJava(jAcl)
        assertTrue(s"The resource should have acl $sAcl", expectedAclsForResource.contains(sAcl))
      }
    }
  }

  @Test
  def testAlterAclsRequests() {
    val resource1 = Resource.ClusterResource
    val jResource1 = resource1.asJava
    val acl1 = new Acl(KafkaPrincipal.ANONYMOUS, Allow, "*", Describe)
    val jAcl1 = acl1.asJava
    val actionRequest1 = new AlterAclsRequest.ActionRequest(AlterAclsRequest.Action.ADD, jAcl1)
    val requests1 = Map(jResource1 -> List(actionRequest1).asJava)

    validateAlterAclsRequests(new AlterAclsRequest(requests1.asJava), Map(resource1 -> Set(acl1)))
    removeAllAcls

    val resource2 = Resource.ClusterResource
    val jResource2 = resource2.asJava
    val acl2a = new Acl(KafkaPrincipal.ANONYMOUS, Allow, "*", Describe)
    val jAcl2a = acl2a.asJava
    val actionRequest2a = new AlterAclsRequest.ActionRequest(AlterAclsRequest.Action.ADD, jAcl2a)
    val acl2b = new Acl(KafkaPrincipal.ANONYMOUS, Deny, "*", Describe)
    val jAcl2b = acl2b.asJava
    // Add a second acl
    val actionRequest2b = new AlterAclsRequest.ActionRequest(AlterAclsRequest.Action.ADD, jAcl2b)
    // Make sure deletes are processed first
    val actionRequest2c = new AlterAclsRequest.ActionRequest(AlterAclsRequest.Action.DELETE, jAcl2a)
    val requests2 = Map(jResource2 -> List(actionRequest2a, actionRequest2b).asJava)

    validateAlterAclsRequests(new AlterAclsRequest(requests2.asJava), Map(resource2 -> Set(acl2a, acl2b)))
    removeAllAcls
  }

  private def validateAlterAclsRequests(request: AlterAclsRequest, expectedAcls: Map[Resource, Set[Acl]]): Unit = {
    val response = sendAlterAclsRequest(request)

    val hasError =
      !response.results.asScala.forall { case (resource, results) =>
        results.asScala.forall(_.error == Errors.NONE)
      }

    assertFalse(s"There should be no errors", hasError)

    expectedAcls.foreach { case (resource, acls) =>
      TestUtils.waitAndVerifyAcls(acls, servers.head.apis.authorizer.get, resource)
    }
  }

  private def sendListAclsRequest(request: ListAclsRequest): ListAclsResponse = {
    val correlationId = -1

    val serializedBytes = {
      val header = new RequestHeader(ApiKeys.LIST_ACLS.id, 0, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    ListAclsResponse.parse(responseBuffer)
  }

  private def sendAlterAclsRequest(request: AlterAclsRequest): AlterAclsResponse = {
    val correlationId = -1

    val serializedBytes = {
      val header = new RequestHeader(ApiKeys.ALTER_ACLS.id, 0, "", correlationId)
      val byteBuffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf)
      header.writeTo(byteBuffer)
      request.writeTo(byteBuffer)
      byteBuffer.array()
    }

    val response = requestAndReceive(serializedBytes)

    val responseBuffer = ByteBuffer.wrap(response)
    val responseHeader = ResponseHeader.parse(responseBuffer)
    AlterAclsResponse.parse(responseBuffer)
  }

  def removeAllAcls() = {
    servers.head.apis.authorizer.get.getAcls().keys.foreach { resource =>
      servers.head.apis.authorizer.get.removeAcls(resource)
      TestUtils.waitAndVerifyAcls(Set.empty[Acl], servers.head.apis.authorizer.get, resource)
    }
  }
}
