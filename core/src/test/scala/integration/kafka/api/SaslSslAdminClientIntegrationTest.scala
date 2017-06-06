/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.File

import kafka.admin.AclCommand
import kafka.security.auth.{All, Allow, Alter, AlterConfigs, Authorizer, ClusterAction, Create, Delete, Deny, Describe, Operation, PermissionType, SimpleAclAuthorizer, Topic, Acl => AuthAcl, Resource => AuthResource}
import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, CreateAclsOptions, DeleteAclsOptions, KafkaAdminClient}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException}
import org.apache.kafka.common.resource.{Resource, ResourceFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert.assertEquals
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SaslSslAdminClientIntegrationTest extends AdminClientIntegrationTest with SaslSetup {
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName())

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart() {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[SimpleAclAuthorizer].getName())
    authorizer.configure(this.configs.head.originals())
    authorizer.addAcls(Set(new AuthAcl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*"), Allow,
                            AuthAcl.WildCardHost, All)), new AuthResource(Topic, "*"))
    authorizer.addAcls(Set(clusterAcl(Allow, Create),
                           clusterAcl(Allow, Delete),
                           clusterAcl(Allow, ClusterAction),
                           clusterAcl(Allow, AlterConfigs),
                           clusterAcl(Allow, Alter)),
                       AuthResource.ClusterResource)
  }

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  private def clusterAcl(permissionType: PermissionType, operation: Operation): AuthAcl = {
    new AuthAcl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*"), permissionType,
      AuthAcl.WildCardHost, operation)
  }

  private def addClusterAcl(permissionType: PermissionType, operation: Operation): Unit = {
    val acls = Set(clusterAcl(permissionType, operation))
    val authorizer = servers.head.apis.authorizer.get
    val prevAcls = authorizer.getAcls(AuthResource.ClusterResource)
    authorizer.addAcls(acls, AuthResource.ClusterResource)
    TestUtils.waitAndVerifyAcls(prevAcls  ++ acls, authorizer, AuthResource.ClusterResource)
  }

  private def removeClusterAcl(permissionType: PermissionType, operation: Operation): Unit = {
    val acls = Set(clusterAcl(permissionType, operation))
    val authorizer = servers.head.apis.authorizer.get
    val prevAcls = authorizer.getAcls(AuthResource.ClusterResource)
    Assert.assertTrue(authorizer.removeAcls(acls, AuthResource.ClusterResource))
    TestUtils.waitAndVerifyAcls(prevAcls -- acls, authorizer, AuthResource.ClusterResource)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  val ACL2 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic2"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW));
  val ACL3 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW));
  val ACL_UNKNOWN = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW));

  @Test
  override def testAclOperations(): Unit = {
    client = AdminClient.create(createConfig())
    assertEquals(6, client.describeAcls(AclBindingFilter.ANY).all().get().size())
    val results = client.createAcls(List(ACL2, ACL3).asJava)
    assertEquals(Set(ACL2, ACL3), results.results().keySet().asScala)
    results.results().values().asScala.foreach(value => value.get)
    val results2 = client.createAcls(List(ACL_UNKNOWN).asJava)
    assertEquals(Set(ACL_UNKNOWN), results2.results().keySet().asScala)
    assertFutureExceptionTypeEquals(results2.all(), classOf[InvalidRequestException])
    val results3 = client.deleteAcls(List(ACL1.toFilter, ACL2.toFilter, ACL3.toFilter).asJava)
    assertEquals(Set(ACL1.toFilter, ACL2.toFilter, ACL3.toFilter), results3.results().keySet().asScala)
    assertEquals(0, results3.results.get(ACL1.toFilter).get.acls.size())
    assertEquals(Set(ACL2), results3.results.get(ACL2.toFilter).get.acls.asScala.map(result => result.acl()).toSet)
    assertEquals(Set(ACL3), results3.results.get(ACL3.toFilter).get.acls.asScala.map(result => result.acl()).toSet)
    client.close()
  }

  def waitForDescribeAcls(client: AdminClient, filter: AclBindingFilter, acls: Set[AclBinding]): Unit = {
    TestUtils.waitUntilTrue(() => {
      val results = client.describeAcls(filter).all().get()
      acls == results.asScala.toSet
    }, "timed out waiting for ACLs")
  }

  @Test
  def testAclOperations2(): Unit = {
    client = AdminClient.create(createConfig())
    val results = client.createAcls(List(ACL2, ACL2).asJava)
    assertEquals(Set(ACL2, ACL2), results.results().keySet().asScala)
    results.all().get()
    waitForDescribeAcls(client, ACL2.toFilter, Set(ACL2))

    val filterA = new AclBindingFilter(new ResourceFilter(ResourceType.GROUP, null), AccessControlEntryFilter.ANY)
    val filterB = new AclBindingFilter(new ResourceFilter(ResourceType.TOPIC, "mytopic2"), AccessControlEntryFilter.ANY)

    waitForDescribeAcls(client, filterA, Set())

    val results2 = client.deleteAcls(List(filterA, filterB).asJava, new DeleteAclsOptions())
    assertEquals(Set(filterA, filterB), results2.results().keySet().asScala)
    assertEquals(Set(), results2.results.get(filterA).get.acls.asScala.map(result => result.acl()).toSet)
    assertEquals(Set(ACL2), results2.results.get(filterB).get.acls.asScala.map(result => result.acl()).toSet)

    waitForDescribeAcls(client, filterB, Set())

    client.close()
  }

  @Test
  def testAttemptToCreateInvalidAcls(): Unit = {
    client = AdminClient.create(createConfig())
    val clusterAcl = new AclBinding(new Resource(ResourceType.CLUSTER, "foobar"),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val emptyResourceNameAcl = new AclBinding(new Resource(ResourceType.TOPIC, ""),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val results = client.createAcls(List(clusterAcl, emptyResourceNameAcl).asJava, new CreateAclsOptions())
    assertEquals(Set(clusterAcl, emptyResourceNameAcl), results.results().keySet().asScala)
    assertFutureExceptionTypeEquals(results.results().get(clusterAcl), classOf[InvalidRequestException])
    assertFutureExceptionTypeEquals(results.results().get(emptyResourceNameAcl), classOf[InvalidRequestException])
  }

  val FOO_ACL = new AclBinding(new Resource(ResourceType.TOPIC, "foobar"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val FOO_ACL_FILTER = FOO_ACL.toFilter

  private def verifyCauseIsClusterAuth(e: Exception): Unit = {
    if (!e.getCause.isInstanceOf[ClusterAuthorizationException]) {
      throw e.getCause
    }
  }

  private def testAclCreateGetDelete(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val result = client.createAcls(List(FOO_ACL).asJava, new CreateAclsOptions())
      if (expectAuth) {
        Try(result.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            false
          }
          case Success(_) => true
        }
      } else {
        Try(result.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            true
          }
          case Success(_) => false
        }
      }
    }, "timed out waiting for createAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, FOO_ACL_FILTER, Set(FOO_ACL))
    }
    TestUtils.waitUntilTrue(() => {
      val result = client.deleteAcls(List(FOO_ACL.toFilter).asJava, new DeleteAclsOptions())
      if (expectAuth) {
        Try(result.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            false
          }
          case Success(_) => true
        }
      } else {
        Try(result.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            true
          }
          case Success(removed) => {
            assertEquals(Set(FOO_ACL), result.results.get(FOO_ACL_FILTER).get.acls.asScala.map(result => result.acl()).toSet)
            true
          }
        }
      }
    }, "timed out waiting for deleteAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, FOO_ACL_FILTER, Set())
    }
  }

  private def testAclGet(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val userAcl = new AclBinding(new Resource(ResourceType.TOPIC, "*"),
        new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      val results = client.describeAcls(userAcl.toFilter)
      if (expectAuth) {
        Try(results.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            false
          }
          case Success(acls) => Set(userAcl).equals(acls.asScala.toSet)
        }
      } else {
        Try(results.all().get) match {
          case Failure(e: Exception) => {
            verifyCauseIsClusterAuth(e)
            true
          }
          case Success(_) => false
        }
      }
    }, "timed out waiting for describeAcls to " + (if (expectAuth) "succeed" else "fail"))
  }

  @Test
  def testAclAuthorizationDenied(): Unit = {
    client = AdminClient.create(createConfig())

    // Test that we cannot create or delete ACLs when Alter is denied.
    addClusterAcl(Deny, Alter)
    testAclGet(true)
    testAclCreateGetDelete(false)

    // Test that we cannot do anything with ACLs when Describe is denied.
    removeClusterAcl(Deny, Alter)
    addClusterAcl(Deny, Describe)
    testAclGet(false)
    testAclCreateGetDelete(false)

    // Test that we can create, delete, and get ACLs with the default ACLs.
    removeClusterAcl(Deny, Describe)
    testAclGet(true)
    testAclCreateGetDelete(true)

    // Test that we can't do anything with ACLs without the Allow Alter ACL in place.
    removeClusterAcl(Allow, Alter)
    removeClusterAcl(Allow, Delete)
    testAclGet(false)
    testAclCreateGetDelete(false)

    // Test that we can describe, but not alter ACLs, with only the Allow Describe ACL in place.
    addClusterAcl(Allow, Describe)
    testAclGet(true)
    testAclCreateGetDelete(false)

    removeClusterAcl(Allow, Describe)
    addClusterAcl(Allow, Alter)
  }
}
