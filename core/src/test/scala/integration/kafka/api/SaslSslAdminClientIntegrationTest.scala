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

import kafka.security.auth.{All, Allow, Alter, AlterConfigs, Authorizer, ClusterAction, Create, Delete, Deny, Describe, Operation, PermissionType, SimpleAclAuthorizer, Topic, Acl => AuthAcl, Resource => AuthResource}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, CreateAclsOptions, DeleteAclsOptions}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException}
import org.apache.kafka.common.resource.{Resource, ResourceFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.junit.Assert.assertEquals
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SaslSslAdminClientIntegrationTest extends AdminClientIntegrationTest with SaslSetup {
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  override def configureSecurityBeforeServersStart() {
    val authorizer = CoreUtils.createObject[Authorizer](classOf[SimpleAclAuthorizer].getName)
    try {
      authorizer.configure(this.configs.head.originals())
      authorizer.addAcls(Set(new AuthAcl(AuthAcl.WildCardPrincipal, Allow,
                              AuthAcl.WildCardHost, All)), new AuthResource(Topic, "*"))
      authorizer.addAcls(Set(clusterAcl(Allow, Create),
                             clusterAcl(Allow, Delete),
                             clusterAcl(Allow, ClusterAction),
                             clusterAcl(Allow, AlterConfigs),
                             clusterAcl(Allow, Alter)),
                         AuthResource.ClusterResource)
    } finally {
      authorizer.close()
    }
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
    TestUtils.waitAndVerifyAcls(prevAcls ++ acls, authorizer, AuthResource.ClusterResource)
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

  val acl2 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic2"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val acl3 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val fooAcl = new AclBinding(new Resource(ResourceType.TOPIC, "foobar"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val transactionalIdAcl = new AclBinding(new Resource(ResourceType.TRANSACTIONAL_ID, "transactional_id"),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))

  @Test
  override def testAclOperations(): Unit = {
    client = AdminClient.create(createConfig())
    assertEquals(6, client.describeAcls(AclBindingFilter.ANY).values.get().size)
    val results = client.createAcls(List(acl2, acl3).asJava)
    assertEquals(Set(acl2, acl3), results.values.keySet().asScala)
    results.values.values().asScala.foreach(value => value.get)
    val aclUnknown = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW))
    val results2 = client.createAcls(List(aclUnknown).asJava)
    assertEquals(Set(aclUnknown), results2.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results2.all, classOf[InvalidRequestException])
    val results3 = client.deleteAcls(List(ACL1.toFilter, acl2.toFilter, acl3.toFilter).asJava).values
    assertEquals(Set(ACL1.toFilter, acl2.toFilter, acl3.toFilter), results3.keySet.asScala)
    assertEquals(0, results3.get(ACL1.toFilter).get.values.size())
    assertEquals(Set(acl2), results3.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results3.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
  }

  def waitForDescribeAcls(client: AdminClient, filter: AclBindingFilter, acls: Set[AclBinding]): Unit = {
    TestUtils.waitUntilTrue(() => {
      val results = client.describeAcls(filter).values.get()
      acls == results.asScala.toSet
    }, s"timed out waiting for ACLs $acls")
  }

  @Test
  def testAclOperations2(): Unit = {
    client = AdminClient.create(createConfig())
    val results = client.createAcls(List(acl2, acl2, transactionalIdAcl).asJava)
    assertEquals(Set(acl2, acl2, transactionalIdAcl), results.values.keySet.asScala)
    results.all.get()
    waitForDescribeAcls(client, acl2.toFilter, Set(acl2))
    waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))

    val filterA = new AclBindingFilter(new ResourceFilter(ResourceType.GROUP, null), AccessControlEntryFilter.ANY)
    val filterB = new AclBindingFilter(new ResourceFilter(ResourceType.TOPIC, "mytopic2"), AccessControlEntryFilter.ANY)
    val filterC = new AclBindingFilter(new ResourceFilter(ResourceType.TRANSACTIONAL_ID, null), AccessControlEntryFilter.ANY)

    waitForDescribeAcls(client, filterA, Set())
    waitForDescribeAcls(client, filterC, Set(transactionalIdAcl))

    val results2 = client.deleteAcls(List(filterA, filterB, filterC).asJava, new DeleteAclsOptions())
    assertEquals(Set(filterA, filterB, filterC), results2.values.keySet.asScala)
    assertEquals(Set(), results2.values.get(filterA).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(transactionalIdAcl), results2.values.get(filterC).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl2), results2.values.get(filterB).get.values.asScala.map(_.binding).toSet)

    waitForDescribeAcls(client, filterB, Set())
    waitForDescribeAcls(client, filterC, Set())
  }

  @Test
  def testAttemptToCreateInvalidAcls(): Unit = {
    client = AdminClient.create(createConfig())
    val clusterAcl = new AclBinding(new Resource(ResourceType.CLUSTER, "foobar"),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val emptyResourceNameAcl = new AclBinding(new Resource(ResourceType.TOPIC, ""),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val results = client.createAcls(List(clusterAcl, emptyResourceNameAcl).asJava, new CreateAclsOptions())
    assertEquals(Set(clusterAcl, emptyResourceNameAcl), results.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results.values.get(clusterAcl), classOf[InvalidRequestException])
    assertFutureExceptionTypeEquals(results.values.get(emptyResourceNameAcl), classOf[InvalidRequestException])
  }

  private def verifyCauseIsClusterAuth(e: Throwable): Unit = {
    if (!e.getCause.isInstanceOf[ClusterAuthorizationException]) {
      throw e.getCause
    }
  }

  private def testAclCreateGetDelete(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val result = client.createAcls(List(fooAcl, transactionalIdAcl).asJava, new CreateAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for createAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set(fooAcl))
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))
    }
    TestUtils.waitUntilTrue(() => {
      val result = client.deleteAcls(List(fooAcl.toFilter, transactionalIdAcl.toFilter).asJava, new DeleteAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) =>
            assertEquals(Set(fooAcl, transactionalIdAcl), result.values.keySet)
            assertEquals(Set(fooAcl), result.values.get(fooAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            assertEquals(Set(transactionalIdAcl),
              result.values.get(transactionalIdAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            true
        }
      }
    }, "timed out waiting for deleteAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set.empty)
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set.empty)
    }
  }

  private def testAclGet(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val userAcl = new AclBinding(new Resource(ResourceType.TOPIC, "*"),
        new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      val results = client.describeAcls(userAcl.toFilter)
      if (expectAuth) {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(acls) => Set(userAcl).equals(acls.asScala.toSet)
        }
      } else {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
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
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we cannot do anything with ACLs when Describe and Alter are denied.
    addClusterAcl(Deny, Describe)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can create, delete, and get ACLs with the default ACLs.
    removeClusterAcl(Deny, Describe)
    removeClusterAcl(Deny, Alter)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = true)

    // Test that we can't do anything with ACLs without the Allow Alter ACL in place.
    removeClusterAcl(Allow, Alter)
    removeClusterAcl(Allow, Delete)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can describe, but not alter ACLs, with only the Allow Describe ACL in place.
    addClusterAcl(Allow, Describe)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)
  }
}
