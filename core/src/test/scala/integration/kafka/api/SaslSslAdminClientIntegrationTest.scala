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

import kafka.security.auth.SimpleAclAuthorizer
import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, CreateAclsOptions, DeleteAclsOptions}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.resource.{Resource, ResourceFilter, ResourceType}
import org.junit.Assert.assertEquals
import org.junit.{After, Assert, Before, Test}

import scala.collection.JavaConverters._

class SaslSslAdminClientIntegrationTest extends KafkaAdminClientIntegrationTest with SaslSetup {
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName())
  this.serverConfig.setProperty(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp, "true")

  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
    super.setUp()
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

  /**
    * Test that ACL operations are not possible when the authorizer is disabled.
    * Also see {@link kafka.api.KafkaAdminClientSecureIntegrationTest} for tests of ACL operations
    * when the authorizer is enabled.
    */
  @Test
  override def testAclOperations(): Unit = {
    client = AdminClient.create(createConfig())
    assertEquals(0, client.describeAcls(AclBindingFilter.ANY).all().get().size())
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
    waitForDescribeAcls(client, AclBindingFilter.ANY, Set(ACL2))

    val filterA = new AclBindingFilter(new ResourceFilter(ResourceType.CLUSTER, null), AccessControlEntryFilter.ANY)
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
}
