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
package kafka.zk.migration

import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.utils.TestUtils
import org.apache.kafka.common.acl._
import org.apache.kafka.common.metadata.AccessControlEntryRecord
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ZkAclMigrationClientTest extends ZkMigrationTestHarness {
  def migrateAclsAndVerify(authorizer: AclAuthorizer, acls: Seq[AclBinding]): Unit = {
    authorizer.createAcls(null, acls.asJava)
    val batches = new mutable.ArrayBuffer[mutable.Buffer[ApiMessageAndVersion]]()
    migrationClient.migrateAcls(batch => batches.append(batch.asScala))
    val records = batches.flatten.map(_.message().asInstanceOf[AccessControlEntryRecord])
    assertEquals(acls.size, records.size, "Expected one record for each ACLBinding")
  }

  def replaceAclsAndReadWithAuthorizer(
    authorizer: AclAuthorizer,
    resourcePattern: ResourcePattern,
    aces: Seq[AccessControlEntry],
    pred: Seq[AclBinding] => Boolean
  ): Seq[AclBinding] = {
    val resourceFilter = new AclBindingFilter(
      new ResourcePatternFilter(resourcePattern.resourceType(), resourcePattern.name(), resourcePattern.patternType()),
      AclBindingFilter.ANY.entryFilter()
    )
    migrationState = migrationClient.aclClient().writeResourceAcls(resourcePattern, aces.asJava, migrationState)
    val (acls, ok) = TestUtils.computeUntilTrue(authorizer.acls(resourceFilter).asScala.toSeq)(pred)
    assertTrue(ok)
    acls
  }

  def deleteResourceAndReadWithAuthorizer(
    authorizer: AclAuthorizer,
    resourcePattern: ResourcePattern
  ): Unit = {
    val resourceFilter = new AclBindingFilter(
      new ResourcePatternFilter(resourcePattern.resourceType(), resourcePattern.name(), resourcePattern.patternType()),
      AclBindingFilter.ANY.entryFilter()
    )
    migrationState = migrationClient.aclClient().deleteResource(resourcePattern, migrationState)
    val (_, ok) = TestUtils.computeUntilTrue(authorizer.acls(resourceFilter).asScala.toSeq)(_.isEmpty)
    assertTrue(ok)
  }


  @Test
  def testAclsMigrateAndDualWrite(): Unit = {
    val resource1 = new ResourcePattern(ResourceType.TOPIC, "foo-" + UUID.randomUUID(), PatternType.LITERAL)
    val resource2 = new ResourcePattern(ResourceType.TOPIC, "bar-" + UUID.randomUUID(), PatternType.LITERAL)
    val prefixedResource = new ResourcePattern(ResourceType.TOPIC, "bar-", PatternType.PREFIXED)
    val username = "alice"
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username)
    val wildcardPrincipal = SecurityUtils.parseKafkaPrincipal(WildcardPrincipalString)

    val ace1 = new AccessControlEntry(principal.toString, WildcardHost, AclOperation.READ, AclPermissionType.ALLOW)
    val acl1 = new AclBinding(resource1, ace1)
    val ace2 = new AccessControlEntry(principal.toString, "192.168.0.1", AclOperation.WRITE, AclPermissionType.ALLOW)
    val acl2 = new AclBinding(resource1, ace2)
    val acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString, WildcardHost, AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    val acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString, WildcardHost, AclOperation.READ, AclPermissionType.ALLOW))

    val authorizer = new AclAuthorizer()
    try {
      authorizer.configure(Map("zookeeper.connect" -> this.zkConnect).asJava)

      // Migrate ACLs
      migrateAclsAndVerify(authorizer, Seq(acl1, acl2, acl3, acl4))

      // Remove one of resource1's ACLs
      var resource1Acls = replaceAclsAndReadWithAuthorizer(authorizer, resource1, Seq(ace1), acls => acls.size == 1)
      assertEquals(acl1, resource1Acls.head)

      // Delete the other ACL from resource1
      deleteResourceAndReadWithAuthorizer(authorizer, resource1)

      // Add a new ACL for resource1
      val newAce1 = new AccessControlEntry(principal.toString, "10.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW)
      resource1Acls = replaceAclsAndReadWithAuthorizer(authorizer, resource1, Seq(newAce1), acls => acls.size == 1)
      assertEquals(newAce1, resource1Acls.head.entry())

      // Add a new ACL for resource2
      val newAce2 = new AccessControlEntry(principal.toString, "10.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW)
      val resource2Acls = replaceAclsAndReadWithAuthorizer(authorizer, resource2, Seq(acl3.entry(), newAce2), acls => acls.size == 2)
      assertEquals(acl3, resource2Acls.head)
      assertEquals(newAce2, resource2Acls.last.entry())
    } finally {
      authorizer.close()
    }
  }
}
