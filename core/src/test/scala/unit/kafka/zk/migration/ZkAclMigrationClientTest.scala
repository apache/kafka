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

import kafka.security.authorizer.{AclAuthorizer, AclEntry}
import kafka.security.authorizer.AclEntry.{WildcardHost, WildcardPrincipalString}
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl._
import org.apache.kafka.common.metadata.{AccessControlEntryRecord, RemoveAccessControlEntryRecord}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.migration.KRaftMigrationZkWriter
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.Test

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
    pred: Set[AclBinding] => Boolean
  ): Set[AclBinding] = {
    val resourceFilter = new AclBindingFilter(
      new ResourcePatternFilter(resourcePattern.resourceType(), resourcePattern.name(), resourcePattern.patternType()),
      AclBindingFilter.ANY.entryFilter()
    )
    migrationState = migrationClient.aclClient().writeResourceAcls(resourcePattern, aces.asJava, migrationState)
    val (acls, ok) = TestUtils.computeUntilTrue(authorizer.acls(resourceFilter).asScala.toSet)(pred)
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
    val (_, ok) = TestUtils.computeUntilTrue(authorizer.acls(resourceFilter).asScala.toSet)(_.isEmpty)
    assertTrue(ok)
  }


  @Test
  def testAclsMigrateAndDualWrite(): Unit = {
    val resource1 = new ResourcePattern(ResourceType.TOPIC, "foo-" + Uuid.randomUuid(), PatternType.LITERAL)
    val resource2 = new ResourcePattern(ResourceType.TOPIC, "bar-" + Uuid.randomUuid(), PatternType.LITERAL)
    val prefixedResource = new ResourcePattern(ResourceType.TOPIC, "bar-" + Uuid.randomUuid(), PatternType.PREFIXED)
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
      assertTrue(resource1Acls.contains(acl1))

      // Delete the other ACL from resource1
      deleteResourceAndReadWithAuthorizer(authorizer, resource1)

      // Add a new ACL for resource1
      val newAce1 = new AccessControlEntry(principal.toString, "10.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW)
      resource1Acls = replaceAclsAndReadWithAuthorizer(authorizer, resource1, Seq(newAce1), acls => acls.size == 1)
      assertTrue(resource1Acls.map(_.entry()).contains(newAce1))

      // Add a new ACL for resource2
      val newAce2 = new AccessControlEntry(principal.toString, "10.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW)
      val resource2Acls = replaceAclsAndReadWithAuthorizer(authorizer, resource2, Seq(acl3.entry(), newAce2), acls => acls.size == 2)
      assertTrue(resource2Acls.map(_.entry()).subsetOf(Set(acl3.entry(), newAce2)))
    } finally {
      authorizer.close()
    }
  }


  @Test
  def testAclsChangesInSnapshot(): Unit = {
    // Create some ACLs in Zookeeper.
    val resource1 = new ResourcePattern(ResourceType.TOPIC, "foo-" + Uuid.randomUuid(), PatternType.LITERAL)
    val resource2 = new ResourcePattern(ResourceType.TOPIC, "bar-" + Uuid.randomUuid(), PatternType.LITERAL)
    val resource3 = new ResourcePattern(ResourceType.TOPIC, "baz-" + Uuid.randomUuid(), PatternType.LITERAL)
    val username1 = "alice"
    val username2 = "blah"
    val principal1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username1)
    val principal2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username2)
    val acl1Resource1 = new AclEntry(new AccessControlEntry(principal1.toString, WildcardHost, AclOperation.WRITE, AclPermissionType.ALLOW))
    val acl1Resource2 = new AclEntry(new AccessControlEntry(principal2.toString, WildcardHost, AclOperation.READ, AclPermissionType.ALLOW))

    zkClient.createAclPaths()
    zkClient.createAclsForResourceIfNotExists(resource1, Set(acl1Resource1))
    zkClient.createAclsForResourceIfNotExists(resource2, Set(acl1Resource2))

    // Create a metadata image such that ACLs for one resource are update, one resource is deleted
    // one new resource is created in Zookeeper.

    // Create an ACL for a new resource.
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    val acl1Resource3 = new AccessControlEntryRecord()
      .setId(Uuid.randomUuid())
      .setHost("192.168.10.1")
      .setOperation(AclOperation.READ.code())
      .setPrincipal(WildcardPrincipalString)
      .setPermissionType(AclPermissionType.ALLOW.code())
      .setPatternType(resource3.patternType().code())
      .setResourceName(resource3.name())
      .setResourceType(resource3.resourceType().code()
      )
    delta.replay(acl1Resource3)

    // Change an ACL for existing resource.
    val acl2Resource1 = new AccessControlEntryRecord()
      .setId(Uuid.randomUuid())
      .setHost("192.168.15.1")
      .setOperation(AclOperation.WRITE.code())
      .setPrincipal(principal1.toString)
      .setPermissionType(AclPermissionType.ALLOW.code())
      .setPatternType(resource1.patternType().code())
      .setResourceName(resource1.name())
      .setResourceType(resource1.resourceType().code()
      )
    delta.replay(acl2Resource1)

    // Do not add anything for resource 2 in the delta.
    val image = delta.apply(MetadataProvenance.EMPTY)

    // load snapshot to Zookeeper.
    val kraftMigrationZkWriter = new KRaftMigrationZkWriter(migrationClient, fail(_))
    kraftMigrationZkWriter.handleSnapshot(image, (_, _, operation) => { migrationState = operation.apply(migrationState) })

    // Verify the new ACLs in Zookeeper.
    val resource1AclsInZk = zkClient.getVersionedAclsForResource(resource1).acls
    assertEquals(1, resource1AclsInZk.size)
    assertEquals(
      new AccessControlEntry(acl2Resource1.principal(), acl2Resource1.host(),
        AclOperation.fromCode(acl2Resource1.operation()),
        AclPermissionType.fromCode(acl2Resource1.permissionType())),
      resource1AclsInZk.head.ace)
    val resource2AclsInZk = zkClient.getVersionedAclsForResource(resource2).acls
    assertTrue(resource2AclsInZk.isEmpty)
    val resource3AclsInZk = zkClient.getVersionedAclsForResource(resource3).acls
    assertEquals(
      new AccessControlEntry(acl1Resource3.principal(), acl1Resource3.host(),
        AclOperation.fromCode(acl1Resource3.operation()),
        AclPermissionType.fromCode(acl1Resource3.permissionType())),
      resource3AclsInZk.head.ace)
  }

  def user(user: String): String = {
    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user).toString
  }

  def acl(resourceName: String,
          resourceType: ResourceType,
          resourcePattern: PatternType,
          principal: String,
          host: String = "*",
          operation: AclOperation = AclOperation.READ,
          permissionType: AclPermissionType = AclPermissionType.ALLOW
  ): AccessControlEntryRecord = {
    new AccessControlEntryRecord()
      .setId(Uuid.randomUuid())
      .setHost(host)
      .setOperation(operation.code())
      .setPrincipal(principal)
      .setPermissionType(permissionType.code())
      .setPatternType(resourcePattern.code())
      .setResourceName(resourceName)
      .setResourceType(resourceType.code())
  }

  @Test
  def testDeleteOneAclOfMany(): Unit = {
    zkClient.createAclPaths()
    val topicName = "topic-" + Uuid.randomUuid()
    val resource = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL)

    // Create a delta with some ACLs
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    val acl1 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("alice"))
    val acl2 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("bob"))
    val acl3 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("carol"))
    delta.replay(acl1)
    delta.replay(acl2)
    delta.replay(acl3)
    val image = delta.apply(MetadataProvenance.EMPTY)

    // Sync image to ZK
    val errorLogs = mutable.Buffer[String]()
    val kraftMigrationZkWriter = new KRaftMigrationZkWriter(migrationClient, msg => errorLogs.append(msg))
    kraftMigrationZkWriter.handleSnapshot(image, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })

    // verify 3 ACLs in ZK
    val aclsInZk = zkClient.getVersionedAclsForResource(resource).acls
    assertEquals(3, aclsInZk.size)

    // Delete one of the ACLs
    val delta2 = new MetadataDelta.Builder()
      .setImage(image)
      .build()
    delta2.replay(new RemoveAccessControlEntryRecord().setId(acl3.id()))
    val image2 = delta2.apply(MetadataProvenance.EMPTY)
    kraftMigrationZkWriter.handleDelta(image, image2, delta2, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })

    // verify the other 2 ACLs are still in ZK
    val aclsInZk2 = zkClient.getVersionedAclsForResource(resource).acls
    assertEquals(2, aclsInZk2.size)
    assertEquals(0, errorLogs.size)

    // Add another ACL
    val acl4 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("carol"))
    delta2.replay(acl4)
    val image3 = delta2.apply(MetadataProvenance.EMPTY)

    // This is a contrived error case. In practice, we will never pass the same image as prev and current.
    // The point of this is to exercise the case of a deleted ACL missing from the prev image.
    kraftMigrationZkWriter.handleDelta(image3, image3, delta2, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })

    val aclsInZk3 = zkClient.getVersionedAclsForResource(resource).acls
    assertEquals(3, aclsInZk3.size)
    assertEquals(1, errorLogs.size)
    assertEquals(s"Cannot delete ACL ${acl3.id()} from ZK since it is missing from previous AclImage", errorLogs.head)
  }

  @Test
  def testAclUpdateAndDelete(): Unit = {
    zkClient.createAclPaths()
    val errorLogs = mutable.Buffer[String]()
    val kraftMigrationZkWriter = new KRaftMigrationZkWriter(migrationClient, msg => errorLogs.append(msg))

    val topicName = "topic-" + Uuid.randomUuid()
    val otherName = "other-" + Uuid.randomUuid()
    val literalResource = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL)
    val prefixedResource = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.PREFIXED)
    val otherResource = new ResourcePattern(ResourceType.TOPIC, otherName, PatternType.LITERAL)

    // Create a delta with some ACLs
    val acl1 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("alice"))
    val acl2 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("bob"))
    val acl3 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("carol"))
    val acl4 = acl(topicName, ResourceType.TOPIC, PatternType.LITERAL, user("dave"))

    val delta1 = new MetadataDelta(MetadataImage.EMPTY)
    delta1.replay(acl1)
    delta1.replay(acl2)
    delta1.replay(acl3)
    delta1.replay(acl4)

    val image1 = delta1.apply(MetadataProvenance.EMPTY)
    kraftMigrationZkWriter.handleDelta(MetadataImage.EMPTY, image1, delta1, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })
    assertEquals(4, zkClient.getVersionedAclsForResource(literalResource).acls.size)
    assertEquals(0, zkClient.getVersionedAclsForResource(prefixedResource).acls.size)
    assertEquals(0, zkClient.getVersionedAclsForResource(otherResource).acls.size)
    assertEquals(0, errorLogs.size)

    val acl5 = acl(topicName, ResourceType.TOPIC, PatternType.PREFIXED, user("alice"))
    val acl6 = acl(topicName, ResourceType.TOPIC, PatternType.PREFIXED, user("bob"))
    val acl7 = acl(otherName, ResourceType.TOPIC, PatternType.LITERAL, user("carol"))
    val acl8 = acl(otherName, ResourceType.TOPIC, PatternType.LITERAL, user("dave"))

    // Add two prefixed and two "other" ACLs, delete one of the literal ACLs
    val delta2 = new MetadataDelta.Builder().setImage(image1).build()
    delta2.replay(acl5)
    delta2.replay(acl6)
    delta2.replay(acl7)
    delta2.replay(acl8)
    delta2.replay(new RemoveAccessControlEntryRecord().setId(acl1.id()))

    val image2 = delta2.apply(MetadataProvenance.EMPTY)
    kraftMigrationZkWriter.handleDelta(image1, image2, delta2, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })
    assertEquals(3, zkClient.getVersionedAclsForResource(literalResource).acls.size)
    assertEquals(2, zkClient.getVersionedAclsForResource(prefixedResource).acls.size)
    assertEquals(2, zkClient.getVersionedAclsForResource(otherResource).acls.size)
    assertEquals(0, errorLogs.size)

    // Delete and add ACL for literal resource, remove both prefixed ACLs, add another "other"
    val acl9 = acl(otherName, ResourceType.TOPIC, PatternType.LITERAL, user("eve"))
    val delta3 = new MetadataDelta.Builder().setImage(image2).build()
    delta3.replay(acl1)
    delta3.replay(new RemoveAccessControlEntryRecord().setId(acl2.id()))
    delta3.replay(new RemoveAccessControlEntryRecord().setId(acl5.id()))
    delta3.replay(new RemoveAccessControlEntryRecord().setId(acl6.id()))
    delta3.replay(acl9)

    val image3 = delta3.apply(MetadataProvenance.EMPTY)
    kraftMigrationZkWriter.handleDelta(image2, image3, delta3, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })
    assertEquals(3, zkClient.getVersionedAclsForResource(literalResource).acls.size)
    assertEquals(0, zkClient.getVersionedAclsForResource(prefixedResource).acls.size)
    assertEquals(3, zkClient.getVersionedAclsForResource(otherResource).acls.size)
    assertEquals(0, errorLogs.size)
  }
}
