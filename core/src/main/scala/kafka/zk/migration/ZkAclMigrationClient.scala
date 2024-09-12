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

package kafka.zk.migration

import kafka.security.authorizer.AclAuthorizer.{ResourceOrdering, VersionedAcls}
import kafka.security.authorizer.AclAuthorizer
import kafka.utils.Logging
import kafka.zk.ZkMigrationClient.{logAndRethrow, wrapZkException}
import kafka.zk.{KafkaZkClient, ResourceZNode, ZkAclStore, ZkVersion}
import kafka.zookeeper.{CreateRequest, DeleteRequest, SetDataRequest}
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.metadata.migration.{AclMigrationClient, MigrationClientException, ZkMigrationLeadershipState}
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.Code

import java.util
import java.util.function.BiConsumer
import scala.jdk.CollectionConverters._

class ZkAclMigrationClient(
  zkClient: KafkaZkClient
) extends AclMigrationClient with Logging {

  private def aclChangeNotificationRequest(resourcePattern: ResourcePattern): CreateRequest = {
    // ZK broker needs the ACL change notification znode to be updated in order to process the new ACLs
    val aclChange = ZkAclStore(resourcePattern.patternType).changeStore.createChangeNode(resourcePattern)
    CreateRequest(aclChange.path, aclChange.bytes, zkClient.defaultAcls(aclChange.path), CreateMode.PERSISTENT_SEQUENTIAL)
  }

  private def tryWriteAcls(
    resourcePattern: ResourcePattern,
    aclEntries: Set[AclEntry],
    create: Boolean,
    state: ZkMigrationLeadershipState
  ): Option[ZkMigrationLeadershipState] = wrapZkException {
    val aclData = ResourceZNode.encode(aclEntries)

    val request = if (create) {
      val path = ResourceZNode.path(resourcePattern)
      CreateRequest(path, aclData, zkClient.defaultAcls(path), CreateMode.PERSISTENT)
    } else {
      SetDataRequest(ResourceZNode.path(resourcePattern), aclData, ZkVersion.MatchAnyVersion)
    }

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    if (responses.head.resultCode.equals(Code.NONODE)) {
      // Need to call this method again with create=true
      None
    } else {
      // Write the ACL notification outside of a metadata multi-op
      zkClient.retryRequestUntilConnected(aclChangeNotificationRequest(resourcePattern))
      Some(state.withMigrationZkVersion(migrationZkVersion))
    }
  }

  override def writeResourceAcls(
    resourcePattern: ResourcePattern,
    aclsToWrite: util.Collection[AccessControlEntry],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {
    val acls = aclsToWrite.asScala.map(new AclEntry(_)).toSet
    tryWriteAcls(resourcePattern, acls, create = false, state) match {
      case Some(newState) => newState
      case None => tryWriteAcls(resourcePattern, acls, create = true, state) match {
        case Some(newState) => newState
        case None => throw new MigrationClientException(s"Could not write ACLs for resource pattern $resourcePattern")
      }
    }
  }

  override def deleteResource(
    resourcePattern: ResourcePattern,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {
    val request = DeleteRequest(ResourceZNode.path(resourcePattern), ZkVersion.MatchAnyVersion)
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    if (responses.head.resultCode.equals(Code.OK) || responses.head.resultCode.equals(Code.NONODE)) {
      // Write the ACL notification outside of a metadata multi-op
      zkClient.retryRequestUntilConnected(aclChangeNotificationRequest(resourcePattern))
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Could not delete ACL for resource pattern $resourcePattern")
    }
  }

  override def iterateAcls(
    aclConsumer: BiConsumer[ResourcePattern, util.Set[AccessControlEntry]]
  ): Unit = {
    // This is probably fairly inefficient, but it preserves the semantics from AclAuthorizer (which is non-trivial)
    var allAcls = new scala.collection.immutable.TreeMap[ResourcePattern, VersionedAcls]()(new ResourceOrdering)
    def updateAcls(resourcePattern: ResourcePattern, versionedAcls: VersionedAcls): Unit = {
      allAcls = allAcls.updated(resourcePattern, versionedAcls)
    }
    AclAuthorizer.loadAllAcls(zkClient, this, updateAcls)
    allAcls.foreach { case (resourcePattern, versionedAcls) =>
      logAndRethrow(this, s"Error in ACL consumer. Resource was $resourcePattern.") {
        aclConsumer.accept(resourcePattern, versionedAcls.acls.map(_.ace).asJava)
      }
    }
  }
}
