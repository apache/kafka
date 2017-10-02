/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}

import kafka.network.RequestChannel.Session
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import scala.collection.JavaConverters._
import org.apache.log4j.Logger

import scala.util.Random

object SimpleAclAuthorizer {
  //optional override zookeeper cluster configuration where acls will be stored, if not specified acls will be stored in
  //same zookeeper where all other kafka broker info is stored.
  val ZkUrlProp = "authorizer.zookeeper.url"
  val ZkConnectionTimeOutProp = "authorizer.zookeeper.connection.timeout.ms"
  val ZkSessionTimeOutProp = "authorizer.zookeeper.session.timeout.ms"

  //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
  val SuperUsersProp = "super.users"
  //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
  val AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found"

  /**
   * The root acl storage node. Under this node there will be one child node per resource type (Topic, Cluster, Group).
   * under each resourceType there will be a unique child for each resource instance and the data for that child will contain
   * list of its acls as a json object. Following gives an example:
   *
   * <pre>
   * /kafka-acl/Topic/topic-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * /kafka-acl/Cluster/kafka-cluster => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * /kafka-acl/Group/group-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * </pre>
   */
  val AclZkPath = ZkUtils.KafkaAclPath

  //notification node which gets updated with the resource name when acl on a resource is changed.
  val AclChangedZkPath = ZkUtils.KafkaAclChangesPath

  //prefix of all the change notification sequence node.
  val AclChangedPrefix = "acl_changes_"

  private case class VersionedAcls(acls: Set[Acl], zkVersion: Int)
}

class SimpleAclAuthorizer extends Authorizer with Logging {
  private val authorizerLogger = Logger.getLogger("kafka.authorizer.logger")
  private var superUsers = Set.empty[KafkaPrincipal]
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkUtils: ZkUtils = null
  private var aclChangeListener: ZkNodeChangeNotificationListener = null

  private val aclCache = new scala.collection.mutable.HashMap[Resource, VersionedAcls]
  private val lock = new ReentrantReadWriteLock()

  // The maximum number of times we should try to update the resource acls in zookeeper before failing;
  // This should never occur, but is a safeguard just in case.
  protected[auth] var maxUpdateRetries = 10

  private val retryBackoffMs = 100
  private val retryBackoffJitterMs = 50

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(javaConfigs: util.Map[String, _]) {
    val configs = javaConfigs.asScala
    val props = new java.util.Properties()
    configs.foreach { case (key, value) => props.put(key, value.toString) }

    superUsers = configs.get(SimpleAclAuthorizer.SuperUsersProp).collect {
      case str: String if str.nonEmpty => str.split(";").map(s => KafkaPrincipal.fromString(s.trim)).toSet
    }.getOrElse(Set.empty[KafkaPrincipal])

    shouldAllowEveryoneIfNoAclIsFound = configs.get(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).exists(_.toString.toBoolean)

    // Use `KafkaConfig` in order to get the default ZK config values if not present in `javaConfigs`. Note that this
    // means that `KafkaConfig.zkConnect` must always be set by the user (even if `SimpleAclAuthorizer.ZkUrlProp` is also
    // set).
    val kafkaConfig = KafkaConfig.fromProps(props, doLog = false)
    val zkUrl = configs.get(SimpleAclAuthorizer.ZkUrlProp).map(_.toString).getOrElse(kafkaConfig.zkConnect)
    val zkConnectionTimeoutMs = configs.get(SimpleAclAuthorizer.ZkConnectionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkConnectionTimeoutMs)
    val zkSessionTimeOutMs = configs.get(SimpleAclAuthorizer.ZkSessionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkSessionTimeoutMs)

    zkUtils = ZkUtils(zkUrl,
                      sessionTimeout = zkSessionTimeOutMs,
                      connectionTimeout = zkConnectionTimeoutMs,
                      kafkaConfig.zkEnableSecureAcls)
    zkUtils.makeSurePersistentPathExists(SimpleAclAuthorizer.AclZkPath)

    loadCache()

    zkUtils.makeSurePersistentPathExists(SimpleAclAuthorizer.AclChangedZkPath)
    aclChangeListener = new ZkNodeChangeNotificationListener(zkUtils, SimpleAclAuthorizer.AclChangedZkPath, SimpleAclAuthorizer.AclChangedPrefix, AclChangedNotificationHandler)
    aclChangeListener.init()
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    val principal = session.principal
    val host = session.clientAddress.getHostAddress
    val acls = getAcls(resource) ++ getAcls(new Resource(resource.resourceType, Resource.WildCardResource))

    // Check if there is any Deny acl match that would disallow this operation.
    val denyMatch = aclMatch(operation, resource, principal, host, Deny, acls)

    // Check if there are any Allow ACLs which would allow this operation.
    // Allowing read, write, delete, or alter implies allowing describe.
    // See #{org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
    val allowOps = operation match {
      case Describe => Set[Operation](Describe, Read, Write, Delete, Alter)
      case DescribeConfigs => Set[Operation](DescribeConfigs, AlterConfigs)
      case _ => Set[Operation](operation)
    }
    val allowMatch = allowOps.exists(operation => aclMatch(operation, resource, principal, host, Allow, acls))

    //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
    //when no acls are found or if no deny acls are found and at least one allow acls matches.
    val authorized = isSuperUser(operation, resource, principal, host) ||
      isEmptyAclAndAuthorized(operation, resource, principal, host, acls) ||
      (!denyMatch && allowMatch)

    logAuditMessage(principal, authorized, operation, resource, host)
    authorized
  }

  def isEmptyAclAndAuthorized(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String, acls: Set[Acl]): Boolean = {
    if (acls.isEmpty) {
      authorizerLogger.debug(s"No acl found for resource $resource, authorized = $shouldAllowEveryoneIfNoAclIsFound")
      shouldAllowEveryoneIfNoAclIsFound
    } else false
  }

  def isSuperUser(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String): Boolean = {
    if (superUsers.contains(principal)) {
      authorizerLogger.debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      true
    } else false
  }

  private def aclMatch(operations: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    acls.find { acl =>
      acl.permissionType == permissionType &&
        (acl.principal == principal || acl.principal == Acl.WildCardPrincipal) &&
        (operations == acl.operation || acl.operation == All) &&
        (acl.host == host || acl.host == Acl.WildCardHost)
    }.exists { acl =>
      authorizerLogger.debug(s"operation = $operations on resource = $resource from host = $host is $permissionType based on acl = $acl")
      true
    }
  }

  override def addAcls(acls: Set[Acl], resource: Resource) {
    if (acls != null && acls.nonEmpty) {
      inWriteLock(lock) {
        updateResourceAcls(resource) { currentAcls =>
          currentAcls ++ acls
        }
      }
    }
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    inWriteLock(lock) {
      updateResourceAcls(resource) { currentAcls =>
        currentAcls -- aclsTobeRemoved
      }
    }
  }

  override def removeAcls(resource: Resource): Boolean = {
    inWriteLock(lock) {
      val result = zkUtils.deletePath(toResourcePath(resource))
      updateCache(resource, VersionedAcls(Set(), 0))
      updateAclChangedFlag(resource)
      result
    }
  }

  override def getAcls(resource: Resource): Set[Acl] = {
    inReadLock(lock) {
      aclCache.get(resource).map(_.acls).getOrElse(Set.empty[Acl])
    }
  }

  override def getAcls(principal: KafkaPrincipal): Map[Resource, Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues { versionedAcls =>
        versionedAcls.acls.filter(_.principal == principal)
      }.filter { case (_, acls) =>
        acls.nonEmpty
      }.toMap
    }
  }

  override def getAcls(): Map[Resource, Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues(_.acls).toMap
    }
  }

  def close() {
    if (aclChangeListener != null) aclChangeListener.close()
    if (zkUtils != null) zkUtils.close()
  }

  private def loadCache()  {
    inWriteLock(lock) {
      val resourceTypes = zkUtils.getChildren(SimpleAclAuthorizer.AclZkPath)
      for (rType <- resourceTypes) {
        val resourceType = ResourceType.fromString(rType)
        val resourceTypePath = SimpleAclAuthorizer.AclZkPath + "/" + resourceType.name
        val resourceNames = zkUtils.getChildren(resourceTypePath)
        for (resourceName <- resourceNames) {
          val versionedAcls = getAclsFromZk(Resource(resourceType, resourceName.toString))
          updateCache(new Resource(resourceType, resourceName), versionedAcls)
        }
      }
    }
  }

  def toResourcePath(resource: Resource): String = {
    SimpleAclAuthorizer.AclZkPath + "/" + resource.resourceType + "/" + resource.name
  }

  private def logAuditMessage(principal: KafkaPrincipal, authorized: Boolean, operation: Operation, resource: Resource, host: String) {
    def logMessage: String = {
      val authResult = if (authorized) "Allowed" else "Denied"
      s"Principal = $principal is $authResult Operation = $operation from host = $host on resource = $resource"
    }

    if (authorized) authorizerLogger.debug(logMessage)
    else authorizerLogger.info(logMessage)
  }

  /**
    * Safely updates the resources ACLs by ensuring reads and writes respect the expected zookeeper version.
    * Continues to retry until it successfully updates zookeeper.
    *
    * Returns a boolean indicating if the content of the ACLs was actually changed.
    *
    * @param resource the resource to change ACLs for
    * @param getNewAcls function to transform existing acls to new ACLs
    * @return boolean indicating if a change was made
    */
  private def updateResourceAcls(resource: Resource)(getNewAcls: Set[Acl] => Set[Acl]): Boolean = {
    val path = toResourcePath(resource)

    var currentVersionedAcls =
      if (aclCache.contains(resource))
        getAclsFromCache(resource)
      else
        getAclsFromZk(resource)
    var newVersionedAcls: VersionedAcls = null
    var writeComplete = false
    var retries = 0
    while (!writeComplete && retries <= maxUpdateRetries) {
      val newAcls = getNewAcls(currentVersionedAcls.acls)
      val data = Json.encode(Acl.toJsonCompatibleMap(newAcls))
      val (updateSucceeded, updateVersion) =
        if (newAcls.nonEmpty) {
         updatePath(path, data, currentVersionedAcls.zkVersion)
        } else {
          trace(s"Deleting path for $resource because it had no ACLs remaining")
          (zkUtils.conditionalDeletePath(path, currentVersionedAcls.zkVersion), 0)
        }

      if (!updateSucceeded) {
        trace(s"Failed to update ACLs for $resource. Used version ${currentVersionedAcls.zkVersion}. Reading data and retrying update.")
        Thread.sleep(backoffTime)
        currentVersionedAcls = getAclsFromZk(resource)
        retries += 1
      } else {
        newVersionedAcls = VersionedAcls(newAcls, updateVersion)
        writeComplete = updateSucceeded
      }
    }

    if(!writeComplete)
      throw new IllegalStateException(s"Failed to update ACLs for $resource after trying a maximum of $maxUpdateRetries times")

    if (newVersionedAcls.acls != currentVersionedAcls.acls) {
      debug(s"Updated ACLs for $resource to ${newVersionedAcls.acls} with version ${newVersionedAcls.zkVersion}")
      updateCache(resource, newVersionedAcls)
      updateAclChangedFlag(resource)
      true
    } else {
      debug(s"Updated ACLs for $resource, no change was made")
      updateCache(resource, newVersionedAcls) // Even if no change, update the version
      false
    }
  }

  /**
    * Updates a zookeeper path with an expected version. If the topic does not exist, it will create it.
    * Returns if the update was successful and the new version.
    */
  private def updatePath(path: String, data: String, expectedVersion: Int): (Boolean, Int) = {
    try {
      zkUtils.conditionalUpdatePersistentPathIfExists(path, data, expectedVersion)
    } catch {
      case _: ZkNoNodeException =>
        try {
          debug(s"Node $path does not exist, attempting to create it.")
          zkUtils.createPersistentPath(path, data)
          (true, 0)
        } catch {
          case _: ZkNodeExistsException =>
            debug(s"Failed to create node for $path because it already exists.")
            (false, 0)
        }
    }
  }

  private def getAclsFromCache(resource: Resource): VersionedAcls = {
    aclCache.getOrElse(resource, throw new IllegalArgumentException(s"ACLs do not exist in the cache for resource $resource"))
  }

  private def getAclsFromZk(resource: Resource): VersionedAcls = {
    val (aclJson, stat) = zkUtils.readDataMaybeNull(toResourcePath(resource))
    VersionedAcls(aclJson.map(Acl.fromJson).getOrElse(Set()), stat.getVersion)
  }

  private def updateCache(resource: Resource, versionedAcls: VersionedAcls) {
    if (versionedAcls.acls.nonEmpty) {
      aclCache.put(resource, versionedAcls)
    } else {
      aclCache.remove(resource)
    }
  }

  private def updateAclChangedFlag(resource: Resource) {
    zkUtils.createSequentialPersistentPath(SimpleAclAuthorizer.AclChangedZkPath + "/" + SimpleAclAuthorizer.AclChangedPrefix, resource.toString)
  }

  private def backoffTime = {
    retryBackoffMs + Random.nextInt(retryBackoffJitterMs)
  }

  object AclChangedNotificationHandler extends NotificationHandler {
    override def processNotification(notificationMessage: String) {
      val resource: Resource = Resource.fromString(notificationMessage)
      inWriteLock(lock) {
        val versionedAcls = getAclsFromZk(resource)
        updateCache(resource, versionedAcls)
      }
    }
  }
}
