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
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.KafkaPrincipal
import scala.collection.JavaConverters._
import org.apache.log4j.Logger

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
   * The root acl storage node. Under this node there will be one child node per resource type (Topic, Cluster, ConsumerGroup).
   * under each resourceType there will be a unique child for each resource instance and the data for that child will contain
   * list of its acls as a json object. Following gives an example:
   *
   * <pre>
   * /kafka-acl/Topic/topic-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * /kafka-acl/Cluster/kafka-cluster => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * /kafka-acl/ConsumerGroup/group-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
   * </pre>
   */
  val AclZkPath = "/kafka-acl"

  //notification node which gets updated with the resource name when acl on a resource is changed.
  val AclChangedZkPath = "/kafka-acl-changes"

  //prefix of all the change notification sequence node.
  val AclChangedPrefix = "acl_changes_"
}

class SimpleAclAuthorizer extends Authorizer with Logging {
  private val authorizerLogger = Logger.getLogger("kafka.authorizer.logger")
  private var superUsers = Set.empty[KafkaPrincipal]
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkUtils: ZkUtils = null
  private var aclChangeListener: ZkNodeChangeNotificationListener = null

  private case class VersionedAcls(acls: Set[Acl], version: Int)
  private val aclCache = new scala.collection.mutable.HashMap[Resource, VersionedAcls]
  private val lock = new ReentrantReadWriteLock()

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
                      zkConnectionTimeoutMs,
                      zkSessionTimeOutMs,
                      JaasUtils.isZkSecurityEnabled())
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

    //check if there is any Deny acl match that would disallow this operation.
    val denyMatch = aclMatch(session, operation, resource, principal, host, Deny, acls)

    //if principal is allowed to read or write we allow describe by default, the reverse does not apply to Deny.
    val ops = if (Describe == operation)
      Set[Operation](operation, Read, Write)
    else
      Set[Operation](operation)

    //now check if there is any allow acl that will allow this operation.
    val allowMatch = ops.exists(operation => aclMatch(session, operation, resource, principal, host, Allow, acls))

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
    if (superUsers.exists( _ == principal)) {
      authorizerLogger.debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      true
    } else false
  }

  private def aclMatch(session: Session, operations: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    acls.find ( acl =>
      acl.permissionType == permissionType
        && (acl.principal == principal || acl.principal == Acl.WildCardPrincipal)
        && (operations == acl.operation || acl.operation == All)
        && (acl.host == host || acl.host == Acl.WildCardHost)
    ).map { acl: Acl =>
      authorizerLogger.debug(s"operation = $operations on resource = $resource from host = $host is $permissionType based on acl = $acl")
      true
    }.getOrElse(false)
  }

  override def addAcls(acls: Set[Acl], resource: Resource) {
    inWriteLock(lock) {
      if (acls != null && acls.nonEmpty) {
        updateResourceAcls(resource) { currentAcls =>
          currentAcls ++ acls
        }
      }
    }
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    inWriteLock(lock) {
      updateResourceAcls(resource) { currentAcls =>
        currentAcls.diff(aclsTobeRemoved)
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
    val permissionType = if (authorized) "Allowed" else "Denied"
    authorizerLogger.debug(s"Principal = $principal is $permissionType Operation = $operation from host = $host on resource = $resource")
  }

  /**
    * Safely updates the resources acls by ensureing reads and writes respect the expected zookeeper version.
    * Continues to retry until it succesfully updates.
    *
    * @param resource the resource to change acls for
    * @param getNewAcls function to transform existing acls to new acls
    * @return boolean indicating if a change was made
    */
  private def updateResourceAcls(resource: Resource)(getNewAcls: Set[Acl] => Set[Acl]): Boolean = {
    val path = toResourcePath(resource)

    var currentVersionedAcls =
      if(aclCache.contains(resource))
        getAclsFromCache(resource)
      else
      getAclsFromZk(resource)
    var newVersionedAcls = currentVersionedAcls
    var writeComplete = false
    while (!writeComplete) {
      val newAcls = getNewAcls(currentVersionedAcls.acls)
      val data = Json.encode(Acl.toJsonCompatibleMap(newAcls))
      try {
        val (updateSucceeded, updateVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(path, data, currentVersionedAcls.version)
        if(!updateSucceeded) {
          trace(s"Failed to update acls for $resource. Used version ${currentVersionedAcls.version}. Reading data and retrying update.")
          currentVersionedAcls = getAclsFromZk(resource);
        } else {
          if(newAcls.isEmpty) {
            trace(s"Deleting path for $resource because it had no acls remaining")
            zkUtils.deletePath(path) // It would be safest to use the versioned request, but this zkClient doesn't support it
          }
          newVersionedAcls = VersionedAcls(newAcls, updateVersion)
        }
        writeComplete = updateSucceeded
      } catch {
        case e: ZkNoNodeException =>
          try {
            debug(s"Node for $resource does not exist, attempting to create it.")
            zkUtils.createPersistentPath(path, data)
            newVersionedAcls = VersionedAcls(newAcls, 0)
            writeComplete = true
          } catch {
            case e: ZkNodeExistsException =>
              debug(s"Failed to create node for $resource because it already exists. Reading data and retrying update.")
              currentVersionedAcls = getAclsFromZk(resource);
          }
      }
    }

    if(newVersionedAcls.acls != currentVersionedAcls.acls) {
      debug(s"Updated acls for $resource to ${newVersionedAcls.acls} with version ${newVersionedAcls.version}")
      updateCache(resource, newVersionedAcls)
      updateAclChangedFlag(resource)
      true
    } else {
      debug(s"Updated acls for $resource, no change was made")
      updateCache(resource, newVersionedAcls) // Even if no change, update the version
      false
    }
  }

  private def getAclsFromCache(resource: Resource): VersionedAcls = {
    aclCache.get(resource).getOrElse(throw new IllegalArgumentException(s"Acls do not exist in the cache for resource $resource"))
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
