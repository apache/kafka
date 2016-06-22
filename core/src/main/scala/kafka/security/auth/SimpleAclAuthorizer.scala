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
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth._

import scala.collection.JavaConverters._
import org.apache.log4j.Logger

import scala.collection.mutable
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

  private case class VersionedAcls(acls: mutable.Set[Acl], zkVersion: Int)
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
    val acls = this.acls(resource)
    acls.addAll(this.acls(new Resource(resource.getResourceType, Resource.WILDCARD_RESOURCE)))

    //check if there is any Deny acl match that would disallow this operation.
    val denyMatch = aclMatch(session, operation, resource, principal, host, PermissionType.DENY, acls)

    //if principal is allowed to read or write we allow describe by default, the reverse does not apply to Deny.
    val ops = if (Operation.DESCRIBE == operation)
      Set[Operation](operation, Operation.READ, Operation.WRITE)
    else
      Set[Operation](operation)

    //now check if there is any allow acl that will allow this operation.
    val allowMatch = ops.exists(operation => aclMatch(session, operation, resource, principal, host, PermissionType.ALLOW, acls))

    //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
    //when no acls are found or if no deny acls are found and at least one allow acls matches.
    val authorized = isSuperUser(operation, resource, principal, host) ||
      isEmptyAclAndAuthorized(operation, resource, principal, host, acls) ||
      (!denyMatch && allowMatch)

    logAuditMessage(principal, authorized, operation, resource, host)
    authorized
  }

  override def description(): String = {
    "\n*********************************\n" +
      "     SIMPLE ACL AUTHORIZER\n" +
      "*********************************\n"
  }

  def isEmptyAclAndAuthorized(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String, acls: java.util.Set[Acl]): Boolean = {
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

  private def aclMatch(session: Session, operations: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: java.util.Set[Acl]): Boolean = {
    acls.asScala.find ( acl =>
      acl.permissionType == permissionType
        && (acl.principal == principal || acl.principal == Acl.WILDCARD_PRINCIPAL)
        && (operations == acl.operation || acl.operation == Operation.ALL)
        && (acl.host == host || acl.host == Acl.WILDCARD_HOST)
    ).map { acl: Acl =>
      authorizerLogger.debug(s"operation = $operations on resource = $resource from host = $host is $permissionType based on acl = $acl")
      true
    }.getOrElse(false)
  }

  override def addAcls(acls: java.util.Set[Acl], resource: Resource) {
    if (acls != null && !acls.isEmpty) {
      inWriteLock(lock) {
        updateResourceAcls(resource) { currentAcls =>
          currentAcls ++ scala.collection.JavaConversions.asScalaSet(acls)
        }
      }
    }
  }

  override def removeAcls(aclsTobeRemoved: java.util.Set[Acl], resource: Resource): Boolean = {
    inWriteLock(lock) {
      updateResourceAcls(resource) { currentAcls =>
        currentAcls -- scala.collection.JavaConversions.asScalaSet(aclsTobeRemoved)
      }
    }
  }

  override def removeAcls(resource: Resource): Boolean = {
    inWriteLock(lock) {
      val result = zkUtils.deletePath(toResourcePath(resource))
      updateCache(resource, VersionedAcls(mutable.Set(), 0))
      updateAclChangedFlag(resource)
      result
    }
  }

  override def acls(resource: Resource): java.util.Set[Acl] = {
    inReadLock(lock) {
      new util.HashSet[Acl](aclCache.get(resource).map(_.acls).getOrElse(mutable.Set.empty[Acl]).asJava)
    }
  }

  override def acls(principal: KafkaPrincipal): java.util.Map[Resource, java.util.Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues { versionedAcls =>
        versionedAcls.acls.filter(_.principal == principal).asJava
      }.filter { case (_, acls) =>
        !acls.isEmpty
      }.toMap.asJava
    }
  }

  override def acls(): java.util.Map[Resource, java.util.Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues(_.acls.asJava).toMap.asJava
    }
  }

  override def close() {
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
          val versionedAcls = getAclsFromZk(new Resource(resourceType, resourceName.toString))
          updateCache(new Resource(resourceType, resourceName), versionedAcls)
        }
      }
    }
  }

  def toResourcePath(resource: Resource): String = {
    SimpleAclAuthorizer.AclZkPath + "/" + resource.getResourceType.name + "/" + resource.name
  }

  private def logAuditMessage(principal: KafkaPrincipal, authorized: Boolean, operation: Operation, resource: Resource, host: String) {
    val permissionType = if (authorized) "Allowed" else "Denied"
    authorizerLogger.debug(s"Principal = $principal is $permissionType Operation = $operation from host = $host on resource = $resource")
  }

  /**
    * Safely updates the resources ACLs by ensuring reads and writes respect the expected zookeeper version.
    * Continues to retry until it succesfully updates zookeeper.
    *
    * Returns a boolean indicating if the content of the ACLs was actually changed.
    *
    * @param resource the resource to change ACLs for
    * @param getNewAcls function to transform existing acls to new ACLs
    * @return boolean indicating if a change was made
    */
  private def updateResourceAcls(resource: Resource)(getNewAcls: mutable.Set[Acl] => mutable.Set[Acl]): Boolean = {
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
      val data = Json.encode(aclToJsonCompatibleMap(newAcls))
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
      case e: ZkNoNodeException =>
        try {
          debug(s"Node $path does not exist, attempting to create it.")
          zkUtils.createPersistentPath(path, data)
          (true, 0)
        } catch {
          case e: ZkNodeExistsException =>
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
    VersionedAcls(aclJson.map(aclFromJson).getOrElse(mutable.Set()), stat.getVersion)
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

  private val AllowAllAcl = new Acl(Acl.WILDCARD_PRINCIPAL, PermissionType.ALLOW, Acl.WILDCARD_HOST, Operation.ALL)
  private val PrincipalKey = "principal"
  private val PermissionTypeKey = "permissionType"
  private val OperationKey = "operation"
  private val HostsKey = "host"
  private val VersionKey = "version"
  private val CurrentVersion = 1
  private val AclsKey = "acls"

  private[auth] def aclToJsonCompatibleMap(acls: mutable.Set[Acl]): Map[String, Any] = {
    Map(VersionKey -> CurrentVersion, AclsKey -> acls.map(acl => aclToMap(acl)).toList)
  }

  private[auth] def aclToMap(acl: Acl): Map[String, Any] = {
    Map(PrincipalKey -> acl.principal.toString,
      PermissionTypeKey -> acl.permissionType.name,
      OperationKey -> acl.operation.name,
      HostsKey -> acl.host)
  }

  /**
    *
    * @param aclJson
    *
    * <p>
      *{
        *"version": 1,
        *"acls": [
          *{
            *"host":"host1",
            *"permissionType": "Deny",
            *"operation": "Read",
            *"principal": "User:alice"
          *}
        *]
      *}
    * </p>
    * @return
    */
  def aclFromJson(aclJson: String): mutable.Set[Acl] = {
    if (aclJson == null || aclJson.isEmpty)
      return mutable.Set.empty[Acl]

    var acls: mutable.HashSet[Acl] = new mutable.HashSet[Acl]()
    Json.parseFull(aclJson) match {
      case Some(m) =>
        val aclMap = m.asInstanceOf[Map[String, Any]]
        //the acl json version.
        require(aclMap(VersionKey) == CurrentVersion)
        val aclSet: List[Map[String, Any]] = aclMap(AclsKey).asInstanceOf[List[Map[String, Any]]]
        aclSet.foreach(item => {
          val principal: KafkaPrincipal = KafkaPrincipal.fromString(item(PrincipalKey).asInstanceOf[String])
          val permissionType: PermissionType = PermissionType.fromString(item(PermissionTypeKey).asInstanceOf[String])
          val operation: Operation = Operation.fromString(item(OperationKey).asInstanceOf[String])
          val host: String = item(HostsKey).asInstanceOf[String]
          acls += new Acl(principal, permissionType, host, operation)
        })
      case None =>
    }
    acls
  }
}
