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
import org.apache.zookeeper.Watcher.Event.KeeperState


import kafka.network.RequestChannel.Session
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.IZkStateListener
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

  private val aclCache = new scala.collection.mutable.HashMap[Resource, Set[Acl]]
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

    zkUtils.zkClient.subscribeStateChanges(ZkStateChangeListener)
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
    if (acls != null && acls.nonEmpty) {
      val updatedAcls = getAcls(resource) ++ acls
      val path = toResourcePath(resource)

      if (zkUtils.pathExists(path))
        zkUtils.updatePersistentPath(path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))
      else
        zkUtils.createPersistentPath(path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))

      updateAclChangedFlag(resource)
    }
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    if (zkUtils.pathExists(toResourcePath(resource))) {
      val existingAcls = getAcls(resource)
      val filteredAcls = existingAcls.filter((acl: Acl) => !aclsTobeRemoved.contains(acl))

      val aclNeedsRemoval = (existingAcls != filteredAcls)
      if (aclNeedsRemoval) {
        val path: String = toResourcePath(resource)
        if (filteredAcls.nonEmpty)
          zkUtils.updatePersistentPath(path, Json.encode(Acl.toJsonCompatibleMap(filteredAcls)))
        else
          zkUtils.deletePath(toResourcePath(resource))

        updateAclChangedFlag(resource)
      }

      aclNeedsRemoval
    } else false
  }

  override def removeAcls(resource: Resource): Boolean = {
    if (zkUtils.pathExists(toResourcePath(resource))) {
      zkUtils.deletePath(toResourcePath(resource))
      updateAclChangedFlag(resource)
      true
    } else false
  }

  override def getAcls(resource: Resource): Set[Acl] = {
    inReadLock(lock) {
      aclCache.get(resource).getOrElse(Set.empty[Acl])
    }
  }

  private def getAclsFromZk(resource: Resource): Set[Acl] = {
    val aclJson = zkUtils.readDataMaybeNull(toResourcePath(resource))._1
    aclJson.map(Acl.fromJson).getOrElse(Set.empty)
  }

  override def getAcls(principal: KafkaPrincipal): Map[Resource, Set[Acl]] = {
    aclCache.mapValues { acls =>
      acls.filter(_.principal == principal)
    }.filter { case (_, acls) =>
      acls.nonEmpty
    }.toMap
  }

  override def getAcls(): Map[Resource, Set[Acl]] = {
    aclCache.toMap
  }

  def close() {
    if (aclChangeListener != null) aclChangeListener.close()
    if (zkUtils != null) zkUtils.close()
  }

  private def loadCache()  {
    var acls = Set.empty[Acl]
    val resourceTypes = zkUtils.getChildren(SimpleAclAuthorizer.AclZkPath)
    for (rType <- resourceTypes) {
      val resourceType = ResourceType.fromString(rType)
      val resourceTypePath = SimpleAclAuthorizer.AclZkPath + "/" + resourceType.name
      val resourceNames = zkUtils.getChildren(resourceTypePath)
      for (resourceName <- resourceNames) {
        acls = getAclsFromZk(Resource(resourceType, resourceName.toString))
        updateCache(new Resource(resourceType, resourceName), acls)
      }
    }
  }

  private def updateCache(resource: Resource, acls: Set[Acl]) {
    inWriteLock(lock) {
      if (acls.nonEmpty)
        aclCache.put(resource, acls)
      else
        aclCache.remove(resource)
    }
  }

  def toResourcePath(resource: Resource): String = {
    SimpleAclAuthorizer.AclZkPath + "/" + resource.resourceType + "/" + resource.name
  }

  private def logAuditMessage(principal: KafkaPrincipal, authorized: Boolean, operation: Operation, resource: Resource, host: String) {
    val permissionType = if (authorized) "Allowed" else "Denied"
    authorizerLogger.debug(s"Principal = $principal is $permissionType Operation = $operation from host = $host on resource = $resource")
  }

  private def updateAclChangedFlag(resource: Resource) {
    zkUtils.createSequentialPersistentPath(SimpleAclAuthorizer.AclChangedZkPath + "/" + SimpleAclAuthorizer.AclChangedPrefix, resource.toString)
  }

  object AclChangedNotificationHandler extends NotificationHandler {

    override def processNotification(notificationMessage: String) {
      val resource: Resource = Resource.fromString(notificationMessage)
      val acls = getAclsFromZk(resource)
      updateCache(resource, acls)
    }
  }

  object ZkStateChangeListener extends IZkStateListener {

    override def handleNewSession() {
      aclChangeListener.processAllNotifications
    }

    override def handleSessionEstablishmentError(error: Throwable) {
      fatal("Could not establish session with zookeeper", error)
    }

    override def handleStateChanged(state: KeeperState) {
      //no op
    }
  }

}
