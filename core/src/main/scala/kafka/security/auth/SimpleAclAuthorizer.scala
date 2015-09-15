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
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
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

  //prefix of all the change notificiation sequence node.
  val AclChangedPrefix = "acl_changes_"
}

class SimpleAclAuthorizer extends Authorizer with Logging {
  private val authorizerLogger = Logger.getLogger("kafka.authorizer.logger")
  private var superUsers = Set.empty[KafkaPrincipal]
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkClient: ZkClient = null
  private var aclChangeListener: ZkNodeChangeNotificationListener = null

  private val aclCache: scala.collection.mutable.HashMap[Resource, Set[Acl]] = new scala.collection.mutable.HashMap[Resource, Set[Acl]]
  private val lock = new ReentrantReadWriteLock()

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    val configs = javaConfigs.asScala
    val props = new java.util.Properties()
    configs foreach { case (key, value) => props.put(key, value.toString) }
    val kafkaConfig = KafkaConfig.fromProps(props)

    superUsers = javaConfigs.get(SimpleAclAuthorizer.SuperUsersProp) match {
      case null => Set.empty[KafkaPrincipal]
      case (str: String) => if (!str.isEmpty) str.split(",").map(s => KafkaPrincipal.fromString(s.trim)).toSet else Set.empty[KafkaPrincipal]
      case _ => Set.empty
    }

    shouldAllowEveryoneIfNoAclIsFound = if (configs.contains(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp))
      javaConfigs.get(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).toString.toBoolean
    else false

    val zkUrl = configs.getOrElse(SimpleAclAuthorizer.ZkUrlProp, kafkaConfig.zkConnect).toString
    val zkConnectionTimeoutMs = configs.getOrElse(SimpleAclAuthorizer.ZkConnectionTimeOutProp, kafkaConfig.zkConnectionTimeoutMs).toString.toInt
    val zkSessionTimeOutMs = configs.getOrElse(SimpleAclAuthorizer.ZkSessionTimeOutProp, kafkaConfig.zkSessionTimeoutMs).toString.toInt

    zkClient = ZkUtils.createZkClient(zkUrl, zkConnectionTimeoutMs, zkSessionTimeOutMs)
    ZkUtils.makeSurePersistentPathExists(zkClient, SimpleAclAuthorizer.AclZkPath)

    aclChangeListener = new ZkNodeChangeNotificationListener(zkClient, SimpleAclAuthorizer.AclChangedZkPath, SimpleAclAuthorizer.AclChangedPrefix, AclChangedNotificaitonHandler)
    aclChangeListener.startup()

    zkClient.subscribeStateChanges(ZkStateChangeListener)

    loadCache
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    val principal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, session.principal.getName)
    val host = session.host

    authorizerLogger.trace(s"Authorizer invoked to authorize principal = $principal from host = $host on resource = $resource for operation = $resource")

    if (superUsers.contains(principal)) {
      authorizerLogger.debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      return true
    }

    val acls: Set[Acl] = getAcls(resource)
    if (acls.isEmpty) {
      authorizerLogger.debug(s"No acl found for resource $resource , authorized = $shouldAllowEveryoneIfNoAclIsFound")
      return shouldAllowEveryoneIfNoAclIsFound
    }

    //first check if there is any Deny acl match that would disallow this operation.
    if (aclMatch(session, operation, resource, principal, host, Deny, acls))
      return false

    /**
     * if principal is allowed to read or write we allow describe by default, the reverse does not apply to Deny.
     */
    val ops: Set[Operation] = if (Describe.equals(operation))
      Set[Operation](operation, Read, Write)
    else
      Set[Operation](operation)

    //now check if there is any allow acl that will allow this operation.
    if (ops.exists(operation => aclMatch(session, operation, resource, principal, host, Allow, acls)))
      return true

    //We have some acls defined and they do not specify any allow ACL for the current session, reject request.
    authorizerLogger.debug(s"principal = $principal from host = $host is not allowed to perform operation = $operation  " +
      s"on resource = $resource as no explicit acls were defined to allow the operation")
    false
  }

  private def aclMatch(session: Session, operations: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    for (acl <- acls) {
      if (acl.permissionType.equals(permissionType)
        && (acl.principal == principal || acl.principal == Acl.WildCardPrincipal)
        && (operations == acl.operation || acl.operation == All)
        && (acl.host == host || acl.host == Acl.WildCardHost)) {
        authorizerLogger.debug(s"operation = $operations on resource = $resource from host = $host is $permissionType.name based on acl = $acl")
        return true
      }
    }
    false
  }

  override def addAcls(acls: Set[Acl], resource: Resource): Unit = {
    if (acls == null || acls.isEmpty)
      return

    val updatedAcls: Set[Acl] = getAcls(resource) ++ acls
    val path: String = toResourcePath(resource)

    if (ZkUtils.pathExists(zkClient, path))
      ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))
    else
      ZkUtils.createPersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(updatedAcls)))

    inWriteLock(lock) {
      aclCache.put(resource, updatedAcls)
    }

    updateAclChangedFlag(resource)
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    if (!ZkUtils.pathExists(zkClient, toResourcePath(resource)))
      return false

    val existingAcls: Set[Acl] = getAcls(resource)
    val filteredAcls: Set[Acl] = existingAcls.filter((acl: Acl) => !aclsTobeRemoved.contains(acl))
    if (existingAcls.equals(filteredAcls))
      return false

    val path: String = toResourcePath(resource)
    ZkUtils.updatePersistentPath(zkClient, path, Json.encode(Acl.toJsonCompatibleMap(filteredAcls)))

    inWriteLock(lock) {
      aclCache.put(resource, filteredAcls)
    }

    updateAclChangedFlag(resource)

    true
  }

  override def removeAcls(resource: Resource): Boolean = {
    if (ZkUtils.pathExists(zkClient, toResourcePath(resource))) {
      ZkUtils.deletePath(zkClient, toResourcePath(resource))
      inWriteLock(lock) {
        aclCache.remove(resource)
      }
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
    val aclJson = ZkUtils.readDataMaybeNull(zkClient, toResourcePath(resource))._1
    aclJson.map(Acl.fromJson).getOrElse(Set.empty)
  }

  override def getAcls(principal: KafkaPrincipal): Set[Acl] = {
    val aclSets = aclCache.values
    var acls = Set.empty[Acl]
    for(aclSet  <- aclSets) {
      acls ++= aclSet.filter(acl => acl.principal.equals(principal))
    }
    acls.toSet[Acl]
  }

  private def loadCache: Set[Acl] = {
    var acls = Set.empty[Acl]
    val resourceTypes = ZkUtils.getChildren(zkClient, SimpleAclAuthorizer.AclZkPath)
    for (rType <- resourceTypes) {
      val resourceType = ResourceType.fromString(rType)
      for (resources <- ZkUtils.getChildren(zkClient, resourceType.name)) {
        for (resourceName <- resources)
          acls ++= getAcls(Resource(resourceType, resourceName.toString))
      }
    }
    acls
  }


  private def toResourcePath(resource: Resource): String = {
    SimpleAclAuthorizer.AclZkPath + "/" + resource.resourceType + "/" + resource.name
  }

  private def updateAclChangedFlag(resource: Resource): Unit = {
    ZkUtils.createSequentialPersistentPath(zkClient, SimpleAclAuthorizer.AclChangedZkPath + "/" + SimpleAclAuthorizer.AclChangedPrefix, resource.toString)
  }

  object AclChangedNotificaitonHandler extends NotificationHandler {

    override def processNotification(notificationMessage: Option[String]): Unit = {
      val resource: Resource = Resource.fromString(notificationMessage.get)
      val acls = getAclsFromZk(resource)

      inWriteLock(lock) {
        aclCache.put(resource, acls)
      }
    }
  }

  object ZkStateChangeListener extends IZkStateListener {

    var reconnection = false;
    override def handleNewSession(): Unit = {
      aclChangeListener.processAllNotifications
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      fatal("Could not establish session with zookeeper", error)
    }

    override def handleStateChanged(state: KeeperState): Unit = {
      //no op
    }
  }

}