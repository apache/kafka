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
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.network.RequestChannel.Session
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import scala.collection.JavaConverters._

object SimpleAclAuthorizer {
  //the zookeeper cluster where acls will be stored, defaults to value specified for "zkConnect"
  val ZkUrlProp = "zookeeper.url"
  //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
  val SuperUsersProp = "super.users"
  //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
  val AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found"

  val AclZkPath = "/kafka-acl"
  val AclChangedZkPath = "/kafka-acl-changed"
}

class SimpleAclAuthorizer extends Authorizer with Logging {
  private var superUsers: Set[KafkaPrincipal] = Set.empty
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkClient: ZkClient = null
  private val scheduler: KafkaScheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "authorizer")

  private val aclCache: scala.collection.mutable.HashMap[Resource, Set[Acl]] = new scala.collection.mutable.HashMap[Resource, Set[Acl]]
  private val lock = new ReentrantReadWriteLock()

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    val config = configs.asScala
    val props = new java.util.Properties()
    config foreach { case (key, value) => props.put(key, value.toString) }
    val kafkaConfig = KafkaConfig.fromProps(props)

    superUsers = configs.get(SimpleAclAuthorizer.SuperUsersProp) match {
      case null => Set.empty[KafkaPrincipal]
      case (str: String) => if (!str.isEmpty) str.split(",").map(s => KafkaPrincipal.fromString(s.trim)).toSet else Set.empty
    }

    shouldAllowEveryoneIfNoAclIsFound = if (config.contains(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp))
      configs.get(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).toString.toBoolean
    else false

    val zkUrl = config.getOrElse(SimpleAclAuthorizer.ZkUrlProp, kafkaConfig.zkConnect).toString

    zkClient = ZkUtils.createZkClient(zkUrl, Integer2int(kafkaConfig.zkConnectionTimeoutMs), Integer2int(kafkaConfig.zkConnectionTimeoutMs))
    ZkUtils.makeSurePersistentPathExists(zkClient, SimpleAclAuthorizer.AclZkPath)
    ZkUtils.makeSurePersistentPathExists(zkClient, SimpleAclAuthorizer.AclChangedZkPath)
    zkClient.subscribeDataChanges(SimpleAclAuthorizer.AclChangedZkPath, AclChangeListener)

    //we still invalidate the cache every hour in case we missed any watch notifications due to re-connections.
    scheduler.startup()
    scheduler.schedule("sync-acls", syncAcls, delay = 0l, period = 1l, unit = TimeUnit.HOURS)

    loadCache
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    val principal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, session.principal.getName)
    val host = session.host

    trace(s"Authorizer invoked to authorize principal = $principal from host = $host on resource = $resource for operation = $resource")

    if (superUsers.contains(principal)) {
      debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      return true
    }

    val acls: Set[Acl] = getAcls(resource)
    if (acls.isEmpty) {
      debug(s"No acl found for resource $resource , authorized = $shouldAllowEveryoneIfNoAclIsFound")
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
    debug("principal = %s from host = %s is not allowed to perform operation = %s  on resource = %s as no explicit acls were defined to allow the operation".format(principal, host, operation, resource))
    false
  }

  private def aclMatch(session: Session, operations: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    for (acl: Acl <- acls) {
      if (acl.permissionType.equals(permissionType)
        && (acl.principal.equals(principal) || acl.principal.equals(Acl.WildCardPrincipal))
        && (operations.equals(acl.operation) || acl.operation.equals(All))
        && (acl.host.contains(host) || acl.host.contains(Acl.WildCardHost))) {
        debug(s"operation = $operations on resource = $resource from host = $host is $permissionType.name based on acl = $acl")
        return true
      }
    }
    false
  }

  def syncAcls(): Unit = {
    debug("Syncing the acl cache for all resources that are currently in cache ")
    var resources: collection.Set[Resource] = Set.empty
    inReadLock(lock) {
      resources = aclCache.keySet
    }

    for (resource: Resource <- resources) {
      val resourceAcls: Set[Acl] = getAcls(resource)
      inWriteLock(lock) {
        aclCache.put(resource, resourceAcls)
      }
    }
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

  override def removeAcls(acls: Set[Acl], resource: Resource): Boolean = {
    if (!ZkUtils.pathExists(zkClient, toResourcePath(resource)))
      return false

    val existingAcls: Set[Acl] = getAcls(resource)
    val filteredAcls: Set[Acl] = existingAcls.filter((acl: Acl) => !acls.contains(acl))
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
      if (aclCache.contains(resource))
        return aclCache.get(resource).get
    }

    val aclJson: Option[String] = ZkUtils.readDataMaybeNull(zkClient, toResourcePath(resource))._1
    val acls: Set[Acl] = aclJson map ((x: String) => Acl.fromJson(x)) getOrElse Set.empty

    inWriteLock(lock) {
      aclCache.put(resource, acls)
    }
    acls
  }

  override def getAcls(principal: KafkaPrincipal): Set[Acl] = {
    val aclSets = aclCache.values
    var acls = Set.empty[Acl]
    for(aclSet: Set[Acl]  <- aclSets) {
      acls ++= aclSet.filter(acl => acl.principal.equals(principal))
    }
    acls.toSet[Acl]
  }

  def loadCache: Set[Acl] = {
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


  def toResourcePath(resource: Resource): String = {
    SimpleAclAuthorizer.AclZkPath + "/" + resource.resourceType + "/" + resource.name
  }

  def updateAclChangedFlag(resource: Resource): Unit = {
      ZkUtils.updatePersistentPath(zkClient, SimpleAclAuthorizer.AclChangedZkPath, resource.toString)
  }

  object AclChangeListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Object): Unit = {
      val resource: Resource = Resource.fromString(data.toString)
      //invalidate the cache entry
      inWriteLock(lock) {
        aclCache.remove(resource)
      }

      //repopulate the cache which is a side effect of calling getAcls with a resource that is not part of cache.
      getAcls(resource)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      //no op.
    }
  }

}