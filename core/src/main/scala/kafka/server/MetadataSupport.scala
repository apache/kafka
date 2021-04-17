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

package kafka.server

import kafka.controller.KafkaController
import kafka.network.RequestChannel
import kafka.security.authorizer.AclAuthorizer
import kafka.server.metadata.{ClientQuotaCache, RaftMetadataCache}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractResponse

sealed trait MetadataSupport {
  /**
   * Provide a uniform way of getting to the ForwardingManager, which is a shared concept
   * despite being optional when using ZooKeeper and required when using Raft
   */
  val forwardingManager: Option[ForwardingManager]

  /**
   * Return this instance downcast for use with ZooKeeper
   *
   * @param createException function to create an exception to throw
   * @return this instance downcast for use with ZooKeeper
   * @throws Exception if this instance is not for ZooKeeper
   */
  def requireZkOrThrow(createException: => Exception): ZkSupport

  /**
   * Ensure that we are using ZooKeeper for ACLs
   *
   * @param createException function to create an exception to throw
   * @throws Exception if this instance is not using ZooKeeper for ACLs
   */
  def requireZkAuthorizerOrThrow(createException: => Exception): Unit

  /**
   * Return this instance downcast for use with Raft
   *
   * @param createException function to create an exception to throw
   * @return this instance downcast for use with Raft
   * @throws Exception if this instance is not for Raft
   */
  def requireRaftOrThrow(createException: => Exception): RaftSupport

  def maybeForward(request: RequestChannel.Request,
                   handler: RequestChannel.Request => Unit,
                   responseCallback: Option[AbstractResponse] => Unit): Unit

  def controllerId: Option[Int]
}

case class ZkSupport(adminManager: ZkAdminManager,
                     controller: KafkaController,
                     zkClient: KafkaZkClient,
                     forwardingManager: Option[ForwardingManager],
                     metadataCache: ZkMetadataCache,
                     config: KafkaConfig) extends MetadataSupport {
  if (!config.requiresZookeeper) {
    throw new IllegalStateException("Config specifies Raft but metadata support instance is for ZooKeeper")
  }
  val adminZkClient = new AdminZkClient(zkClient)

  override def requireZkOrThrow(createException: => Exception): ZkSupport = this
  override def requireZkAuthorizerOrThrow(createException: => Exception) = {}

  override def requireRaftOrThrow(createException: => Exception): RaftSupport = throw createException

  override def maybeForward(request: RequestChannel.Request,
                            handler: RequestChannel.Request => Unit,
                            responseCallback: Option[AbstractResponse] => Unit): Unit = {
    forwardingManager match {
      case Some(mgr) if !request.isForwarded && !controller.isActive => mgr.forwardRequest(request, responseCallback)
      case _ => handler(request)
    }
  }

  override def controllerId: Option[Int] =  metadataCache.getControllerId
}

case class RaftSupport(fwdMgr: ForwardingManager, metadataCache: RaftMetadataCache, quotaCache: ClientQuotaCache, config: KafkaConfig)
    extends MetadataSupport {
  if (config.requiresZookeeper) {
    throw new IllegalStateException("Config specifies ZooKeeper but metadata support instance is for Raft")
  }
  override val forwardingManager: Option[ForwardingManager] = Some(fwdMgr)
  override def requireZkOrThrow(createException: => Exception): ZkSupport = throw createException
  override def requireZkAuthorizerOrThrow(createException: => Exception) = {
    if (!hasZkAuthorizer) {
      throw createException
    }
  }
  override def requireRaftOrThrow(createException: => Exception): RaftSupport = this

  override def maybeForward(request: RequestChannel.Request,
                            handler: RequestChannel.Request => Unit,
                            responseCallback: Option[AbstractResponse] => Unit): Unit = {
    if (!request.isForwarded) {
      request.header.apiKey match {
        case ApiKeys.CREATE_ACLS | ApiKeys.DELETE_ACLS =>
          if (hasZkAuthorizer) {
            handler(request)
          } else {
            fwdMgr.forwardRequest(request, responseCallback)
          }
        case _ => fwdMgr.forwardRequest(request, responseCallback)
      }
    } else {
      handler(request) // will reject
    }
  }

  private def hasZkAuthorizer = {
    classOf[AclAuthorizer].getName == config.getString(KafkaConfig.AuthorizerClassNameProp)
  }

  override def controllerId: Option[Int] = {
    // We send back a random controller ID when running with a Raft-based metadata quorum.
    // Raft-based controllers are not directly accessible to clients; rather, clients can send
    // requests destined for the controller to any broker node, and the receiving broker will
    // automatically forward the request on the client's behalf to the active Raft-based
    // controller  as per KIP-590.
    metadataCache.currentImage().brokers.randomAliveBrokerId()
  }

}
