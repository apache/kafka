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
import kafka.zk.{AdminZkClient, KafkaZkClient}

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
  def requireZk(createException: () => Exception): ZkSupport = {
    this match {
      case zkSupport@ZkSupport(_, _, _, _) => zkSupport
      case RaftSupport(_) => throw createException()
    }
  }

  /**
   * Return this instance downcast for use with Raft
   *
   * @param createException function to create an exception to throw
   * @return this instance downcast for use with Raft
   * @throws Exception if this instance is not for Raft
   */
  def requireRaft(createException: () => Exception): RaftSupport = {
    this match {
      case raftSupport@RaftSupport(_) => raftSupport
      case ZkSupport(_, _, _, _) => throw createException()
    }
  }
}

case class ZkSupport(adminManager: ZkAdminManager,
                     controller: KafkaController,
                     zkClient: KafkaZkClient,
                     forwardingManager: Option[ForwardingManager]) extends MetadataSupport {
  private val _adminZkClient = new AdminZkClient(zkClient)
  def adminZkClient: AdminZkClient = _adminZkClient
}

case class RaftSupport(fwdMgr: ForwardingManager) extends MetadataSupport {
  override val forwardingManager: Option[ForwardingManager] = Some(fwdMgr)
}
