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

package kafka.coordinator

import java.util

import kafka.utils.nonthreadsafe

import scala.collection.Map

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 */
@nonthreadsafe
private[coordinator] class MemberMetadata(val memberId: String,
                                          val groupId: String,
                                          val sessionTimeoutMs: Int,
                                          var metadata: Array[Byte],
                                          var subProtocols: Set[String]) {
  def matches(metadata: Array[Byte], subProtocols: Set[String]): Boolean = {
    util.Arrays.equals(metadata, this.metadata) && subProtocols == this.subProtocols
  }

  def hasAssignment(assignment: Array[Byte]): Boolean = {
    util.Arrays.equals(assignment, this.assignment)
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    subProtocols.find(candidates.contains(_)) match {
      case Some(protocol) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  var assignment: Array[Byte] = null

  var awaitingJoinCallback: JoinGroupResult => Unit = null
  var awaitingSyncCallback: (Array[Byte], Short) => Unit = null
  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false
}
