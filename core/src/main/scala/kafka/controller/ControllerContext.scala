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

package kafka.controller

import scala.collection.Seq

object ReplicaAssignment {
  def apply(replicas: Seq[Int]): ReplicaAssignment = {
    apply(replicas, Seq.empty, Seq.empty)
  }

  val empty: ReplicaAssignment = apply(Seq.empty)
}


/**
 * @param replicas the sequence of brokers assigned to the partition. It includes the set of brokers
 *                 that were added (`addingReplicas`) and removed (`removingReplicas`).
 * @param addingReplicas the replicas that are being added if there is a pending reassignment
 * @param removingReplicas the replicas that are being removed if there is a pending reassignment
 */
case class ReplicaAssignment private (replicas: Seq[Int],
                                      addingReplicas: Seq[Int],
                                      removingReplicas: Seq[Int]) {

  lazy val targetReplicas: Seq[Int] = replicas.diff(removingReplicas)

  def isBeingReassigned: Boolean = {
    addingReplicas.nonEmpty || removingReplicas.nonEmpty
  }

  override def toString: String = s"ReplicaAssignment(" +
    s"replicas=${replicas.mkString(",")}, " +
    s"addingReplicas=${addingReplicas.mkString(",")}, " +
    s"removingReplicas=${removingReplicas.mkString(",")})"
}

