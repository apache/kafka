/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import scala.collection.mutable

class MockReplicaStateMachine(controllerContext: ControllerContext) extends ReplicaStateMachine(controllerContext) {
  val stateChangesByTargetState = mutable.Map.empty[ReplicaState, Int].withDefaultValue(0)

  def stateChangesCalls(targetState: ReplicaState): Int = {
    stateChangesByTargetState(targetState)
  }

  def clear(): Unit = {
    stateChangesByTargetState.clear()
  }

  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    stateChangesByTargetState(targetState) = stateChangesByTargetState(targetState) + 1

    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    if (invalidReplicas.nonEmpty) {
      val currentStates = invalidReplicas.map(replica => replica -> controllerContext.replicaStates.get(replica)).toMap
      throw new IllegalStateException(s"Invalid state transition to $targetState for replicas $currentStates")
    }
    validReplicas.foreach { replica =>
      if (targetState == NonExistentReplica)
        controllerContext.removeReplicaState(replica)
      else
        controllerContext.putReplicaState(replica, targetState)
    }
  }

}
