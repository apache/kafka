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

import kafka.utils.ZkUtils._
import kafka.utils.Logging
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext, electionPath: String, onBecomingLeader: () => Unit,
                             brokerId: Int)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    controllerContext.controllerLock synchronized {
      elect
    }
  }

  def elect: Boolean = {
    controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
    try {
      createEphemeralPathExpectConflict(controllerContext.zkClient, electionPath, brokerId.toString)
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId
      onBecomingLeader()
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        val data: String = controllerContext.zkClient.readData(electionPath, true)
        debug("Broker %d was elected as leader instead of broker %d".format(data.toInt, brokerId))
        if (data != null) {
          leaderId = data.toInt
        }
      case e2 =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  def resign() = {
    deletePath(controllerContext.zkClient, electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      controllerContext.controllerLock synchronized {
        leaderId = data.toString.toInt
        info("New leader is %d".format(leaderId))
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      controllerContext.controllerLock synchronized {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        elect
      }
    }
  }
}
