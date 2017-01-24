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

import kafka.utils.CoreUtils._
import kafka.utils.{Json, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit,
                             brokerId: Int,
                             time: Time)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      elect
    }
  }

  def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  def elect: Boolean = {
    val timestamp = time.milliseconds.toString
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))
   
   leaderId = getControllerID 
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId
      onBecomingLeader()
    } catch {
      case _: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
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
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
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
    @throws[Exception]
    def handleDataChange(dataPath: String, data: Object) {
      val shouldResign = inLock(controllerContext.controllerLock) {
        val amILeaderBeforeDataChange = amILeader
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        amILeaderBeforeDataChange && !amILeader
      }

      if (shouldResign)
        onResigningAsLeader()
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    @throws[Exception]
    def handleDataDeleted(dataPath: String) { 
      val shouldResign = inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        amILeader
      }

      if (shouldResign)
        onResigningAsLeader()

      inLock(controllerContext.controllerLock) {
        elect
      }
    }
  }
}
