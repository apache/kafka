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
package kafka.coordinator.transaction

import kafka.common.KafkaException
import kafka.utils.{Json, Logging, ZkUtils}

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds (PIDs) in a unique way
 * such that the same PID will not be assigned twice across multiple transaction coordinators.
 *
 * PIDs are managed via ZooKeeper, where the latest pid block is written on the corresponding ZK path by the manager who
 * claims the block, where the written block_start_pid and block_end_pid are both inclusive.
 */
object ProducerIdManager extends Logging {
  val CurrentVersion: Long = 1L
  val PidBlockSize: Long = 1000L

  def generatePidBlockJson(pidBlock: ProducerIdBlock): String = {
    Json.encode(Map("version" -> CurrentVersion,
      "broker" -> pidBlock.brokerId,
      "block_start" -> pidBlock.blockStartPid.toString,
      "block_end" -> pidBlock.blockEndPid.toString)
    )
  }

  def parsePidBlockData(jsonData: String): ProducerIdBlock = {
    try {
      Json.parseFull(jsonData).flatMap { m =>
        val pidBlockInfo = m.asInstanceOf[Map[String, Any]]
        val brokerId = pidBlockInfo("broker").asInstanceOf[Int]
        val blockStartPID = pidBlockInfo("block_start").asInstanceOf[String].toLong
        val blockEndPID = pidBlockInfo("block_end").asInstanceOf[String].toLong
        Some(ProducerIdBlock(brokerId, blockStartPID, blockEndPID))
      }.getOrElse(throw new KafkaException(s"Failed to parse the pid block json $jsonData"))
    } catch {
      case e: java.lang.NumberFormatException =>
        // this should never happen: the written data has exceeded long type limit
        fatal(s"Read jason data $jsonData contains pids that have exceeded long type limit")
        throw e
    }
  }
}

case class ProducerIdBlock(brokerId: Int, blockStartPid: Long, blockEndPid: Long) {
  override def toString: String = {
    val pidBlockInfo = new StringBuilder
    pidBlockInfo.append("(brokerId:" + brokerId)
    pidBlockInfo.append(",blockStartPID:" + blockStartPid)
    pidBlockInfo.append(",blockEndPID:" + blockEndPid + ")")
    pidBlockInfo.toString()
  }
}

class ProducerIdManager(val brokerId: Int, val zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[ProducerId Manager " + brokerId + "]: "

  private var currentPIDBlock: ProducerIdBlock = null
  private var nextPID: Long = -1L

  // grab the first block of PIDs
  this synchronized {
    getNewPidBlock()
    nextPID = currentPIDBlock.blockStartPid
  }

  private def getNewPidBlock(): Unit = {
    var zkWriteComplete = false
    while (!zkWriteComplete) {
      // refresh current pid block from zookeeper again
      val (dataOpt, zkVersion) = zkUtils.readDataAndVersionMaybeNull(ZkUtils.PidBlockPath)

      // generate the new pid block
      currentPIDBlock = dataOpt match {
        case Some(data) =>
          val currPIDBlock = ProducerIdManager.parsePidBlockData(data)
          debug(s"Read current pid block $currPIDBlock, Zk path version $zkVersion")

          if (currPIDBlock.blockEndPid > Long.MaxValue - ProducerIdManager.PidBlockSize) {
            // we have exhausted all pids (wow!), treat it as a fatal error
            fatal(s"Exhausted all pids as the next block's end pid is will has exceeded long type limit (current block end pid is ${currPIDBlock.blockEndPid})")
            throw new KafkaException("Have exhausted all pids.")
          }

          ProducerIdBlock(brokerId, currPIDBlock.blockEndPid + 1L, currPIDBlock.blockEndPid + ProducerIdManager.PidBlockSize)
        case None =>
          debug(s"There is no pid block yet (Zk path version $zkVersion), creating the first block")
          ProducerIdBlock(brokerId, 0L, ProducerIdManager.PidBlockSize - 1)
      }

      val newPIDBlockData = ProducerIdManager.generatePidBlockJson(currentPIDBlock)

      // try to write the new pid block into zookeeper
      val (succeeded, version) = zkUtils.conditionalUpdatePersistentPath(ZkUtils.PidBlockPath, newPIDBlockData, zkVersion, Some(checkPidBlockZkData))
      zkWriteComplete = succeeded

      if (zkWriteComplete)
        info(s"Acquired new pid block $currentPIDBlock by writing to Zk with path version $version")
    }
  }

  private def checkPidBlockZkData(zkUtils: ZkUtils, path: String, expectedData: String): (Boolean, Int) = {
    try {
      val expectedPidBlock = ProducerIdManager.parsePidBlockData(expectedData)
      val (dataOpt, zkVersion) = zkUtils.readDataAndVersionMaybeNull(ZkUtils.PidBlockPath)
      dataOpt match {
        case Some(data) =>
          val currPIDBlock = ProducerIdManager.parsePidBlockData(data)
          (currPIDBlock.equals(expectedPidBlock), zkVersion)
        case None =>
          (false, -1)
      }
    } catch {
      case e: Exception =>
        warn(s"Error while checking for pid block Zk data on path $path: expected data $expectedData", e)
        
        (false, -1)
    }
  }

  def nextPid(): Long = {
    this synchronized {
      // grab a new block of PIDs if this block has been exhausted
      if (nextPID > currentPIDBlock.blockEndPid) {
        getNewPidBlock()
        nextPID = currentPIDBlock.blockStartPid + 1
      } else {
        nextPID += 1
      }

      nextPID - 1
    }
  }

  def shutdown() {
    info(s"Shutdown complete: last PID assigned $nextPID")
  }
}
