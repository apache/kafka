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

import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.server.common.ProducerIdsBlock

object ZkProducerIdManager {
  def getNewProducerIdBlock(brokerId: Int, zkClient: KafkaZkClient, logger: Logging): ProducerIdsBlock = {
    // Get or create the existing PID block from ZK and attempt to update it. We retry in a loop here since other
    // brokers may be generating PID blocks during a rolling upgrade
    var zkWriteComplete = false
    while (!zkWriteComplete) {
      // refresh current producerId block from zookeeper again
      val (dataOpt, zkVersion) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)

      // generate the new producerId block
      val newProducerIdBlock = dataOpt match {
        case Some(data) =>
          val currProducerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
          logger.debug(s"Read current producerId block $currProducerIdBlock, Zk path version $zkVersion")

          if (currProducerIdBlock.lastProducerId > Long.MaxValue - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            // we have exhausted all producerIds (wow!), treat it as a fatal error
            logger.fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.lastProducerId})")
            throw new KafkaException("Have exhausted all producerIds.")
          }

          new ProducerIdsBlock(brokerId, currProducerIdBlock.nextBlockFirstId(), ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
        case None =>
          logger.debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block")
          new ProducerIdsBlock(brokerId, 0L, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      }

      val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(newProducerIdBlock)

      // try to write the new producerId block into zookeeper
      val (succeeded, version) = zkClient.conditionalUpdatePath(ProducerIdBlockZNode.path, newProducerIdBlockData, zkVersion, None)
      zkWriteComplete = succeeded

      if (zkWriteComplete) {
        logger.info(s"Acquired new producerId block $newProducerIdBlock by writing to Zk with path version $version")
        return newProducerIdBlock
      }
    }
    throw new IllegalStateException()
  }
}

class ZkProducerIdManager(brokerId: Int, zkClient: KafkaZkClient) extends ProducerIdManager with Logging {

  this.logIdent = "[ZK ProducerId Manager " + brokerId + "]: "
  val RETRY_BACKOFF_MS = 50

  private var currentProducerIdBlock: ProducerIdsBlock = ProducerIdsBlock.EMPTY
  private var nextProducerId: Long = _

  // grab the first block of producerIds
  this synchronized {
    allocateNewProducerIdBlock()
    nextProducerId = currentProducerIdBlock.firstProducerId
  }

  private def allocateNewProducerIdBlock(): Unit = {
    this synchronized {
      currentProducerIdBlock = ZkProducerIdManager.getNewProducerIdBlock(brokerId, zkClient, this)
    }
  }

  def generateProducerId(): Long = {
    this synchronized {
      // grab a new block of producerIds if this block has been exhausted
      if (nextProducerId > currentProducerIdBlock.lastProducerId) {
        try {
          allocateNewProducerIdBlock()
        } catch {
          case t: Throwable => throw new KafkaException("Failed to acquire a new block of producerIds", t)
        }
        nextProducerId = currentProducerIdBlock.firstProducerId
      }
      nextProducerId += 1
      nextProducerId - 1
    }
  }
}