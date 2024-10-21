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

import kafka.coordinator.transaction.ProducerIdManager.{IterationLimit, NoRetry, RetryBackoffMs}
import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AllocateProducerIdsRequest, AllocateProducerIdsResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.common.ProducerIdsBlock

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.jdk.OptionConverters.RichOptional
import scala.util.{Failure, Success, Try}

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 *
 * ProducerIds are managed by the controller. When requesting a new range of IDs, we are guaranteed to receive
 * a unique block.
 */

object ProducerIdManager {
  // Once we reach this percentage of PIDs consumed from the current block, trigger a fetch of the next block
  val PidPrefetchThreshold: Double = 0.90
  val IterationLimit: Int = 3
  val RetryBackoffMs: Int = 50
  val NoRetry: Long = -1L

  // Creates a ProducerIdGenerate that directly interfaces with ZooKeeper, IBP < 3.0-IV0
  def zk(brokerId: Int, zkClient: KafkaZkClient): ZkProducerIdManager = {
    new ZkProducerIdManager(brokerId, zkClient)
  }

  // Creates a ProducerIdGenerate that uses AllocateProducerIds RPC, IBP >= 3.0-IV0
  def rpc(brokerId: Int,
          time: Time,
          brokerEpochSupplier: () => Long,
          controllerChannel: NodeToControllerChannelManager): RPCProducerIdManager = {

    new RPCProducerIdManager(brokerId, time, brokerEpochSupplier, controllerChannel)
  }
}

trait ProducerIdManager {
  def generateProducerId(): Try[Long]
  def shutdown() : Unit = {}

  // For testing purposes
  def hasValidBlock: Boolean
}

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

  def generateProducerId(): Try[Long] = {
    this synchronized {
      // grab a new block of producerIds if this block has been exhausted
      if (nextProducerId > currentProducerIdBlock.lastProducerId) {
        try {
          allocateNewProducerIdBlock()
        } catch {
          case t: Throwable =>
            return Failure(t)
        }
        nextProducerId = currentProducerIdBlock.firstProducerId
      }
      nextProducerId += 1
      Success(nextProducerId - 1)
    }
  }

  override def hasValidBlock: Boolean = {
    this synchronized {
      !currentProducerIdBlock.equals(ProducerIdsBlock.EMPTY)
    }
  }
}

/**
 * RPCProducerIdManager allocates producer id blocks asynchronously and will immediately fail requests
 * for producers to retry if it does not have an available producer id and is waiting on a new block.
 */
class RPCProducerIdManager(brokerId: Int,
                           time: Time,
                           brokerEpochSupplier: () => Long,
                           controllerChannel: NodeToControllerChannelManager) extends ProducerIdManager with Logging {

  this.logIdent = "[RPC ProducerId Manager " + brokerId + "]: "

  // Visible for testing
  private[transaction] var nextProducerIdBlock = new AtomicReference[ProducerIdsBlock](null)
  private val currentProducerIdBlock: AtomicReference[ProducerIdsBlock] = new AtomicReference(ProducerIdsBlock.EMPTY)
  private val requestInFlight = new AtomicBoolean(false)
  private val backoffDeadlineMs = new AtomicLong(NoRetry)

  override def hasValidBlock: Boolean = {
    nextProducerIdBlock.get != null
  }

  override def generateProducerId(): Try[Long] = {
    var result: Try[Long] = null
    var iteration = 0
    while (result == null) {
      currentProducerIdBlock.get.claimNextId().toScala match {
        case None =>
          // Check the next block if current block is full
          val block = nextProducerIdBlock.getAndSet(null)
          if (block == null) {
            // Return COORDINATOR_LOAD_IN_PROGRESS rather than REQUEST_TIMED_OUT since older clients treat the error as fatal
            // when it should be retriable like COORDINATOR_LOAD_IN_PROGRESS.
            maybeRequestNextBlock()
            result = Failure(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block"))
          } else {
            currentProducerIdBlock.set(block)
            requestInFlight.set(false)
            iteration = iteration + 1
          }

        case Some(nextProducerId) =>
          // Check if we need to prefetch the next block
          val prefetchTarget = currentProducerIdBlock.get.firstProducerId + (currentProducerIdBlock.get.size * ProducerIdManager.PidPrefetchThreshold).toLong
          if (nextProducerId == prefetchTarget) {
            maybeRequestNextBlock()
          }
          result = Success(nextProducerId)
      }
      if (iteration == IterationLimit) {
        result = Failure(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block"))
      }
    }
    result
  }


  private def maybeRequestNextBlock(): Unit = {
    val retryTimestamp = backoffDeadlineMs.get()
    if (retryTimestamp == NoRetry || time.milliseconds() >= retryTimestamp) {
      // Send a request only if we reached the retry deadline, or if no deadline was set.

      if (nextProducerIdBlock.get == null &&
        requestInFlight.compareAndSet(false, true)) {

        sendRequest()
        // Reset backoff after a successful send.
        backoffDeadlineMs.set(NoRetry)
      }
    }
  }

  // Visible for testing
  private[transaction] def sendRequest(): Unit = {
    val message = new AllocateProducerIdsRequestData()
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setBrokerId(brokerId)

    val request = new AllocateProducerIdsRequest.Builder(message)
    debug("Requesting next Producer ID block")
    controllerChannel.sendRequest(request, new ControllerRequestCompletionHandler() {
      override def onComplete(response: ClientResponse): Unit = {
        val message = response.responseBody().asInstanceOf[AllocateProducerIdsResponse]
        handleAllocateProducerIdsResponse(message)
      }

      override def onTimeout(): Unit = handleTimeout()
    })
  }

  // Visible for testing
  private[transaction] def handleAllocateProducerIdsResponse(response: AllocateProducerIdsResponse): Unit = {
    val data = response.data
    var successfulResponse = false
    Errors.forCode(data.errorCode()) match {
      case Errors.NONE =>
        debug(s"Got next producer ID block from controller $data")
        // Do some sanity checks on the response
        if (data.producerIdStart() < currentProducerIdBlock.get.lastProducerId) {
          error(s"Producer ID block is not monotonic with current block: current=$currentProducerIdBlock response=$data")
        } else if (data.producerIdStart() < 0 || data.producerIdLen() < 0 || data.producerIdStart() > Long.MaxValue - data.producerIdLen()) {
          error(s"Producer ID block includes invalid ID range: $data")
        } else {
          nextProducerIdBlock.set(new ProducerIdsBlock(brokerId, data.producerIdStart(), data.producerIdLen()))
          successfulResponse = true
        }
      case Errors.STALE_BROKER_EPOCH =>
        warn("Our broker currentBlockCount was stale, trying again.")
      case Errors.BROKER_ID_NOT_REGISTERED =>
        warn("Our broker ID is not yet known by the controller, trying again.")
      case e: Errors =>
        error(s"Received an unexpected error code from the controller: $e")
    }

    if (!successfulResponse) {
      // There is no need to compare and set because only one thread
      // handles the AllocateProducerIds response.
      backoffDeadlineMs.set(time.milliseconds() + RetryBackoffMs)
      requestInFlight.set(false)
    }
  }

  private def handleTimeout(): Unit = {
    warn("Timed out when requesting AllocateProducerIds from the controller.")
    requestInFlight.set(false)
  }
}
