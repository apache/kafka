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

import kafka.server.{BrokerToControllerChannelManager, ControllerRequestCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AllocateProducerIdsRequest, AllocateProducerIdsResponse}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 *
 * ProducerIds are managed by the controller. When requesting a new range of IDs, we are guaranteed to receive
 * a unique block. The block start and block end are inclusive.
 */
object ProducerIdManager {
  val PidBlockSize = 1000L

  // Max time to wait on the next PID to be available
  val PidTimeoutMs = 60000

  // Once we reach this percentage of PIDs consumed from the current block, trigger a fetch of the next block
  val PidPrefetchThreshold = 0.90
}

case class ProducerIdBlock(brokerId: Int, blockStartId: Long, blockEndId: Long) {
  override def toString: String = {
    val producerIdBlockInfo = new StringBuilder
    producerIdBlockInfo.append("(brokerId:" + brokerId)
    producerIdBlockInfo.append(",blockStartProducerId:" + blockStartId)
    producerIdBlockInfo.append(",blockEndProducerId:" + blockEndId + ")")
    producerIdBlockInfo.toString()
  }

  val blockSize: Long = blockEndId - blockStartId + 1 // inclusive
}

trait ProducerIdGenerator {
  def generateProducerId(): Long
  def shutdown() : Unit = {}
}

class ProducerIdManager(brokerId: Int,
                        brokerEpochSupplier: () => Long,
                        controllerChannel: BrokerToControllerChannelManager) extends ProducerIdGenerator with Logging {

  this.logIdent = "[ProducerId Manager " + brokerId + "]: "

  private val nextProducerIdBlock = new ArrayBlockingQueue[Try[ProducerIdBlock]](1)
  private val requestInFlight = new AtomicBoolean(false)
  private var currentProducerIdBlock: ProducerIdBlock = ProducerIdBlock(brokerId, 0L, 0L)
  private var nextProducerId: Long = 0L

  // Send an initial request to get the first block
  maybeRequestNextBlock()

  override def generateProducerId(): Long = {
    this synchronized {
      // Advance the ID
      nextProducerId += 1

      // Check if we need to fetch the next block
      if (nextProducerId >= (currentProducerIdBlock.blockStartId + currentProducerIdBlock.blockSize * ProducerIdManager.PidPrefetchThreshold)) {
        maybeRequestNextBlock()
      }

      // If we've exhausted the current block, grab the next block (waiting if necessary)
      if (nextProducerId > currentProducerIdBlock.blockEndId) {
        val block = nextProducerIdBlock.poll(ProducerIdManager.PidTimeoutMs, TimeUnit.MILLISECONDS)
        if (block == null) {
          throw new KafkaException(s"sTimed out waiting for next block of Producer IDs after ${ProducerIdManager.PidTimeoutMs}ms.")
        } else {
          block match {
            case Success(nextBlock) =>
              currentProducerIdBlock = nextBlock
              nextProducerId = currentProducerIdBlock.blockStartId
            case Failure(t) => throw t
          }
        }
      }
      nextProducerId
    }
  }


  private def maybeRequestNextBlock(): Unit = {
    if (nextProducerIdBlock.isEmpty && requestInFlight.compareAndSet(false, true)) {
      sendRequest()
    }
  }

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

      override def onTimeout(): Unit = {
        warn("Encountered unexpected timeout when requesting AllocateProducerIds from the controller, trying again.")
        requestInFlight.set(false)
        maybeRequestNextBlock()
      }
    })
  }

  private[transaction] def handleAllocateProducerIdsResponse(response: AllocateProducerIdsResponse): Unit = {
    requestInFlight.set(false)
    val data = response.data
    Errors.forCode(data.errorCode()) match {
      case Errors.NONE =>
        debug(s"Got next producer ID block from controller $data")
        // Do some sanity checks on the response
        if (data.producerIdStart() < currentProducerIdBlock.blockEndId) {
          nextProducerIdBlock.put(Failure(new KafkaException(
            s"Producer ID block is not monotonic with current block: current=$currentProducerIdBlock response=$data")))
        } else if (data.producerIdStart() < 0 || data.producerIdLen() < 0 || data.producerIdStart() > Long.MaxValue - data.producerIdLen()) {
          nextProducerIdBlock.put(Failure(new KafkaException(s"Producer ID block includes invalid ID range: $data")))
        } else {
          nextProducerIdBlock.put(
            Success(ProducerIdBlock(brokerId, data.producerIdStart(), data.producerIdStart() + data.producerIdLen() - 1)))
        }
      case Errors.STALE_BROKER_EPOCH =>
        warn("Our broker epoch was stale, trying again.")
        maybeRequestNextBlock()
      case e: Errors =>
        warn("Had an unknown error from the controller, giving up.")
        nextProducerIdBlock.put(Failure(e.exception()))
    }
  }
}
