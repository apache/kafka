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

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

trait BrokerToControllerRequestItem {
  def topicPartition(): TopicPartition
}

trait BrokerToControllerRequestManager[Item <: BrokerToControllerRequestItem] {
  def start(): Unit

  def shutdown(): Unit

  def submit(item: Item): Boolean
}

object BrokerToControllerRequestManager {
  val defaultTimeout = 60000
}

abstract class AbstractBrokerToControllerRequestManager[Item <: BrokerToControllerRequestItem](
  val controllerChannelManager: BrokerToControllerChannelManager,
  val scheduler: Scheduler,
  val time: Time,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long
) extends BrokerToControllerRequestManager[Item] with Logging with KafkaMetricsGroup {

  private[server] val unsentItemQueue: BlockingQueue[Item] = new LinkedBlockingQueue()
  // The inflightItems map is populated by the unsentItemQueue, and the items in it are removed in the response callback.
  // Putting new items to the inflightItems and removing items from it won't be performed at the same time, and the coordination is
  // done via the inflightRequest flag.
  private[server] val inflightItems: util.Map[TopicPartition, Item] = new ConcurrentHashMap[TopicPartition, Item]()

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  override def start(): Unit = {
    controllerChannelManager.start()
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  override def submit(item: Item): Boolean = {
    unsentItemQueue.put(item)
    maybeSendRequest()
    true
  }

  private[server] def maybeSendRequest(): Unit = {
    // Send all pending items if there is not already a request in-flight.
    if ((!unsentItemQueue.isEmpty || !inflightItems.isEmpty) && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent ISRs but don't remove from the map, they get cleared in the response handler
      while (!unsentItemQueue.isEmpty) {
        val item = unsentItemQueue.poll()
        // if there are multiple items for the same partition in the queue, the last one wins
        // and the previous ones will be discarded
        inflightItems.put(item.topicPartition, item)
      }
      // Since the maybeSendRequest can be called from the response callback,
      // there may be cases where right after the while loop, some new items are added to the unsentIsrQueue in the submit
      // thread. In such a case, the newly added item will be sent in the next request

      val itemsToSend = new ListBuffer[Item]()
      inflightItems.values().forEach(item => itemsToSend.append(item))
      sendRequest(itemsToSend)
    }
  }

  def removeInflightItem(topicPartition: TopicPartition): Unit = {
    inflightItems.remove(topicPartition)
  }

  private[server] def clearInFlightRequest(): Unit = {
    if (!inflightRequest.compareAndSet(true, false)) {
      warn("Attempting to clear in-flight flag when no apparent request is in-flight")
    }
  }

  private def sendRequest(itemsToSend: Seq[Item]): Unit = {
    val brokerEpoch = brokerEpochSupplier.apply()
    val request = buildRequest(itemsToSend, brokerEpoch)
    debug(s"Sending to controller $request")

    // We will not timeout the request, instead letting it retry indefinitely
    // until a response is received.
    controllerChannelManager.sendRequest(request,
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          debug(s"Received response $response")
          val error = try {
            if (response.authenticationException != null) {
              // For now we treat authentication errors as retriable. We use the
              // `NETWORK_EXCEPTION` error code for lack of a good alternative.
              // Note that `BrokerToControllerChannelManager` will still log the
              // authentication errors so that users have a chance to fix the problem.
              Errors.NETWORK_EXCEPTION
            } else if (response.versionMismatch != null) {
              Errors.UNSUPPORTED_VERSION
            } else {
              handleResponse(response, brokerEpoch, itemsToSend)
            }
          } finally {
            // clear the flag so future requests can proceed
            clearInFlightRequest()
          }

          // check if we need to send another request right away
          error match {
            case Errors.NONE =>
              // In the normal case, check for pending updates to send immediately
              maybeSendRequest()
            case _ =>
              // If we received a top-level error from the controller, retry the request in the near future
              scheduler.schedule("send-alter-isr", () => maybeSendRequest(), 50, -1, TimeUnit.MILLISECONDS)
          }
        }

        override def onTimeout(): Unit = {
          throw new IllegalStateException("Encountered unexpected timeout when sending request to the controller")
        }
      })
  }

  def buildRequest(inflightItems: Seq[Item], brokerEpoch: Long): AbstractRequest.Builder[_ <: AbstractRequest]

  def handleResponse(clientResponse: ClientResponse, sentBrokerEpoch: Long, itemsToSend: Seq[Item]): Errors
}
