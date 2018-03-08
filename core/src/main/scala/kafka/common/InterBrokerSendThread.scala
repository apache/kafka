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
package kafka.common

import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time


/**
 *  Class for inter-broker send thread that utilize a non-blocking network client.
 */
abstract class InterBrokerSendThread(name: String,
                                     networkClient: NetworkClient,
                                     time: Time,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  def generateRequests(): Iterable[RequestAndCompletionHandler]
  var unsentRequests = new UnsentRequests

  override def shutdown(): Unit = {
    initiateShutdown()
    // wake up the thread in case it is blocked inside poll
    networkClient.wakeup()
    awaitShutdown()
  }

  override def doWork() {
    val now = time.milliseconds()
    var pollTimeout = Long.MaxValue

    try {
      generateRequests()
      if (unsentRequests.hasRequests) {
        unsentRequests.nodes.toArray.toList.foreach { reqNode =>
          val node = reqNode.asInstanceOf[Node]
          val requestIterator = unsentRequests.requestIterator(node)
          while (requestIterator.hasNext) {
            val request = requestIterator.next
            val destination = Integer.toString(request.destination.id())
            val completionHandler = request.handler
            val clientRequest = networkClient.newClientRequest(destination,
              request.request,
              now,
              true,
              completionHandler)

            if (networkClient.ready(request.destination, now)) {
              networkClient.send(clientRequest, now)
              requestIterator.remove()
              unsentRequests.clean()
            }
          }
        }
      }
      networkClient.poll(pollTimeout, now)
    } catch {
      case e: FatalExitError => throw e
      case t: Throwable =>
        error(s"unhandled exception caught in InterBrokerSendThread", t)
        // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
        // as we will be in an unknown state with potentially some requests dropped and not
        // being able to make progress. Known and expected Errors should have been appropriately
        // dealt with already.
        throw new FatalExitError()
    }
  }

  def wakeup(): Unit = networkClient.wakeup()

}

case class RequestAndCompletionHandler(destination: Node, request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       handler: RequestCompletionHandler)

class UnsentRequests {
  private var unsent = new ConcurrentHashMap[Node, ConcurrentLinkedQueue[RequestAndCompletionHandler]]

  def put(node: Node, request: RequestAndCompletionHandler): Unit = { // the lock protects the put from a concurrent removal of the queue for the node
    this.synchronized {
      var requests: ConcurrentLinkedQueue[RequestAndCompletionHandler] = unsent.get(node)
      if (requests == null) {
        requests = new ConcurrentLinkedQueue[RequestAndCompletionHandler]
        unsent.putIfAbsent(node, requests)
      }
      requests.add(request)
    }
  }

  def hasRequests: Boolean = {
    import scala.collection.JavaConversions._
    for (requests <- unsent.values) {
      if (!requests.isEmpty) return true
    }
    false
  }

  def clean(): Unit = {
    // the lock protects removal from a concurrent put which could otherwise mutate the
    // queue after it has been removed from the map
    this.synchronized {
      val iterator: util.Iterator[ConcurrentLinkedQueue[RequestAndCompletionHandler]] = unsent.values.iterator
      while ( {
        iterator.hasNext
      }) {
        val requests: ConcurrentLinkedQueue[RequestAndCompletionHandler] = iterator.next
        if (requests.isEmpty) iterator.remove()
      }
    }
  }

  def requestIterator(node: Node): util.Iterator[RequestAndCompletionHandler] = {
    val requests: ConcurrentLinkedQueue[RequestAndCompletionHandler] = unsent.get(node)
    if (requests == null) Collections.emptyIterator[RequestAndCompletionHandler]
    else requests.iterator
  }

  def nodes: util.Collection[Node] = unsent.keySet
}
