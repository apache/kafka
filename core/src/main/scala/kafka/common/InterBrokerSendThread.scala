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

import kafka.admin.AdminClient
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConversions._


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
  val unsentExpiryMs = AdminClient.DefaultRequestTimeoutMs

  override def shutdown(): Unit = {
    initiateShutdown()
    // wake up the thread in case it is blocked inside poll
    networkClient.wakeup()
    awaitShutdown()
  }

  override def doWork() {
    val now = time.milliseconds()
    var pollTimeout = Long.MaxValue

    generateRequests().foreach { request =>
      val completionHandler = request.handler
      unsentRequests.put(request.destination,
        networkClient.newClientRequest(request.destination.idString(), request.request, now, true, completionHandler))
    }

    try {
      if (unsentRequests.hasRequests) {
        for (node <- unsentRequests.nodes) {
          val requestIterator = unsentRequests.requestIterator(node)
          while (requestIterator.hasNext) {
            val request = requestIterator.next
            if (networkClient.ready(node, now)) {
              networkClient.send(request, now)
              requestIterator.remove()
              unsentRequests.clean()
            } else
              pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(node, now))
          }
        }
      }
      networkClient.poll(pollTimeout, now)
      checkDisconnects(now)
      failExpiredRequests(now)
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

  private def checkDisconnects(now: Long): Unit = {
    // any disconnects affecting requests that have already been transmitted will be handled
    // by NetworkClient, so we just need to check whether connections for any of the unsent
    // requests have been disconnected; if they have, then we complete the corresponding future
    // and set the disconnect flag in the ClientResponse
    for (node <- unsentRequests.nodes) {
      if (networkClient.connectionFailed(node)) {
        // Remove entry before invoking request callback to avoid callbacks handling
        // coordinator failures traversing the unsent list again.
        val requests = unsentRequests.remove(node)
        for (request: ClientRequest <- requests) {
          val handler = request.callback
          val authenticationException = networkClient.authenticationException(node)
          if (authenticationException == null) {
            handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
              handler, request.destination, now /* createdTimeMs */ , now /* receivedTimeMs */ , true /* disconnected */ ,
              null /* versionMismatch */ , null /* responseBody */))
          }
        }
      }
    }
  }

  private def failExpiredRequests(now: Long): Unit = {
    // clear all expired unsent requests
    val expiredRequests = unsentRequests.removeExpiredRequests(now, unsentExpiryMs)
    for (request: ClientRequest <- expiredRequests)
      info(s"Failed to send the following request after $unsentExpiryMs ms: ${request.toString}")
  }

  def wakeup(): Unit = networkClient.wakeup()
}

case class RequestAndCompletionHandler(destination: Node, request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       handler: RequestCompletionHandler)

class UnsentRequests {
  private var unsent = new ConcurrentHashMap[Node, ConcurrentLinkedQueue[ClientRequest]]

  def put(node: Node, request: ClientRequest): Unit = {
    // the lock protects the put from a concurrent removal of the queue for the node
    this.synchronized {
      var requests = unsent.get(node)
      if (requests == null) {
        requests = new ConcurrentLinkedQueue[ClientRequest]
        unsent.putIfAbsent(node, requests)
      }
      requests.add(request)
    }
  }

  def hasRequests: Boolean = {
    for (requests <- unsent.values)
      if (!requests.isEmpty) return true
    false
  }

  def removeExpiredRequests(now: Long, unsentExpiryMs: Long): util.Collection[ClientRequest] = {
    val expiredRequests = new util.ArrayList[ClientRequest]
    for (requests: ConcurrentLinkedQueue[ClientRequest] <- unsent.values) {
      val requestIterator = requests.iterator
      var foundExpiredRequest = false
      while (requestIterator.hasNext && !foundExpiredRequest) {
        val request = requestIterator.next
        if (request.createdTimeMs < now - unsentExpiryMs) {
          expiredRequests.add(request)
          requestIterator.remove()
          foundExpiredRequest = true
        }
      }
    }
    expiredRequests
  }

  def clean(): Unit = {
    // the lock protects removal from a concurrent put which could otherwise mutate the
    // queue after it has been removed from the map
    this.synchronized {
      val iterator = unsent.values.iterator
      while (iterator.hasNext) {
        val requests = iterator.next
        if (requests.isEmpty)
          iterator.remove()
      }
    }
  }

  def remove(node: Node): util.Collection[ClientRequest] = {
    // the lock protects removal from a concurrent put which could otherwise mutate the
    // queue after it has been removed from the map
    this.synchronized {
      val requests = unsent.remove(node)
      if (requests == null)
        new ConcurrentLinkedQueue[ClientRequest]()
      else
        requests
    }
  }

  def requestIterator(node: Node): util.Iterator[ClientRequest] = {
    val requests = unsent.get(node)
    if (requests == null)
      Collections.emptyIterator[ClientRequest]
    else
      requests.iterator
  }

  def nodes = unsent.keySet
}