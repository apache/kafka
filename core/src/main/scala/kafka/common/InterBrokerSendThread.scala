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

import java.util.{ArrayDeque, ArrayList, Collection, Collections, HashMap, Iterator}
import java.util.Map.Entry

import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._

/**
 *  Class for inter-broker send thread that utilize a non-blocking network client.
 */
abstract class InterBrokerSendThread(name: String,
                                     networkClient: KafkaClient,
                                     time: Time,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  def generateRequests(): Iterable[RequestAndCompletionHandler]
  def requestTimeoutMs: Int
  private val unsentRequests = new UnsentRequests

  def hasUnsentRequests = unsentRequests.iterator().hasNext

  override def shutdown(): Unit = {
    initiateShutdown()
    // wake up the thread in case it is blocked inside poll
    networkClient.wakeup()
    awaitShutdown()
  }

  override def doWork(): Unit = {
    var now = time.milliseconds()

    generateRequests().foreach { request =>
      val completionHandler = request.handler
      unsentRequests.put(request.destination,
        networkClient.newClientRequest(
          request.destination.idString,
          request.request,
          now,
          true,
          requestTimeoutMs,
          completionHandler))
    }

    try {
      val timeout = sendRequests(now)
      networkClient.poll(timeout, now)
      now = time.milliseconds()
      checkDisconnects(now)
      failExpiredRequests(now)
      unsentRequests.clean()
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

  private def sendRequests(now: Long): Long = {
    var pollTimeout = Long.MaxValue
    for (node <- unsentRequests.nodes.asScala) {
      val requestIterator = unsentRequests.requestIterator(node)
      while (requestIterator.hasNext) {
        val request = requestIterator.next
        if (networkClient.ready(node, now)) {
          networkClient.send(request, now)
          requestIterator.remove()
        } else
          pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(node, now))
      }
    }
    pollTimeout
  }

  private def checkDisconnects(now: Long): Unit = {
    // any disconnects affecting requests that have already been transmitted will be handled
    // by NetworkClient, so we just need to check whether connections for any of the unsent
    // requests have been disconnected; if they have, then we complete the corresponding future
    // and set the disconnect flag in the ClientResponse
    val iterator = unsentRequests.iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      val (node, requests) = (entry.getKey, entry.getValue)
      if (!requests.isEmpty && networkClient.connectionFailed(node)) {
        iterator.remove()
        for (request <- requests.asScala) {
          val authenticationException = networkClient.authenticationException(node)
          if (authenticationException != null)
            error(s"Failed to send the following request due to authentication error: $request")
          completeWithDisconnect(request, now, authenticationException)
        }
      }
    }
  }

  private def failExpiredRequests(now: Long): Unit = {
    // clear all expired unsent requests
    val timedOutRequests = unsentRequests.removeAllTimedOut(now)
    for (request <- timedOutRequests.asScala) {
      debug(s"Failed to send the following request after ${request.requestTimeoutMs} ms: $request")
      completeWithDisconnect(request, now, null)
    }
  }

  def completeWithDisconnect(request: ClientRequest,
                             now: Long,
                             authenticationException: AuthenticationException): Unit = {
    val handler = request.callback
    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
      handler, request.destination, now /* createdTimeMs */ , now /* receivedTimeMs */ , true /* disconnected */ ,
      null /* versionMismatch */ , authenticationException, null))
  }

  def wakeup(): Unit = networkClient.wakeup()
}

case class RequestAndCompletionHandler(destination: Node,
                                       request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       handler: RequestCompletionHandler)

private class UnsentRequests {
  private val unsent = new HashMap[Node, ArrayDeque[ClientRequest]]

  def put(node: Node, request: ClientRequest): Unit = {
    var requests = unsent.get(node)
    if (requests == null) {
      requests = new ArrayDeque[ClientRequest]
      unsent.put(node, requests)
    }
    requests.add(request)
  }

  def removeAllTimedOut(now: Long): Collection[ClientRequest] = {
    val expiredRequests = new ArrayList[ClientRequest]
    for (requests <- unsent.values.asScala) {
      val requestIterator = requests.iterator
      var foundExpiredRequest = false
      while (requestIterator.hasNext && !foundExpiredRequest) {
        val request = requestIterator.next
        val elapsedMs = Math.max(0, now - request.createdTimeMs)
        if (elapsedMs > request.requestTimeoutMs) {
          expiredRequests.add(request)
          requestIterator.remove()
          foundExpiredRequest = true
        }
      }
    }
    expiredRequests
  }

  def clean(): Unit = {
    val iterator = unsent.values.iterator
    while (iterator.hasNext) {
      val requests = iterator.next
      if (requests.isEmpty)
        iterator.remove()
    }
  }

  def iterator(): Iterator[Entry[Node, ArrayDeque[ClientRequest]]] = {
    unsent.entrySet().iterator()
  }

  def requestIterator(node: Node): Iterator[ClientRequest] = {
    val requests = unsent.get(node)
    if (requests == null)
      Collections.emptyIterator[ClientRequest]
    else
      requests.iterator
  }

  def nodes = unsent.keySet
}
