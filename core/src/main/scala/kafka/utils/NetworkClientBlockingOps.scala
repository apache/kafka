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

package kafka.utils

import java.io.IOException
import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient}
import org.apache.kafka.common.Node

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.kafka.common.utils.{Time => JTime}

object NetworkClientBlockingOps {
  implicit def networkClientBlockingOps(client: NetworkClient): NetworkClientBlockingOps =
    new NetworkClientBlockingOps(client)
}

/**
 * Provides extension methods for `NetworkClient` that are useful for implementing blocking behaviour. Use with care.
 *
 * Example usage:
 *
 * {{{
 * val networkClient: NetworkClient = ...
 * import NetworkClientBlockingOps._
 * networkClient.blockingReady(...)
 * }}}
 */
class NetworkClientBlockingOps(val client: NetworkClient) extends AnyVal {

  /**
    * Checks whether the node is currently connected, first calling `client.poll` to ensure that any pending
    * disconnects have been processed.
    *
    * This method can be used to check the status of a connection prior to calling `blockingReady` to be able
    * to tell whether the latter completed a new connection.
    */
  def isReady(node: Node)(implicit time: JTime): Boolean = {
    val currentTime = time.milliseconds()
    client.poll(0, currentTime)
    client.isReady(node, currentTime)
  }

  /**
   * Invokes `client.poll` to discard pending disconnects, followed by `client.ready` and 0 or more `client.poll`
   * invocations until the connection to `node` is ready, the timeout expires or the connection fails.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. If the connection fails,
   * an `IOException` is thrown instead. Note that if the `NetworkClient` has been configured with a positive
   * connection timeout, it is possible for this method to raise an `IOException` for a previous connection which
   * has recently disconnected.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  def blockingReady(node: Node, timeout: Long)(implicit time: JTime): Boolean = {
    require(timeout >=0, "timeout should be >= 0")

    val startTime = time.milliseconds()
    val expiryTime = startTime + timeout

    @tailrec
    def awaitReady(iterationStartTime: Long): Boolean = {
      if (client.isReady(node, iterationStartTime))
        true
      else if (client.connectionFailed(node))
        throw new IOException(s"Connection to $node failed")
      else {
        val pollTimeout = expiryTime - iterationStartTime
        client.poll(pollTimeout, iterationStartTime)
        val afterPollTime = time.milliseconds()
        if (afterPollTime < expiryTime) awaitReady(afterPollTime)
        else false
      }
    }

    isReady(node) || client.ready(node, startTime) || awaitReady(startTime)
  }

  /**
   * Invokes `client.send` followed by 1 or more `client.poll` invocations until a response is received or a
   * disconnection happens (which can happen for a number of reasons including a request timeout).
   *
   * In case of a disconnection, an `IOException` is thrown.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  def blockingSendAndReceive(request: ClientRequest)(implicit time: JTime): ClientResponse = {
    client.send(request, time.milliseconds())

    pollContinuously { responses =>
      val response = responses.find { response =>
        response.request.request.header.correlationId == request.request.header.correlationId
      }
      response.foreach { r =>
        if (r.wasDisconnected) {
          val destination = request.request.destination
          throw new IOException(s"Connection to $destination was disconnected before the response was read")
        }
      }
      response
    }

  }

  /**
    * Invokes `client.poll` until `collect` returns `Some`. The value inside `Some` is returned.
    *
    * Exceptions thrown via `collect` are not handled and will bubble up.
    *
    * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
    * care.
    */
  private def pollContinuously[T](collect: Seq[ClientResponse] => Option[T])(implicit time: JTime): T = {

    @tailrec
    def recursivePoll: T = {
      // rely on request timeout to ensure we don't block forever
      val responses = client.poll(Long.MaxValue, time.milliseconds()).asScala
      collect(responses) match {
        case Some(result) => result
        case None => recursivePoll
      }
    }

    recursivePoll
  }

}
