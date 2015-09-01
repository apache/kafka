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
import org.apache.kafka.clients.{ConnectionState, ClientRequest, ClientResponse, NetworkClient}
import org.apache.kafka.common.Node

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.kafka.common.utils.{Time => JTime}

object NetworkClientBlockingOps {
  implicit def networkClientBlockingOps(client: NetworkClient): NetworkClientBlockingOps =
    new NetworkClientBlockingOps(client)
}

class NetworkClientBlockingOps(val client: NetworkClient) extends AnyVal {

  def blockingReady(node: Node, timeout: Long)(implicit time: JTime): Boolean = {
    client.ready(node, time.milliseconds()) || pollUntil(timeout) { (_, now) =>
      if (client.isReady(node, now))
        true
      else if (client.connectionFailed(node))
        throw new IOException(s"Connection to $node failed")
      else false
    }
  }

  def blockingSendAndReceive(request: ClientRequest, timeout: Long)(implicit time: JTime): Option[ClientResponse] = {
    client.send(request)

    pollUntilFound(timeout) { case (responses, _) =>
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

  private def pollUntil(timeout: Long)(predicate: (Seq[ClientResponse], Long) => Boolean)(implicit time: JTime): Boolean = {
    pollUntilFound(timeout) { (responses, now) =>
      if (predicate(responses, now)) Some(true)
      else None
    }.fold(false)(_ => true)
  }

  private def pollUntilFound[T](timeout: Long)(collect: (Seq[ClientResponse], Long) => Option[T])(implicit time: JTime): Option[T] = {

    val methodStartTime = time.milliseconds()
    val timeoutExpiryTime = methodStartTime + timeout

    @tailrec
    def recurse(iterationStartTime: Long): Option[T] = {
      val pollTimeout = if (timeout < 0) timeout else timeoutExpiryTime - iterationStartTime
      val responses = client.poll(pollTimeout, iterationStartTime).asScala
      val result = collect(responses, iterationStartTime)
      if (result.isDefined) result
      else {
        val afterPollTime = time.milliseconds()
        if (timeout < 0 || afterPollTime < timeoutExpiryTime)
          recurse(afterPollTime)
        else None
      }
    }

    recurse(methodStartTime)
  }

}
