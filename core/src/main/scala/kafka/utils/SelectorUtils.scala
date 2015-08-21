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
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

import org.apache.kafka.common.network.{NetworkReceive, Selector}
import org.apache.kafka.common.utils.{Time => JTime}

object SelectorUtils {

  /**
   * Invokes `selector.poll` until `id` is connected, the timeout expires or a disconnection for `id` happens.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. If `id` is disconnected,
   * an `IOException` is thrown (an edge case, but it could happen).
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `Selector`, use it with
   * care.
   */
  def pollUntilConnected(selector: Selector, id: String, timeout: Long)(implicit time: JTime): Boolean =
    pollUntil(selector, id, timeout)(selector.connected.contains(id))

  /**
   * Invokes `selector.poll` until a receive has been completed for `id`, the timeout expires or a disconnection for
   * `id` happens.
   *
   * It returns the received value in a `Some` if the call completes normally or `None` if the timeout expires. If
   * `id` is disconnected, an `IOException` is thrown instead.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `Selector`, use it with
   * care.
   */
  def pollUntilReceiveCompleted(selector: Selector, id: String, timeout: Long)(implicit time: JTime): Option[NetworkReceive] =
    pollUntilFound(selector, id, timeout)(selector.completedReceives.asScala.find(_.source() == id))

  /**
   * Invokes `selector.poll` until `predicate` returns `true`, the timeout expires or a disconnection for `id` happens.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. If `id` is disconnected,
   * an `IOException` is thrown instead.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `Selector`, use it with
   * care.
   */
  private def pollUntil(selector: Selector, id: String, timeout: Long)(predicate: => Boolean)(implicit time: JTime): Boolean =
    pollUntilFound(selector, id, timeout) {
      if (predicate) Some(true)
      else None
    }.fold(false)(_ => true)

  /**
   * Invokes `selector.poll` until `find` returns `Some`, the timeout expires or the connection with `id` is disconnected.
   *
   * It returns the result of `find` if the call completes normally or `None` if the timeout expires. If `id` `is
   * disconnected, an `IOException` is thrown instead.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `Selector`, use it with
   * care.
   */
  private def pollUntilFound[T](selector: Selector, id: String, timeout: Long)(find: => Option[T])(implicit time: JTime): Option[T] = {
    var result: Option[T] = None
    var isDisconnected = false
    // for consistency with `Selector.poll`
    if (timeout < 0) {
      do {
        selector.poll(timeout)
        result = find
        isDisconnected = selector.disconnected.asScala.contains(id)
      } while (result.isEmpty && !isDisconnected)
    }
    else {
      var timeRemaining = timeout
      val endTime = TimeUnit.NANOSECONDS.toMillis(time.nanoseconds) + timeout
      do {
        selector.poll(timeRemaining)
        result = find
        isDisconnected = selector.disconnected.asScala.contains(id)
        timeRemaining = endTime - TimeUnit.NANOSECONDS.toMillis(time.nanoseconds)
      } while (result.isEmpty && !isDisconnected && timeRemaining > 0)
    }
    if (result.isEmpty && isDisconnected)
      throw new IOException(s"$id has been disconnected")
    else result
  }

}
