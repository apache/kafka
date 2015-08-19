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

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.utils.{Time => JTime}

object SelectorUtils {

  /**
   * Invokes `selector.poll` until `find` returns `Some` or the timeout expires. It returns the result of `find` if
   * the call completes normally or `None` if the timeout expires.
   */
  def pollUntilFound[T](selector: Selector, timeout: Long)(find: => Option[T])(implicit time: JTime): Option[T] = {
    var result: Option[T] = None
    // for consistency with `Selector.poll`
    if (timeout < 0) {
      do {
        selector.poll(timeout)
        result = find
      } while (result.isEmpty)
    }
    else {
      var timeRemaining = timeout
      val endTime = TimeUnit.NANOSECONDS.toMillis(time.nanoseconds) + timeout
      do {
        selector.poll(timeRemaining)
        result = find
        timeRemaining = endTime - TimeUnit.NANOSECONDS.toMillis(time.nanoseconds)
      } while (result.isEmpty && timeRemaining > 0)
    }
    result
  }

}
