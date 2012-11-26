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

package kafka.javaapi

class FetchResponse(private val underlying: kafka.api.FetchResponse) {

  def messageSet(topic: String, partition: Int): kafka.javaapi.message.ByteBufferMessageSet = {
    import Implicits._
    underlying.messageSet(topic, partition)
  }

  def highWatermark(topic: String, partition: Int) = underlying.highWatermark(topic, partition)

  def hasError = underlying.hasError

  def errorCode(topic: String, partition: Int) = underlying.errorCode(topic, partition)

  override def equals(other: Any) = canEqual(other) && {
    val otherFetchResponse = other.asInstanceOf[kafka.javaapi.FetchResponse]
    this.underlying.equals(otherFetchResponse.underlying)
  }

  def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.FetchResponse]

  override def hashCode = underlying.hashCode
}
