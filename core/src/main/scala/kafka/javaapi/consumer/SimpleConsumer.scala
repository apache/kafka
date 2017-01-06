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

package kafka.javaapi.consumer

import kafka.utils.threadsafe
import kafka.javaapi.FetchResponse
import kafka.javaapi.OffsetRequest

/**
 * A consumer of kafka messages
 */
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0")
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int,
                     val clientId: String) {

  private val underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize, clientId)

  /**
   *  Fetch a set of messages from a topic. This version of the fetch method
   *  takes the Scala version of a fetch request (i.e.,
   *  [[kafka.api.FetchRequest]] and is intended for use with the
   *  [[kafka.api.FetchRequestBuilder]].
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  def fetch(request: kafka.api.FetchRequest): FetchResponse = {
    import kafka.javaapi.Implicits._
    underlying.fetch(request)
  }

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  def fetch(request: kafka.javaapi.FetchRequest): FetchResponse = {
    fetch(request.underlying)
  }

  /**
   *  Fetch metadata for a sequence of topics.
   *
   *  @param request specifies the versionId, clientId, sequence of topics.
   *  @return metadata for each topic in the request.
   */
  def send(request: kafka.javaapi.TopicMetadataRequest): kafka.javaapi.TopicMetadataResponse = {
    import kafka.javaapi.Implicits._
    underlying.send(request.underlying)
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *
   *  @param request a [[kafka.javaapi.OffsetRequest]] object.
   *  @return a [[kafka.javaapi.OffsetResponse]] object.
   */
  def getOffsetsBefore(request: OffsetRequest): kafka.javaapi.OffsetResponse = {
    import kafka.javaapi.Implicits._
    underlying.getOffsetsBefore(request.underlying)
  }

  /**
   * Commit offsets for a topic to Zookeeper
   * @param request a [[kafka.javaapi.OffsetCommitRequest]] object.
   * @return a [[kafka.javaapi.OffsetCommitResponse]] object.
   */
  def commitOffsets(request: kafka.javaapi.OffsetCommitRequest): kafka.javaapi.OffsetCommitResponse = {
    import kafka.javaapi.Implicits._
    underlying.commitOffsets(request.underlying)
  }

  /**
   * Fetch offsets for a topic from Zookeeper
   * @param request a [[kafka.javaapi.OffsetFetchRequest]] object.
   * @return a [[kafka.javaapi.OffsetFetchResponse]] object.
   */
  def fetchOffsets(request: kafka.javaapi.OffsetFetchRequest): kafka.javaapi.OffsetFetchResponse = {
    import kafka.javaapi.Implicits._
    underlying.fetchOffsets(request.underlying)
  }

  def close() {
    underlying.close
  }
}
