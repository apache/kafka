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
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.MultiFetchResponse
import kafka.api.FetchRequest

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int) {
  val underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize)

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  def fetch(request: FetchRequest): ByteBufferMessageSet = {
    import kafka.javaapi.Implicits._
    underlying.fetch(request)
  }

  /**
   *  Combine multiple fetch requests in one call.
   *
   *  @param fetches  a sequence of fetch requests.
   *  @return a sequence of fetch responses
   */
  def multifetch(fetches: java.util.List[FetchRequest]): MultiFetchResponse = {
    import scala.collection.JavaConversions._
    import kafka.javaapi.Implicits._
    underlying.multifetch(asBuffer(fetches): _*)
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  The result is a list of offsets, in descending order.
   *
   *  @param time: time in millisecs (-1, from the latest offset available, -2 from the smallest offset available)
   *  @return an array of offsets
   */
  def getOffsetsBefore(topic: String, partition: Int, time: Long, maxNumOffsets: Int): Array[Long] =
    underlying.getOffsetsBefore(topic, partition, time, maxNumOffsets)

  def close() {
    underlying.close
  }
}
