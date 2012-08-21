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

package kafka.server

import kafka.cluster.Broker
import kafka.consumer.SimpleConsumer
import kafka.common.ErrorMapping
import collection.mutable
import kafka.message.ByteBufferMessageSet
import kafka.api.{FetchResponse, PartitionData, FetchRequestBuilder}
import kafka.utils.ShutdownableThread


/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class  AbstractFetcherThread(name: String, sourceBroker: Broker, socketTimeout: Int, socketBufferSize: Int,
                                     fetchSize: Int, fetcherBrokerId: Int = -1, maxWait: Int = 0, minBytes: Int = 1)
  extends ShutdownableThread(name) {
  private val fetchMap = new mutable.HashMap[Tuple2[String,Int], Long] // a (topic, partitionId) -> offset map
  private val fetchMapLock = new Object
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize)
  // callbacks to be defined in subclass

  // process fetched data
  def processPartitionData(topic: String, fetchOffset: Long, partitionData: PartitionData)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topic: String, partitionId: Int): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[(String, Int)])

  override def shutdown(){
    super.shutdown()
    simpleConsumer.close()
  }

  override def doWork() {
    val builder = new FetchRequestBuilder().
            clientId(name).
            replicaId(fetcherBrokerId).
            maxWait(maxWait).
            minBytes(minBytes)

    fetchMapLock synchronized {
      for ( ((topic, partitionId), offset) <- fetchMap )
        builder.addFetch(topic, partitionId, offset.longValue, fetchSize)
    }

    val fetchRequest = builder.build()
    val partitionsWithError = new mutable.HashSet[(String, Int)]
    var response: FetchResponse = null
    try {
      trace("issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t =>
        debug("error in fetch %s".format(fetchRequest), t)
        if (isRunning.get) {
          fetchMapLock synchronized {
            partitionsWithError ++= fetchMap.keys
            fetchMap.clear()
          }
        }
    }

    if (response != null) {
      // process fetched data
      fetchMapLock synchronized {
        for ( topicData <- response.data ) {
          for ( partitionData <- topicData.partitionDataArray) {
            val topic = topicData.topic
            val partitionId = partitionData.partition
            val key = (topic, partitionId)
            val currentOffset = fetchMap.get(key)
            if (currentOffset.isDefined) {
              partitionData.error match {
                case ErrorMapping.NoError =>
                  processPartitionData(topic, currentOffset.get, partitionData)
                  val newOffset = currentOffset.get + partitionData.messages.asInstanceOf[ByteBufferMessageSet].validBytes
                  fetchMap.put(key, newOffset)
                case ErrorMapping.OffsetOutOfRangeCode =>
                  val newOffset = handleOffsetOutOfRange(topic, partitionId)
                  fetchMap.put(key, newOffset)
                  warn("current offset %d for topic %s partition %d out of range; reset offset to %d"
                               .format(currentOffset.get, topic, partitionId, newOffset))
                case _ =>
                  error("error for %s %d to broker %d".format(topic, partitionId, sourceBroker.host),
                        ErrorMapping.exceptionFor(partitionData.error))
                  partitionsWithError += key
                  fetchMap.remove(key)
              }
            }
          }
        }
      }
    }
    if (partitionsWithError.size > 0) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartition(topic: String, partitionId: Int, initialOffset: Long) {
    fetchMapLock synchronized {
      fetchMap.put((topic, partitionId), initialOffset)
    }
  }

  def removePartition(topic: String, partitionId: Int) {
    fetchMapLock synchronized {
      fetchMap.remove((topic, partitionId))
    }
  }

  def hasPartition(topic: String, partitionId: Int): Boolean = {
    fetchMapLock synchronized {
      fetchMap.get((topic, partitionId)).isDefined
    }
  }

  def partitionCount() = {
    fetchMapLock synchronized {
      fetchMap.size
    }
  }
}