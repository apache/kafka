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

import java.util.concurrent.CountDownLatch
import kafka.cluster.Broker
import kafka.consumer.SimpleConsumer
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils.Logging
import kafka.common.ErrorMapping
import kafka.api.{PartitionData, FetchRequestBuilder}
import scala.collection.mutable
import kafka.message.ByteBufferMessageSet

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(val name: String, sourceBroker: Broker, socketTimeout: Int, socketBufferSize: Int,
                                     fetchSize: Int, fetcherBrokerId: Int = -1, maxWait: Int = 0, minBytes: Int = 1)
  extends Thread(name) with Logging {
  private val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)
  private val fetchMap = new mutable.HashMap[Tuple2[String,Int], Long] // a (topic, partitionId) -> offset map
  private val fetchMapLock = new Object
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize)
  this.logIdent = name + " "
  info("starting")
  // callbacks to be defined in subclass

  // process fetched data and return the new fetch offset
  def processPartitionData(topic: String, fetchOffset: Long, partitionData: PartitionData)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topic: String, partitionId: Int): Long

  // any logic for partitions whose leader has changed
  def handlePartitionsWithNewLeader(partitions: List[Tuple2[String, Int]]): Unit

  override def run() {
    try {
      while(isRunning.get()) {
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
        val response = simpleConsumer.fetch(fetchRequest)
        trace("issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))

        var partitionsWithNewLeader : List[Tuple2[String, Int]] = Nil
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
                  case ErrorMapping.NotLeaderForPartitionCode =>
                    partitionsWithNewLeader ::= key
                  case _ =>
                    error("error for %s %d to broker %d".format(topic, partitionId, sourceBroker.host),
                          ErrorMapping.exceptionFor(partitionData.error))
                }
              }
            }
          }
        }
        if (partitionsWithNewLeader.size > 0) {
          debug("changing leaders for %s".format(partitionsWithNewLeader))
          handlePartitionsWithNewLeader(partitionsWithNewLeader)
        }
      }
    } catch {
      case e: InterruptedException => info("replica fetcher runnable interrupted. Shutting down")
      case e1 => error("error in replica fetcher runnable", e1)
    }
    shutdownComplete()
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
  
  private def shutdownComplete() = {
    simpleConsumer.close()
    shutdownLatch.countDown
  }

  def shutdown() {
    isRunning.set(false)
    interrupt()
    shutdownLatch.await()
    info("shutdown completed")
  }
}