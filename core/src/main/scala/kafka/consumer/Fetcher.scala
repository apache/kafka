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

package kafka.consumer

import scala.collection._
import kafka.cluster._
import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.BlockingQueue
import kafka.utils._

/**
 * The fetcher is a background thread that fetches data from a set of servers
 */
private [consumer] class Fetcher(val config: ConsumerConfig, val zkClient : ZkClient) extends Logging {
  private val EMPTY_FETCHER_THREADS = new Array[FetcherRunnable](0)
  @volatile
  private var fetcherThreads : Array[FetcherRunnable] = EMPTY_FETCHER_THREADS

  /**
   *  shutdown all fetcher threads
   */
  def stopConnectionsToAllBrokers = {
    // shutdown the old fetcher threads, if any
    for (fetcherThread <- fetcherThreads)
      fetcherThread.shutdown
    fetcherThreads = EMPTY_FETCHER_THREADS
  }

  def clearFetcherQueues[T](topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster,
                            queuesTobeCleared: Iterable[BlockingQueue[FetchedDataChunk]],
                            kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]]) {

    // Clear all but the currently iterated upon chunk in the consumer thread's queue
    queuesTobeCleared.foreach(_.clear)
    info("Cleared all relevant queues for this fetcher")

    // Also clear the currently iterated upon chunk in the consumer threads
    if(kafkaMessageStreams != null)
       kafkaMessageStreams.foreach(_._2.foreach(s => s.clear()))

    info("Cleared the data chunks in all the consumer message iterators")

  }

  def startConnections[T](topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster,
                            kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]]) {
    if (topicInfos == null)
      return

    // re-arrange by broker id
    val m = new mutable.HashMap[Int, List[PartitionTopicInfo]]
    for(info <- topicInfos) {
      m.get(info.brokerId) match {
        case None => m.put(info.brokerId, List(info))
        case Some(lst) => m.put(info.brokerId, info :: lst)
      }
    }

    // open a new fetcher thread for each broker
    val ids = Set() ++ topicInfos.map(_.brokerId)
    val brokers = ids.map(cluster.getBroker(_))
    fetcherThreads = new Array[FetcherRunnable](brokers.size)
    var i = 0
    for(broker <- brokers) {
      val fetcherThread = new FetcherRunnable("FetchRunnable-" + i, zkClient, config, broker, m.get(broker.id).get)
      fetcherThreads(i) = fetcherThread
      fetcherThread.start
      i +=1
    }
  }    
}


