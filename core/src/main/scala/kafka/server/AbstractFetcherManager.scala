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

import scala.collection.mutable
import kafka.utils.Logging
import kafka.cluster.Broker

abstract class AbstractFetcherManager(protected val name: String, numFetchers: Int = 1) extends Logging {
    // map of (source brokerid, fetcher Id per source broker) => fetcher
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, AbstractFetcherThread]
  private val mapLock = new Object
  this.logIdent = "[" + name + "] "

  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    (topic.hashCode() + 31 * partitionId) % numFetchers
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread

  def addFetcher(topic: String, partitionId: Int, initialOffset: Long, sourceBroker: Broker) {
    mapLock synchronized {
      var fetcherThread: AbstractFetcherThread = null
      val key = new BrokerAndFetcherId(sourceBroker, getFetcherId(topic, partitionId))
      fetcherThreadMap.get(key) match {
        case Some(f) => fetcherThread = f
        case None =>
          fetcherThread = createFetcherThread(key.fetcherId, sourceBroker)
          fetcherThreadMap.put(key, fetcherThread)
          fetcherThread.start
      }
      fetcherThread.addPartition(topic, partitionId, initialOffset)
      info("adding fetcher on topic %s, partion %d, initOffset %d to broker %d with fetcherId %d"
          .format(topic, partitionId, initialOffset, sourceBroker.id, key.fetcherId))
    }
  }

  def removeFetcher(topic: String, partitionId: Int) {
    info("removing fetcher on topic %s, partition %d".format(topic, partitionId))
    mapLock synchronized {
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartition(topic, partitionId)
      }
    }
  }

  def shutdownIdleFetcherThreads() {
    mapLock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers() {
    mapLock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}

case class BrokerAndFetcherId(broker: Broker, fetcherId: Int)