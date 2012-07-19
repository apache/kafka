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

package kafka.producer

import kafka.cluster.Broker
import java.util.Properties
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, Logging}
import collection.mutable.HashMap
import java.lang.Object
import kafka.common.{UnavailableProducerException, NoBrokersForPartitionException}

class ProducerPool(val config: ProducerConfig, val zkClient: ZkClient) extends Logging {
  private val syncProducers = new HashMap[Int, SyncProducer]
  private val lock = new Object()

  private def addProducer(broker: Broker) {
    val props = new Properties()
    props.put("host", broker.host)
    props.put("port", broker.port.toString)
    props.putAll(config.props)
    val producer = new SyncProducer(new SyncProducerConfig(props))
    info("Creating sync producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port)
    syncProducers.put(broker.id, producer)
  }

  /**
   *  For testing purpose
   */
  def addProducer(brokerId: Int, syncProducer: SyncProducer) {
    syncProducers.put(brokerId, syncProducer)
  }

  def addProducers(config: ProducerConfig) {
    lock.synchronized {
      debug("Connecting to %s for creating sync producers for all brokers in the cluster".format(config.zkConnect))
      val brokers = ZkUtils.getAllBrokersInCluster(zkClient)
      brokers.foreach(broker => addProducer(broker))
    }
  }

  def getProducer(brokerId: Int) : SyncProducer = {
    lock.synchronized {
      val producer = syncProducers.get(brokerId)
      producer match {
        case Some(p) => p
        case None => throw new UnavailableProducerException("Sync producer for broker id %d does not exist".format(brokerId))
      }
    }
  }

  def getAnyProducer: SyncProducer = {
    lock.synchronized {
      if(syncProducers.size == 0) {
        // refresh the list of brokers from zookeeper
        info("No sync producers available. Refreshing the available broker list from ZK and creating sync producers")
        addProducers(config)
        if(syncProducers.size == 0)
          throw new NoBrokersForPartitionException("No brokers available")
      }
      syncProducers.head._2
    }
  }

  def getZkClient: ZkClient = zkClient

  /**
   * Closes all the producers in the pool
   */
  def close() = {
    lock.synchronized {
      info("Closing all sync producers")
      val iter = syncProducers.values.iterator
      while(iter.hasNext)
        iter.next.close
      zkClient.close()
    }
  }
}
