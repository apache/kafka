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
import collection.mutable.HashMap
import java.lang.Object
import kafka.common.UnavailableProducerException
import kafka.utils.{Utils, Logging}
import kafka.api.TopicMetadata


object ProducerPool{
  def createSyncProducer(config: ProducerConfig): SyncProducer = {
    val brokerList = config.brokerList
    val brokers = Utils.getAllBrokersFromBrokerList(brokerList)
    createSyncProducer(config, brokers.head)
  }

  def createSyncProducer(config: ProducerConfig, broker: Broker): SyncProducer = {
    val props = new Properties()
    props.put("host", broker.host)
    props.put("port", broker.port.toString)
    props.putAll(config.props.props)
    new SyncProducer(new SyncProducerConfig(props))
  }
}

class ProducerPool(val config: ProducerConfig) extends Logging {
  private val syncProducers = new HashMap[Int, SyncProducer]
  private val lock = new Object()

  def updateProducer(topicMetaDatas: Seq[TopicMetadata]) {
    val newBrokers = new collection.mutable.HashSet[Broker]
    topicMetaDatas.foreach(tmd => {
      tmd.partitionsMetadata.foreach(pmd => {
        if(pmd.leader.isDefined)
          newBrokers+=(pmd.leader.get)
      })
    })
    lock synchronized {
      newBrokers.foreach(b => {
        if(syncProducers.contains(b.id)){
          syncProducers(b.id).close()
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
        } else
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
      })
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

  /**
   * Closes all the producers in the pool
   */
  def close() = {
    lock.synchronized {
      info("Closing all sync producers")
      val iter = syncProducers.values.iterator
      while(iter.hasNext)
        iter.next.close
    }
  }
}
