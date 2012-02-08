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

import java.util.Properties
import kafka.cluster.Broker
import kafka.utils.Logging
import java.util.concurrent.ConcurrentHashMap

class ProducerPool(private val config: ProducerConfig) extends Logging {
  private val syncProducers = new ConcurrentHashMap[Int, SyncProducer]

  def addProducer(broker: Broker) {
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

  def getProducer(brokerId: Int) : SyncProducer = {
    syncProducers.get(brokerId)
  }

  /**
   * Closes all the producers in the pool
   */
  def close() = {
    info("Closing all sync producers")
    val iter = syncProducers.values.iterator
    while(iter.hasNext)
      iter.next.close
  }
}
