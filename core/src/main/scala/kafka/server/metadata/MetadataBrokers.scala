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

package kafka.server.metadata

import java.util
import java.util.Collections
import java.util.concurrent.ThreadLocalRandom
import kafka.cluster.{Broker, BrokerEndPoint}
import kafka.common.BrokerEndPointNotAvailableException
import org.apache.kafka.common.Node
import org.apache.kafka.common.metadata.RegisterBrokerRecord
import org.apache.kafka.common.network.ListenerName
import org.slf4j.Logger

import scala.jdk.CollectionConverters._

object MetadataBroker {
  def apply(record: RegisterBrokerRecord): MetadataBroker = {
    new MetadataBroker(record.brokerId, record.rack,
      record.endPoints().asScala.map { endPoint =>
        endPoint.name() ->
          new Node(record.brokerId, endPoint.host, endPoint.port, record.rack)
      }.toMap,
      fenced = true)
  }

  def apply(broker: Broker): MetadataBroker = {
    new MetadataBroker(broker.id, broker.rack.orNull,
      broker.endPoints.map { endpoint =>
        endpoint.listenerName.value -> new Node(broker.id, endpoint.host, endpoint.port, broker.rack.orNull)
      }.toMap,
      fenced = false)
  }
}

case class MetadataBroker(id: Int,
                          rack: String,
                          endpoints: collection.Map[String, Node],
                          fenced: Boolean) {
  def brokerEndPoint(listenerName: ListenerName): BrokerEndPoint = {
    endpoints.get(listenerName.value) match {
      case None => throw new BrokerEndPointNotAvailableException(
        s"End point with listener name ${listenerName.value} not found for broker $id")
      case Some(node) => new BrokerEndPoint(node.id, node.host, node.port)
    }
  }

  def node(listenerName: ListenerName): Node = {
    endpoints.getOrElse(listenerName.value, throw new BrokerEndPointNotAvailableException(
      s"End point with listener name ${listenerName.value} not found for broker $id"))
  }
}

class MetadataBrokersBuilder(log: Logger, prevBrokers: MetadataBrokers) {
  private var newBrokerMap = prevBrokers.cloneBrokerMap()

  def add(broker: MetadataBroker): Unit = {
    newBrokerMap.put(broker.id, broker)
  }

  def changeFencing(id: Int, fenced: Boolean): Unit = {
    val broker = newBrokerMap.get(id)
    if (broker == null) {
      throw new RuntimeException(s"Unknown broker id ${id}")
    }
    val newBroker = new MetadataBroker(broker.id, broker.rack, broker.endpoints, fenced)
    newBrokerMap.put(id, newBroker)
  }

  def remove(id: Int): Unit = {
    newBrokerMap.remove(id)
  }

  def get(brokerId: Int): Option[MetadataBroker] = Option(newBrokerMap.get(brokerId))

  def build(): MetadataBrokers = {
    val result = MetadataBrokers(log, newBrokerMap)
    newBrokerMap = Collections.unmodifiableMap(newBrokerMap)
    result
  }
}

object MetadataBrokers {
  def apply(log: Logger,
            brokerMap: util.Map[Integer, MetadataBroker]): MetadataBrokers = {
    var listenersIdenticalAcrossBrokers = true
    var prevListeners: collection.Set[String] = null
    val _aliveBrokers = new util.ArrayList[MetadataBroker](brokerMap.size())
    brokerMap.values().iterator().asScala.foreach { broker =>
      if (!broker.fenced) {
        if (prevListeners == null) {
          prevListeners = broker.endpoints.keySet
        } else if (!prevListeners.equals(broker.endpoints.keySet)) {
          listenersIdenticalAcrossBrokers = false
        }
        _aliveBrokers.add(broker)
      }
    }
    if (!listenersIdenticalAcrossBrokers) {
      log.error("Listeners are not identical across alive brokers. " +
        _aliveBrokers.asScala.map(
          broker => s"${broker.id}: ${broker.endpoints.keySet.mkString(", ")}"))
    }
    new MetadataBrokers(_aliveBrokers, brokerMap)
  }
}

case class MetadataBrokers(private val aliveBrokersList: util.List[MetadataBroker],
                           private val brokerMap: util.Map[Integer, MetadataBroker]) {
  def size(): Int = brokerMap.size()

  def iterator(): Iterator[MetadataBroker] = brokerMap.values().iterator().asScala

  def cloneBrokerMap(): util.Map[Integer, MetadataBroker] = {
    val result = new util.HashMap[Integer, MetadataBroker]
    result.putAll(brokerMap)
    result
  }

  def aliveBroker(id: Int): Option[MetadataBroker] = {
    get(id).filter(!_.fenced)
  }

  def randomAliveBrokerId(): Option[Int] = {
    if (aliveBrokersList.isEmpty) {
      None
    } else {
      Some(aliveBrokersList.get(ThreadLocalRandom.current().nextInt(aliveBrokersList.size())).id)
    }
  }

  def aliveBrokers(): collection.Seq[MetadataBroker] = aliveBrokersList.asScala

  def get(id: Int): Option[MetadataBroker] = Option(brokerMap.get(id))
}
