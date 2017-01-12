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

package kafka.cluster

import kafka.common.{BrokerEndPointNotAvailableException, BrokerNotAvailableException, KafkaException}
import kafka.utils.Json
import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol

/**
 * A Kafka broker.
 * A broker has an id and a collection of end-points.
 * Each end-point is (host, port, protocolType).
 */
object Broker {

  /**
    * Create a broker object from id and JSON string.
    *
    * @param id
    * @param brokerInfoString
    *
    * Version 1 JSON schema for a broker is:
    * {
    *   "version":1,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666"
    * }
    *
    * Version 2 JSON schema for a broker is:
    * {
    *   "version":2,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://host1:9092", "SSL://host1:9093"]
    * }
    *
    * Version 3 (current) JSON schema for a broker is:
    * {
    *   "version":3,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://host1:9092", "SSL://host1:9093"],
    *   "rack":"dc1"
    * }
    */
  def createBroker(id: Int, brokerInfoString: String): Broker = {
    if (brokerInfoString == null)
      throw new BrokerNotAvailableException(s"Broker id $id does not exist")
    try {
      Json.parseFull(brokerInfoString) match {
        case Some(m) =>
          val brokerInfo = m.asInstanceOf[Map[String, Any]]
          val version = brokerInfo("version").asInstanceOf[Int]
          val endpoints =
            if (version < 1)
              throw new KafkaException(s"Unsupported version of broker registration: $brokerInfoString")
            else if (version == 1) {
              val host = brokerInfo("host").asInstanceOf[String]
              val port = brokerInfo("port").asInstanceOf[Int]
              val securityProtocol = SecurityProtocol.PLAINTEXT
              val endPoint = new EndPoint(host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
              Seq(endPoint)
            }
            else {
              val securityProtocolMap = brokerInfo.get("listener_security_protocol_map").map(
                _.asInstanceOf[Map[String, String]]).map(_.map { case (listenerName, securityProtocol) =>
                new ListenerName(listenerName) -> SecurityProtocol.forName(securityProtocol)
              })
              val listeners = brokerInfo("endpoints").asInstanceOf[List[String]]
              listeners.map(EndPoint.createEndPoint(_, securityProtocolMap))
            }
          val rack = brokerInfo.get("rack").filter(_ != null).map(_.asInstanceOf[String])

          Broker(id, endpoints, rack)
        case None =>
          throw new BrokerNotAvailableException(s"Broker id $id does not exist")
      }
    } catch {
      case t: Throwable =>
        throw new KafkaException(s"Failed to parse the broker info from zookeeper: $brokerInfoString", t)
    }
  }
}

case class Broker(id: Int, endPoints: Seq[EndPoint], rack: Option[String]) {

  private val endPointsMap = endPoints.map { endPoint =>
    endPoint.listenerName -> endPoint
  }.toMap

  if (endPointsMap.size != endPoints.size)
    throw new IllegalArgumentException(s"There is more than one end point with the same listener name: ${endPoints.mkString(",")}")

  override def toString: String =
    s"$id : ${endPointsMap.values.mkString("(",",",")")} : ${rack.orNull}"

  def this(id: Int, host: String, port: Int, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(id, Seq(EndPoint(host, port, listenerName, protocol)), None)
  }

  def this(bep: BrokerEndPoint, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(bep.id, bep.host, bep.port, listenerName, protocol)
  }

  def getNode(listenerName: ListenerName): Node = {
    val endpoint = endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with protocol label $listenerName not found for broker $id"))
    new Node(id, endpoint.host, endpoint.port, rack.orNull)
  }

  def getBrokerEndPoint(listenerName: ListenerName): BrokerEndPoint = {
    val endpoint = endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with security protocol $listenerName not found for broker $id"))
    new BrokerEndPoint(id, endpoint.host, endpoint.port)
  }

}
