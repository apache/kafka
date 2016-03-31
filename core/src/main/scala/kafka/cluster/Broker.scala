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

import java.nio.ByteBuffer

import kafka.api.ApiUtils._
import kafka.common.{BrokerEndPointNotAvailableException, BrokerNotAvailableException, KafkaException}
import kafka.utils.Json
import org.apache.kafka.common.Node
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
              Map(SecurityProtocol.PLAINTEXT -> new EndPoint(host, port, SecurityProtocol.PLAINTEXT))
            }
            else {
              val listeners = brokerInfo("endpoints").asInstanceOf[List[String]]
              listeners.map { listener =>
                val ep = EndPoint.createEndPoint(listener)
                (ep.protocolType, ep)
              }.toMap
            }
          val rack = brokerInfo.get("rack").filter(_ != null).map(_.asInstanceOf[String])
          new Broker(id, endpoints, rack)
        case None =>
          throw new BrokerNotAvailableException(s"Broker id $id does not exist")
      }
    } catch {
      case t: Throwable =>
        throw new KafkaException(s"Failed to parse the broker info from zookeeper: $brokerInfoString", t)
    }
  }
}

case class Broker(id: Int, endPoints: collection.Map[SecurityProtocol, EndPoint], rack: Option[String]) {

  override def toString: String =
    s"$id : ${endPoints.values.mkString("(",",",")")} : ${rack.orNull}"

  def this(id: Int, endPoints: Map[SecurityProtocol, EndPoint]) = {
    this(id, endPoints, None)
  }

  def this(id: Int, host: String, port: Int, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    this(id, Map(protocol -> EndPoint(host, port, protocol)), None)
  }

  def this(bep: BrokerEndPoint, protocol: SecurityProtocol) = {
    this(bep.id, bep.host, bep.port, protocol)
  }

  def getNode(protocolType: SecurityProtocol): Node = {
    val endpoint = endPoints.getOrElse(protocolType,
      throw new BrokerEndPointNotAvailableException(s"End point with security protocol $protocolType not found for broker $id"))
    new Node(id, endpoint.host, endpoint.port, rack.orNull)
  }

  def getBrokerEndPoint(protocolType: SecurityProtocol): BrokerEndPoint = {
    val endpoint = endPoints.getOrElse(protocolType,
      throw new BrokerEndPointNotAvailableException(s"End point with security protocol $protocolType not found for broker $id"))
    new BrokerEndPoint(id, endpoint.host, endpoint.port)
  }

}
