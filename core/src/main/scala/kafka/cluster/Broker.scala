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

import kafka.common.{BrokerEndPointNotAvailableException, BrokerNotAvailableException, KafkaException}
import kafka.utils.Json
import org.apache.kafka.common.protocol.SecurityProtocol

/**
 * A Kafka broker.
 * A broker has an id and a collection of end-points.
 * Each end-point is (host, port, protocolType).
 */
object Broker {

  /**
   * Create a broker object from id and JSON string.
   * @param id
   * @param brokerInfoString
   *
   * Version 1 JSON schema for a broker is:
   * {"version":1,
   *  "host":"localhost",
   *  "port":9092
   *  "jmx_port":9999,
   *  "timestamp":"2233345666" }
   *
   * The current JSON schema for a broker is:
   * {"version":2,
   *  "host","localhost",
   *  "port",9092
   *  "jmx_port":9999,
   *  "timestamp":"2233345666",
   *  "endpoints": ["PLAINTEXT://host1:9092",
   *                "SSL://host1:9093"]
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


          new Broker(id, endpoints)
        case None =>
          throw new BrokerNotAvailableException(s"Broker id $id does not exist")
      }
    } catch {
      case t: Throwable =>
        throw new KafkaException(s"Failed to parse the broker info from zookeeper: $brokerInfoString", t)
    }
  }

  /**
   *
   * @param buffer Containing serialized broker.
   *               Current serialization is:
   *               id (int), number of endpoints (int), serialized endpoints
   * @return broker object
   */
  def readFrom(buffer: ByteBuffer): Broker = {
    val id = buffer.getInt
    val numEndpoints = buffer.getInt

    val endpoints = List.range(0, numEndpoints).map(i => EndPoint.readFrom(buffer))
            .map(ep => ep.protocolType -> ep).toMap
    new Broker(id, endpoints)
  }
}

case class Broker(id: Int, endPoints: Map[SecurityProtocol, EndPoint]) {

  override def toString: String = id + " : " + endPoints.values.mkString("(",",",")")

  def this(id: Int, host: String, port: Int, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    this(id, Map(protocol -> EndPoint(host, port, protocol)))
  }

  def this(bep: BrokerEndPoint, protocol: SecurityProtocol) = {
    this(bep.id, bep.host, bep.port, protocol)
  }


  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(id)
    buffer.putInt(endPoints.size)
    for(endpoint <- endPoints.values) {
      endpoint.writeTo(buffer)
    }
  }

  def sizeInBytes: Int =
    4 + /* broker id*/
    4 + /* number of endPoints */
    endPoints.values.map(_.sizeInBytes).sum /* end points */

  def supportsChannel(protocolType: SecurityProtocol): Unit = {
    endPoints.contains(protocolType)
  }

  def getBrokerEndPoint(protocolType: SecurityProtocol): BrokerEndPoint = {
    val endpoint = endPoints.get(protocolType)
    endpoint match {
      case Some(endpoint) => new BrokerEndPoint(id, endpoint.host, endpoint.port)
      case None =>
        throw new BrokerEndPointNotAvailableException("End point %s not found for broker %d".format(protocolType,id))
    }
  }

}
