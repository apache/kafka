/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import kafka.common.KafkaException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.Utils

object EndPoint {

  private val uriParseExp = """^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)""".r

  def readFrom(buffer: ByteBuffer): EndPoint = {
    val port = buffer.getInt()
    val host = readShortString(buffer)
    val protocol = buffer.getShort()
    EndPoint(host, port, SecurityProtocol.forId(protocol))
  }

  /**
   * Create EndPoint object from connectionString
   * @param connectionString the format is protocol://host:port or protocol://[ipv6 host]:port
   *                         for example: PLAINTEXT://myhost:9092 or PLAINTEXT://[::1]:9092
   *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
   *                         Negative ports are also accepted, since they are used in some unit tests
   * @return
   */
  def createEndPoint(connectionString: String): EndPoint = {
    connectionString match {
      case uriParseExp(protocol, "", port) => new EndPoint(null, port.toInt, SecurityProtocol.forName(protocol))
      case uriParseExp(protocol, host, port) => new EndPoint(host, port.toInt, SecurityProtocol.forName(protocol))
      case _ => throw new KafkaException("Unable to parse " + connectionString + " to a broker endpoint")
    }
  }
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
 */
case class EndPoint(host: String, port: Int, protocolType: SecurityProtocol) {

  def connectionString(): String = {
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    protocolType + "://" + hostport
  }

  def writeTo(buffer: ByteBuffer): Unit = {
    buffer.putInt(port)
    writeShortString(buffer, host)
    buffer.putShort(protocolType.id)
  }

  def sizeInBytes: Int =
    4 + /* port */
    shortStringLength(host) +
    2 /* protocol id */
}
