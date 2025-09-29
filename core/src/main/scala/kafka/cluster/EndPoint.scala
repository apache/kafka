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

import org.apache.kafka.common.{KafkaException, Endpoint => JEndpoint}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import java.util.Locale

object EndPoint {
  def parseListenerName(connectionString: String): String = {
    val firstColon = connectionString.indexOf(':')
    if (firstColon < 0) {
      throw new KafkaException(s"Unable to parse a listener name from $connectionString")
    }
    connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT)
  }

  def fromJava(endpoint: JEndpoint): EndPoint =
    new EndPoint(endpoint.host(),
      endpoint.port(),
      new ListenerName(endpoint.listenerName().get()),
      endpoint.securityProtocol())
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
  def connectionString: String = {
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    listenerName.value + "://" + hostport
  }

  def toJava: JEndpoint = {
    new JEndpoint(listenerName.value, securityProtocol, host, port)
  }
}
