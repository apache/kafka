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

import java.net.InetAddress
import java.util.{Calendar, Locale}
import java.util.concurrent.TimeUnit

import kafka.api.ApiVersion
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import com.yammer.metrics.core.Gauge
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.I0Itec.zkclient.IZkStateListener
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection.mutable.Set

/**
  * This class registers the broker in zookeeper to allow
  * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
  *   /brokers/ids/[0...N] --> advertisedHost:advertisedPort
  *
  * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
  * we retry for the connectionRetryMs, then log and die.
  *
  * The callback for disconnect allows the upstream services to handle disconnects gracefully
 */
class KafkaHealthcheck(brokerId: Int,
                       advertisedEndpoints: Seq[EndPoint],
                       zkClient: KafkaZkClient,
                       brokerInfo: BrokerInfo
                      ) extends Logging {


  def startup() {
    register()
  }

  /**
   * Register this broker as "alive" in zookeeper
   */
  def register() {
    zkClient.registerBrokerInZk(brokerInfo)
  }

  def shutdown(): Unit = ???

}
