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
import java.util.Locale
import java.util.concurrent.TimeUnit

import kafka.api.ApiVersion
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.I0Itec.zkclient.IZkStateListener
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.Watcher.Event.KeeperState

/**
 * This class registers the broker in zookeeper to allow 
 * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
 *   /brokers/ids/[0...N] --> advertisedHost:advertisedPort
 *   
 * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
 * we are dead.
 */
class KafkaHealthcheck(brokerId: Int,
                       advertisedEndpoints: Map[SecurityProtocol, EndPoint],
                       zkUtils: ZkUtils,
                       rack: Option[String],
                       interBrokerProtocolVersion: ApiVersion) extends Logging {

  private val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + brokerId
  private[server] val sessionExpireListener = new SessionExpireListener

  def startup() {
    zkUtils.zkClient.subscribeStateChanges(sessionExpireListener)
    register()
  }

  /**
   * Register this broker as "alive" in zookeeper
   */
  def register() {
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    val updatedEndpoints = advertisedEndpoints.mapValues(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        EndPoint(InetAddress.getLocalHost.getCanonicalHostName, endpoint.port, endpoint.protocolType)
      else
        endpoint
    )

    // the default host and port are here for compatibility with older client
    // only PLAINTEXT is supported as default
    // if the broker doesn't listen on PLAINTEXT protocol, an empty endpoint will be registered and older clients will be unable to connect
    val plaintextEndpoint = updatedEndpoints.getOrElse(SecurityProtocol.PLAINTEXT, new EndPoint(null,-1,null))
    zkUtils.registerBrokerInZk(brokerId, plaintextEndpoint.host, plaintextEndpoint.port, updatedEndpoints, jmxPort, rack,
      interBrokerProtocolVersion)
  }

  /**
   *  When we get a SessionExpired event, it means that we have lost all ephemeral nodes and ZKClient has re-established
   *  a connection for us. We need to re-register this broker in the broker registry. We rely on `handleStateChanged`
   *  to record ZooKeeper connection state metrics.
   */
  class SessionExpireListener extends IZkStateListener with KafkaMetricsGroup {

    private[server] val stateToMeterMap = {
      import KeeperState._
      val stateToEventTypeMap = Map(
        Disconnected -> "Disconnects",
        SyncConnected -> "SyncConnects",
        AuthFailed -> "AuthFailures",
        ConnectedReadOnly -> "ReadOnlyConnects",
        SaslAuthenticated -> "SaslAuthentications",
        Expired -> "Expires"
      )
      stateToEventTypeMap.map { case (state, eventType) =>
        state -> newMeter(s"ZooKeeper${eventType}PerSec", eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
      }
    }

    @throws(classOf[Exception])
    override def handleStateChanged(state: KeeperState) {
      stateToMeterMap.get(state).foreach(_.mark())
    }

    @throws(classOf[Exception])
    override def handleNewSession() {
      info("re-registering broker info in ZK for broker " + brokerId)
      register()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }

    override def handleSessionEstablishmentError(error: Throwable) {
      fatal("Could not establish session with zookeeper", error)
    }

  }

}
