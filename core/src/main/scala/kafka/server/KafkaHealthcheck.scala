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
import kafka.zk.KafkaZkClient
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
                       rack: Option[String],
                       interBrokerProtocolVersion: ApiVersion,
                       stateChangeCallback: (ZKState.Value, Throwable)=> Unit) extends Logging {

  private[server] val sessionExpireListener = new SessionExpireListener

  def startup() {
    //zkUtils.subscribeStateChanges(sessionExpireListener)
    register()
  }

  /**
   * Register this broker as "alive" in zookeeper
   */
  def register() {
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    val updatedEndpoints = advertisedEndpoints.map(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    // the default host and port are here for compatibility with older clients that only support PLAINTEXT
    // we choose the first plaintext port, if there is one
    // or we register an empty endpoint, which means that older clients will not be able to connect
    val plaintextEndpoint = updatedEndpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).getOrElse(
      new EndPoint(null, -1, null, null))

    zkClient.registerBrokerInZk(brokerId, plaintextEndpoint.host, plaintextEndpoint.port, updatedEndpoints, jmxPort, rack,
      interBrokerProtocolVersion)
  }

  def shutdown(): Unit = sessionExpireListener.shutdown()

  /**
   *  When we get a SessionExpired event, it means that we have lost all ephemeral nodes and ZKClient has re-established
   *  a connection for us. We need to re-register this broker in the broker registry. We rely on `handleStateChanged`
   *  to record ZooKeeper connection state metrics.
   */
  class SessionExpireListener extends IZkStateListener with KafkaMetricsGroup {

    private val metricNames = Set[String]()

    private var zkReconnectStartMs = null

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
        val name = s"ZooKeeper${eventType}PerSec"
        metricNames += name
        state -> newMeter(name, eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
      }
    }

    private[server] val sessionStateGauge =
      newGauge("SessionState", new Gauge[String] {
        override def value: String = "NOT_IMPLEMENTED_YET"
          //Option(zkUtils.zkConnection.getZookeeperState.toString).getOrElse("DISCONNECTED")
      })

    metricNames += "SessionState"

    @throws[Exception]
    override def handleStateChanged(state: KeeperState) {
      stateToMeterMap.get(state).foreach(_.mark())
      if (stateChangeCallback != null){
        stateChangeCallback(ZKState.withName(state.toString), null)
      }
    }

    @throws[Exception]
    override def handleNewSession() {
      info("re-registering broker info in ZK for broker " + brokerId)
      register()
      if (stateChangeCallback != null){
        stateChangeCallback(ZKState.NewSession, null)
      }
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }

    override def handleSessionEstablishmentError(error: Throwable) {
      this.error("Session Establishment Error: %s".format(error))
      if (stateChangeCallback == null){
        // this is a check for the callers (library writers)
        // ideally should never see it any errors
        this.fatal("Session Establishment Error: No callback configured to cleanly exit, exiting without a callback")
      }
      stateChangeCallback(ZKState.SessionEstablishmentError, error)
    }

    def shutdown(): Unit = metricNames.foreach(removeMetric(_))

  }

}
