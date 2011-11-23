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

import kafka.utils._
import kafka.cluster.Broker
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.log.LogManager
import java.net.InetAddress

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 * 
 */
class KafkaZooKeeper(config: KafkaConfig, logManager: LogManager) extends Logging {
  
  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId
  var zkClient: ZkClient = null
  var topics: List[String] = Nil
  val lock = new Object()
  
  def startup() {
    /* start client */
    info("connecting to ZK: " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
    zkClient.subscribeStateChanges(new SessionExpireListener)
  }

  def registerBrokerInZk() {
    info("Registering broker " + brokerIdPath)
    val hostName = if (config.hostName == null) InetAddress.getLocalHost.getHostAddress else config.hostName
    val creatorId = hostName + "-" + System.currentTimeMillis
    val broker = new Broker(config.brokerId, creatorId, hostName, config.port)
    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " + 
                                   "indicates that you either have configured a brokerid that is already in use, or " + 
                                   "else you have shutdown this broker and restarted it faster than the zookeeper " + 
                                   "timeout so it appears to be re-registering.")
    }
    info("Registering broker " + brokerIdPath + " succeeded with " + broker)
  }

  def registerTopicInZk(topic: String) {
    registerTopicInZkInternal(topic)
    lock synchronized {
      topics ::= topic
    }
  }

  def registerTopicInZkInternal(topic: String) {
    val brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + config.brokerId
    val numParts = logManager.getTopicPartitionsMap.getOrElse(topic, config.numPartitions)
    info("Begin registering broker topic " + brokerTopicPath + " with " + numParts.toString + " partitions")
    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, numParts.toString)
    info("End registering broker topic " + brokerTopicPath)
  }

  /**
   *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
   *  connection for us. We need to re-register this broker in the broker registry.
   */
  class SessionExpireListener() extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + config.brokerId)
      registerBrokerInZk()
      lock synchronized {
        info("re-registering broker topics in ZK for broker " + config.brokerId)
        for (topic <- topics)
          registerTopicInZkInternal(topic)
      }
      info("done re-registering broker")
    }
  }

  def close() {
    if (zkClient != null) {
      info("Closing zookeeper client...")
      zkClient.close()
    }
  } 
  
}
