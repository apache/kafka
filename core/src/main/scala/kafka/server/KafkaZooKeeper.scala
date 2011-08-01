/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.log4j.Logger
import kafka.cluster.Broker
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.log.LogManager
import java.net.InetAddress

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 * 
 */
class KafkaZooKeeper(config: KafkaConfig, logManager: LogManager) {
  
  private val logger = Logger.getLogger(classOf[KafkaZooKeeper])
  
  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId
  var zkClient: ZkClient = null
  var topics: List[String] = Nil
  val lock = new Object()
  
  def startup() {
    /* start client */
    logger.info("connecting to ZK: " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, StringSerializer)
    zkClient.subscribeStateChanges(new SessionExpireListener)
  }

  def registerBrokerInZk() {
    logger.info("Registering broker " + brokerIdPath)
    val hostName = if (config.hostName == null) InetAddress.getLocalHost.getHostAddress else config.hostName
    val creatorId = hostName + "-" + System.currentTimeMillis
    val broker = new Broker(config.brokerId, creatorId, hostName, config.port)
    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    logger.info("Registering broker " + brokerIdPath + " succeeded with " + broker)
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
    logger.info("Begin registering broker topic " + brokerTopicPath + " with " + numParts.toString + " partitions")
    ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, numParts.toString)
    logger.info("End registering broker topic " + brokerTopicPath)
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
      logger.info("re-registering broker info in ZK for broker " + config.brokerId)
      registerBrokerInZk()
      lock synchronized {
        logger.info("re-registering broker topics in ZK for broker " + config.brokerId)
        for (topic <- topics)
          registerTopicInZkInternal(topic)
      }
      logger.info("done re-registering broker")
    }
  }

  def close() {
    if (zkClient != null) {
      logger.info("Closing zookeeper client...")
      zkClient.close()
    }
  } 
  
}
