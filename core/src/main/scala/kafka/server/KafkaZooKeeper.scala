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
import kafka.utils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import kafka.common._


/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 */
class KafkaZooKeeper(config: KafkaConfig) extends Logging {

  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId
  private var zkClient: ZkClient = null

   def startup() {
     /* start client */
     info("connecting to ZK: " + config.zkConnect)
     zkClient = KafkaZookeeperClient.getZookeeperClient(config)
     zkClient.subscribeStateChanges(new SessionExpireListener)
     registerBrokerInZk()
   }

  private def registerBrokerInZk() {
    info("Registering broker " + brokerIdPath)
    val hostName = config.hostName
    val creatorId = hostName + "-" + System.currentTimeMillis
    ZkUtils.registerBrokerInZk(zkClient, config.brokerId, hostName, creatorId, config.port)
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
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }
  }

  def shutdown() {
    if (zkClient != null) {
      info("Closing zookeeper client...")
      zkClient.close()
    }
  }

  private def doesTopicExistInCluster(topic: String) : Boolean = {
    val allTopics = ZkUtils.getAllTopics(zkClient)
    trace("all topics, %s, topic %s".format(allTopics, topic))
    allTopics.contains(topic)
  }

  def ensurePartitionLeaderOnThisBroker(topic: String, partition: Int) {
    if(!doesTopicExistInCluster(topic))
      throw new UnknownTopicException("Topic %s doesn't exist in the cluster".format(topic))
    // check if partition id is invalid
    if(partition < 0)
      throw new InvalidPartitionException("Partition %d is invalid".format(partition))
    ZkUtils.getLeaderForPartition(zkClient, topic, partition) match {
      case Some(leader) =>
        if(leader != config.brokerId)
          throw new LeaderNotAvailableException("Broker %d is not leader for partition %d for topic %s"
            .format(config.brokerId, partition, topic))
      case None =>
        throw new LeaderNotAvailableException("There is no leader for topic %s partition %d".format(topic, partition))
    }
  }

  def getZookeeperClient = {
    zkClient
  }
}
