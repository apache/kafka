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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch
import kafka.api.FetchRequestBuilder
import kafka.utils.Logging
import kafka.cluster.{Broker, Replica}
import kafka.consumer.SimpleConsumer

class ReplicaFetcherThread(name: String, replica: Replica, leaderBroker: Broker, config: KafkaConfig)
  extends Thread(name) with Logging {
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)
  private val replicaConsumer = new SimpleConsumer(leaderBroker.host, leaderBroker.port,
    config.replicaSocketTimeoutMs, config.replicaSocketBufferSize)

  override def run() {
    try {
      info("Starting replica fetch thread %s for topic %s partition %d".format(name, replica.topic, replica.partition.partitionId))
      while(isRunning.get()) {
        val builder = new FetchRequestBuilder().
          clientId(name).
          replicaId(replica.brokerId).
          maxWait(config.replicaMaxWaitTimeMs).
          minBytes(config.replicaMinBytes)

        // TODO: KAFKA-339 Keep this simple single fetch for now. Change it to fancier multi fetch when message
        // replication actually works
        val fetchOffset = replica.logEndOffset()
        trace("Follower %d issuing fetch request for topic %s partition %d to leader %d from offset %d"
          .format(replica.brokerId, replica.topic, replica.partition.partitionId, leaderBroker.id, fetchOffset))
        builder.addFetch(replica.topic, replica.partition.partitionId, fetchOffset, config.replicaFetchSize)

        val fetchRequest = builder.build()
        val response = replicaConsumer.fetch(fetchRequest)
        // TODO: KAFKA-339 Check for error. Don't blindly read the messages
        // append messages to local log
        replica.log.get.append(response.messageSet(replica.topic, replica.partition.partitionId))
        // record the hw sent by the leader for this partition
        val followerHighWatermark = replica.logEndOffset().min(response.data.head.partitionData.head.hw)
        replica.highWatermark(Some(followerHighWatermark))
        trace("Follower %d set replica highwatermark for topic %s partition %d to %d"
          .format(replica.brokerId, replica.topic, replica.partition.partitionId, replica.highWatermark()))
      }
    }catch {
      case e: InterruptedException => warn("Replica fetcher thread %s interrupted. Shutting down".format(name))
      case e1 => error("Error in replica fetcher thread. Shutting down due to ", e1)
    }
    shutdownComplete()
  }

  private def shutdownComplete() = {
    replicaConsumer.close()
    shutdownLatch.countDown
  }

  def getLeader(): Broker = leaderBroker

  def shutdown() {
    info("Shutting down replica fetcher thread")
    isRunning.set(false)
    interrupt()
    shutdownLatch.await()
    info("Replica fetcher thread shutdown completed")
  }
}