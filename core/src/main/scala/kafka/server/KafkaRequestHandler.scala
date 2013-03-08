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

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(id: Int, brokerId: Int, val requestChannel: RequestChannel, apis: KafkaApis) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

  def run() { 
    while(true) {
      try {
        val req = requestChannel.receiveRequest()
        if(req eq RequestChannel.AllDone) {
          trace("receives shut down command, shut down".format(brokerId, id))
          return
        }
        req.dequeueTimeMs = SystemTime.milliseconds
        debug("handles request " + req)
        apis.handle(req)
      } catch {
        case e: Throwable => error("exception when handling request", e)
      }
    }
  }

  def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
}

class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              numThreads: Int) extends Logging {
  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) { 
    runnables(i) = new KafkaRequestHandler(i, brokerId, requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()
  }
  
  def shutdown() {
    info("shutting down")
    for(handler <- runnables)
      handler.shutdown
    for(thread <- threads)
      thread.join
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: String) extends KafkaMetricsGroup {
  val messagesInRate = newMeter(name + "MessagesInPerSec",  "messages", TimeUnit.SECONDS)
  val bytesInRate = newMeter(name + "BytesInPerSec",  "bytes", TimeUnit.SECONDS)
  val bytesOutRate = newMeter(name + "BytesOutPerSec",  "bytes", TimeUnit.SECONDS)
  val failedProduceRequestRate = newMeter(name + "FailedProduceRequestsPerSec",  "requests", TimeUnit.SECONDS)
  val failedFetchRequestRate = newMeter(name + "FailedFetchRequestsPerSec",  "requests", TimeUnit.SECONDS)
}

object BrokerTopicStats extends Logging {
  private val valueFactory = (k: String) => new BrokerTopicMetrics(k)
  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  private val allTopicsStats = new BrokerTopicMetrics("AllTopics")

  def getBrokerAllTopicsStats(): BrokerTopicMetrics = allTopicsStats

  def getBrokerTopicStats(topic: String): BrokerTopicMetrics = {
    stats.getAndMaybePut(topic + "-")
  }
}
