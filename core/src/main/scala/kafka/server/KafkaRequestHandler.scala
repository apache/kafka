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
import java.util.concurrent.atomic.AtomicLong

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(val requestChannel: RequestChannel, apis: KafkaApis) extends Runnable with Logging { 
     
  def run() { 
    while(true) { 
      val req = requestChannel.receiveRequest()
      trace("Processor " + Thread.currentThread.getName + " got request " + req)
      if(req == RequestChannel.AllDone)
        return
      apis.handle(req)
    }
  }

  def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
  
}

class KafkaRequestHandlerPool(val requestChannel: RequestChannel, 
                              val apis: KafkaApis, 
                              numThreads: Int) { 
  
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) { 
    runnables(i) = new KafkaRequestHandler(requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()
  }
  
  def shutdown() {
    for(handler <- runnables)
      handler.shutdown
    for(thread <- threads)
      thread.join
  }
  
}

trait BrokerTopicStatMBean {
  def getMessagesIn: Long
  def getBytesIn: Long
  def getBytesOut: Long
  def getFailedProduceRequest: Long
  def getFailedFetchRequest: Long
}

@threadsafe
class BrokerTopicStat extends BrokerTopicStatMBean {
  private val numCumulatedMessagesIn = new AtomicLong(0)
  private val numCumulatedBytesIn = new AtomicLong(0)
  private val numCumulatedBytesOut = new AtomicLong(0)
  private val numCumulatedFailedProduceRequests = new AtomicLong(0)
  private val numCumulatedFailedFetchRequests = new AtomicLong(0)

  def getMessagesIn: Long = numCumulatedMessagesIn.get

  def recordMessagesIn(nMessages: Int) = numCumulatedMessagesIn.getAndAdd(nMessages)

  def getBytesIn: Long = numCumulatedBytesIn.get

  def recordBytesIn(nBytes: Long) = numCumulatedBytesIn.getAndAdd(nBytes)

  def getBytesOut: Long = numCumulatedBytesOut.get

  def recordBytesOut(nBytes: Long) = numCumulatedBytesOut.getAndAdd(nBytes)

  def recordFailedProduceRequest = numCumulatedFailedProduceRequests.getAndIncrement

  def getFailedProduceRequest = numCumulatedFailedProduceRequests.get()

  def recordFailedFetchRequest = numCumulatedFailedFetchRequests.getAndIncrement

  def getFailedFetchRequest = numCumulatedFailedFetchRequests.get()
}

object BrokerTopicStat extends Logging {
  private val stats = new Pool[String, BrokerTopicStat]
  private val allTopicStat = new BrokerTopicStat
  Utils.registerMBean(allTopicStat, "kafka:type=kafka.BrokerAllTopicStat")

  def getBrokerAllTopicStat(): BrokerTopicStat = allTopicStat

  def getBrokerTopicStat(topic: String): BrokerTopicStat = {
    var stat = stats.get(topic)
    if (stat == null) {
      stat = new BrokerTopicStat
      if (stats.putIfNotExists(topic, stat) == null)
        Utils.registerMBean(stat, "kafka:type=kafka.BrokerTopicStat." + topic)
      else
        stat = stats.get(topic)
    }
    return stat
  }
}
