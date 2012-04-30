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

import org.apache.log4j._
import kafka.network._
import kafka.utils._

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
