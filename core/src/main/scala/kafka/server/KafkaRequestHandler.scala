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

/**
 * Thread that answers kafka requests.
 */
class KafkaRequestHandler(val requestChannel: RequestChannel, val handle: (Receive) => Option[Send]) extends Runnable with Logging { 
  
  def run() { 
    while(true) { 
      val req = requestChannel.receiveRequest()
      trace("Processor " + Thread.currentThread.getName + " got request " + req)
      if(req == RequestChannel.AllDone)
        return
      handle(req.request) match { 
        case Some(send) => { 
          val resp = new RequestChannel.Response(processor = req.processor, 
                                                 requestKey = req.requestKey, 
						 response = send, 
						 start = req.start, 
						 ellapsed = -1)
          requestChannel.sendResponse(resp)
          trace("Processor " + Thread.currentThread.getName + " sent response " + resp)
        }
        case None =>
      }
    }
  }

  def shutdown() {
    requestChannel.sendRequest(RequestChannel.AllDone)
  }
  
}

/**
 * Pool of request handling threads.
 */
class KafkaRequestHandlerPool(val requestChannel: RequestChannel, val handler: (Receive) => Option[Send], numThreads: Int) { 
  
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) { 
    runnables(i) = new KafkaRequestHandler(requestChannel, handler)
    threads(i) = new Thread(runnables(i), "kafka-request-handler-" + i)
    threads(i).setDaemon(true)
    threads(i).start()
  }
  
  def shutdown() {
    for(handler <- runnables)
      handler.shutdown
    for(thread <- threads)
      thread.join
  }
  
}
