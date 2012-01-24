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

package kafka.network

import java.util.concurrent._

object RequestChannel { 
  val AllDone = new Request(1, 2, null, 0)
  case class Request(val processor: Int, requestKey: Any, request: Receive, start: Long)
  case class Response(val processor: Int, requestKey: Any, response: Send, start: Long, ellapsed: Long)
}

class RequestChannel(val numProcessors: Int, val queueSize: Int) { 
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new ArrayBlockingQueue[RequestChannel.Response](queueSize)
    

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }
  
  /** Send a response back to the socket server to be sent over the network */ 
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for(onResponse <- responseListeners)
      onResponse(response.processor)
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request = 
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response =
    responseQueues(processor).poll()

  def addResponseListener(onResponse: Int => Unit) { 
    responseListeners ::= onResponse
  }

}
