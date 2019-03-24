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

import java.util.concurrent.TimeUnit
import kafka.network.RequestChannel
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ProduceRequest, RequestContext}
import org.apache.kafka.common.Configurable

/**
  * Top level interface that all pluggable observer must implement. Kafka will read the 'observer.class.name' config
  * value at startup time, create an instance of the specificed class using the default constructor, and call its
  * 'configure' method.
  *
  * From that point onwards, every pair of request and response will be routed to the 'record' method.
  *
  * If 'observer.class.name' has no value specified or the specified class does not exist, the <code>NoOpObserver</code>
  * will be used as a place holder.
  */
trait Observer extends Configurable {

  /**
    * Observe a request and its corresponding response
    *
    * @param requestContext the context information about the request
    * @param request  the request being observed for a various purpose(s)
    * @param response the response to the request
    */
  def observe(requestContext: RequestContext, request: AbstractRequest, response: AbstractResponse): Unit

  /**
    * Observe a produce request. This method handles only the produce request since produce request is special in
    * two ways. Firstly, if ACK is set to be 0, there is no produce response associated with the produce request.
    * Secondly, the lifecycle of some inner fields in a ProduceRequest is shorter than the lifecycle of the produce
    * request itself. That means in some situations, when <code>observe</code> is called on a produce request and
    * response pair, some fields in the produce request has been null-ed already so that the produce request and
    * response is not observable (or no useful information). Therefore this method exists for the purpose of allowing
    * users to observe on the produce request before its corresponding response is created.
    *
    * @param requestContext the context information about the request
    * @param produceRequest  the produce request being observed for a various purpose(s)
    */
  def observeProduceRequest(requestContext: RequestContext, produceRequest: ProduceRequest): Unit

  /**
    * Close the observer with timeout.
    *
    * @param timeout the maximum time to wait to close the observer.
    * @param unit    the time unit.
    */
  def close(timeout: Long, unit: TimeUnit): Unit
}

object Observer {

  /**
    * Generates a description of the given request and response. It could be used mostly for debugging purpose.
    *
    * @param request  the request being described
    * @param response the response to the request
    */
  def describeRequestAndResponse(request: RequestChannel.Request, response: AbstractResponse): String = {
    var requestDesc = "Request"
    var responseDesc = "Response"
    try {
      if (request == null) {
        requestDesc += " null"
      } else {
        requestDesc += (" header: " + request.header)
        requestDesc += (" from service with principal: " +
          request.session.sanitizedUser +
          " IP address: " + request.session.clientAddress)
      }
      requestDesc += " | " // Separate the response description from the request description

      if (response == null) {
        responseDesc += " null"
      } else {
        responseDesc += (if (response.errorCounts == null || response.errorCounts.size == 0) {
          " with no error"
        } else {
          " with errors: " + response.errorCounts
        })
      }
    } catch {
      case e: Exception => return e.toString // If describing fails, return the exception message directly
    }
    requestDesc + responseDesc
  }
}