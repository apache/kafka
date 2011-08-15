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

import java.nio.channels._
import org.apache.log4j.Logger
import kafka.producer._
import kafka.consumer._
import kafka.log._
import kafka.network._
import kafka.message._
import kafka.server._
import kafka.api._
import kafka.common.ErrorMapping
import kafka.utils.{Utils, SystemTime}
import java.io.IOException

/**
 * Logic to handle the various Kafka requests
 */
private[kafka] class KafkaRequestHandlers(val logManager: LogManager) {
  
  private val logger = Logger.getLogger(classOf[KafkaRequestHandlers])
  private val requestLogger = Logger.getLogger("kafka.request.logger")

  def handlerFor(requestTypeId: Short, request: Receive): Handler.Handler = {
    requestTypeId match {
      case RequestKeys.Produce => handleProducerRequest _
      case RequestKeys.Fetch => handleFetchRequest _
      case RequestKeys.MultiFetch => handleMultiFetchRequest _
      case RequestKeys.MultiProduce => handleMultiProducerRequest _
      case RequestKeys.Offsets => handleOffsetRequest _
      case _ => throw new IllegalStateException("No mapping found for handler id " + requestTypeId)
    }
  }
  
  def handleProducerRequest(receive: Receive): Option[Send] = {
    val sTime = SystemTime.milliseconds
    val request = ProducerRequest.readFrom(receive.buffer)

    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Producer request " + request.toString)
    handleProducerRequest(request, "ProduceRequest")
    if (logger.isDebugEnabled)
      logger.debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    None
  }

  def handleMultiProducerRequest(receive: Receive): Option[Send] = {
    val request = MultiProducerRequest.readFrom(receive.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Multiproducer request ")
    request.produces.map(handleProducerRequest(_, "MultiProducerRequest"))
    None
  }

  private def handleProducerRequest(request: ProducerRequest, requestHandlerName: String) = {
    val partition = request.getTranslatedPartition(logManager.chooseRandomPartition)
    try {
      logManager.getOrCreateLog(request.topic, partition).append(request.messages)
      if(logger.isTraceEnabled)
        logger.trace(request.messages.sizeInBytes + " bytes written to logs.")
    }
    catch {
      case e =>
        logger.error("Error processing " + requestHandlerName + " on " + request.topic + ":" + partition, e)
        e match {
          case _: IOException =>
            logger.fatal("Halting due to unrecoverable I/O error while handling producer request: " + e.getMessage, e)
            Runtime.getRuntime.halt(1)
          case _ =>
        }
        throw e
    }
    None
  }

  def handleFetchRequest(request: Receive): Option[Send] = {
    val fetchRequest = FetchRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Fetch request " + fetchRequest.toString)
    Some(readMessageSet(fetchRequest))
  }
  
  def handleMultiFetchRequest(request: Receive): Option[Send] = {
    val multiFetchRequest = MultiFetchRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Multifetch request")
    multiFetchRequest.fetches.foreach(req => requestLogger.trace(req.toString))
    var responses = multiFetchRequest.fetches.map(fetch =>
        readMessageSet(fetch)).toList
    
    Some(new MultiMessageSetSend(responses))
  }

  private def readMessageSet(fetchRequest: FetchRequest): MessageSetSend = {
    var  response: MessageSetSend = null
    try {
      logger.trace("Fetching log segment for topic = " + fetchRequest.topic + " and partition = " + fetchRequest.partition)
      val log = logManager.getOrCreateLog(fetchRequest.topic, fetchRequest.partition)
      response = new MessageSetSend(log.read(fetchRequest.offset, fetchRequest.maxSize))
    }
    catch {
      case e =>
        logger.error("error when processing request " + fetchRequest, e)
        response=new MessageSetSend(MessageSet.Empty, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    response
  }

  def handleOffsetRequest(request: Receive): Option[Send] = {
    val offsetRequest = OffsetRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Offset request " + offsetRequest.toString)
    val log = logManager.getOrCreateLog(offsetRequest.topic, offsetRequest.partition)
    val offsets = log.getOffsetsBefore(offsetRequest)
    val response = new OffsetArraySend(offsets)
    Some(response)
  }
}
