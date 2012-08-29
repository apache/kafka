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

import org.apache.log4j.Logger
import kafka.log._
import kafka.network._
import kafka.message._
import kafka.api._
import kafka.common.{MessageSizeTooLargeException, ErrorMapping}
import java.util.concurrent.atomic.AtomicLong
import kafka.utils._

/**
 * Logic to handle the various Kafka requests
 */
private[kafka] class KafkaRequestHandlers(val logManager: LogManager) extends Logging {
  
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
    debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    None
  }

  def handleMultiProducerRequest(receive: Receive): Option[Send] = {
    val request = MultiProducerRequest.readFrom(receive.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Multiproducer request " + request.toString)
    request.produces.map(handleProducerRequest(_, "MultiProducerRequest"))
    None
  }

  private def handleProducerRequest(request: ProducerRequest, requestHandlerName: String) = {
    val partition = request.getTranslatedPartition(logManager.chooseRandomPartition)
    try {
      logManager.getOrCreateLog(request.topic, partition).append(request.messages)
      trace(request.messages.sizeInBytes + " bytes written to logs.")
      request.messages.foreach(m => trace("wrote message %s to disk".format(m.message.checksum)))
      BrokerTopicStat.getBrokerTopicStat(request.topic).recordBytesIn(request.messages.sizeInBytes)
      BrokerTopicStat.getBrokerAllTopicStat.recordBytesIn(request.messages.sizeInBytes)
    }
    catch {
      case e: MessageSizeTooLargeException =>
        warn(e.getMessage() + " on " + request.topic + ":" + partition)
        BrokerTopicStat.getBrokerTopicStat(request.topic).recordFailedProduceRequest
        BrokerTopicStat.getBrokerAllTopicStat.recordFailedProduceRequest
      case t =>
        error("Error processing " + requestHandlerName + " on " + request.topic + ":" + partition, t)
        BrokerTopicStat.getBrokerTopicStat(request.topic).recordFailedProduceRequest
        BrokerTopicStat.getBrokerAllTopicStat.recordFailedProduceRequest
        throw t
    }
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
      trace("Fetching log segment for topic, partition, offset, maxSize = " + fetchRequest)
      val log = logManager.getLog(fetchRequest.topic, fetchRequest.partition)
      if (log != null) {
        response = new MessageSetSend(log.read(fetchRequest.offset, fetchRequest.maxSize))
        BrokerTopicStat.getBrokerTopicStat(fetchRequest.topic).recordBytesOut(response.messages.sizeInBytes)
        BrokerTopicStat.getBrokerAllTopicStat.recordBytesOut(response.messages.sizeInBytes)
      }
      else
        response = new MessageSetSend()
    }
    catch {
      case e =>
        error("error when processing request " + fetchRequest, e)
        BrokerTopicStat.getBrokerTopicStat(fetchRequest.topic).recordFailedFetchRequest
        BrokerTopicStat.getBrokerAllTopicStat.recordFailedFetchRequest
        response=new MessageSetSend(MessageSet.Empty, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    }
    response
  }

  def handleOffsetRequest(request: Receive): Option[Send] = {
    val offsetRequest = OffsetRequest.readFrom(request.buffer)
    if(requestLogger.isTraceEnabled)
      requestLogger.trace("Offset request " + offsetRequest.toString)
    val offsets = logManager.getOffsets(offsetRequest)
    val response = new OffsetArraySend(offsets)
    Some(response)
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
