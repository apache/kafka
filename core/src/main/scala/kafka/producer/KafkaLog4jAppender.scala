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

package kafka.producer

import async.MissingConfigException
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.helpers.LogLog
import kafka.utils.Logging
import java.util.{Properties, Date}

class KafkaLog4jAppender extends AppenderSkeleton with Logging {
  var topic:String = null
  var serializerClass:String = null
  var brokerList:String = null
  var producerType:String = null
  var compressionCodec:String = null
  var enqueueTimeout:String = null
  var queueSize:String = null
  var requiredNumAcks: Int = Int.MaxValue

  private var producer: Producer[String, String] = null

  def getTopic:String = topic
  def setTopic(topic: String) { this.topic = topic }

  def getBrokerList:String = brokerList
  def setBrokerList(brokerList: String) { this.brokerList = brokerList }

  def getSerializerClass:String = serializerClass
  def setSerializerClass(serializerClass:String) { this.serializerClass = serializerClass }

  def getProducerType:String = producerType
  def setProducerType(producerType:String) { this.producerType = producerType }

  def getCompressionCodec:String = compressionCodec
  def setCompressionCodec(compressionCodec:String) { this.compressionCodec = compressionCodec }

  def getEnqueueTimeout:String = enqueueTimeout
  def setEnqueueTimeout(enqueueTimeout:String) { this.enqueueTimeout = enqueueTimeout }

  def getQueueSize:String = queueSize
  def setQueueSize(queueSize:String) { this.queueSize = queueSize }

  def getRequiredNumAcks:Int = requiredNumAcks
  def setRequiredNumAcks(requiredNumAcks:Int) { this.requiredNumAcks = requiredNumAcks }

  override def activateOptions() {
    // check for config parameter validity
    val props = new Properties()
    if(brokerList != null)
      props.put("broker.list", brokerList)
    if(props.isEmpty)
      throw new MissingConfigException("The broker.list property should be specified")
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(serializerClass == null) {
      serializerClass = "kafka.serializer.StringEncoder"
      LogLog.debug("Using default encoder - kafka.serializer.StringEncoder")
    }
    props.put("serializer.class", serializerClass)
    //These have default values in ProducerConfig and AsyncProducerConfig. We don't care if they're not specified
    if(producerType != null) props.put("producer.type", producerType)
    if(compressionCodec != null) props.put("compression.codec", compressionCodec)
    if(enqueueTimeout != null) props.put("queue.enqueue.timeout.ms", enqueueTimeout)
    if(queueSize != null) props.put("queue.buffering.max.messages", queueSize)
    if(requiredNumAcks != Int.MaxValue) props.put("request.required.acks", requiredNumAcks.toString)
    val config : ProducerConfig = new ProducerConfig(props)
    producer = new Producer[String, String](config)
    LogLog.debug("Kafka producer connected to " +  config.brokerList)
    LogLog.debug("Logging for topic: " + topic)
  }

  override def append(event: LoggingEvent)  {
    val message : String = if( this.layout == null) {
      event.getRenderedMessage
    }
    else this.layout.format(event)
    LogLog.debug("[" + new Date(event.getTimeStamp).toString + "]" + message)
    val messageData = new KeyedMessage[String, String](topic, message)
    producer.send(messageData);
  }

  override def close() {
    if(!this.closed) {
      this.closed = true
      producer.close()
    }
  }

  override def requiresLayout: Boolean = false
}
