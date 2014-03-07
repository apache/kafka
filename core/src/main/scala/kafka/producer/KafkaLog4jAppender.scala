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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaLog4jAppender extends AppenderSkeleton with Logging {
  var topic:String = null
  var brokerList:String = null
  var compressionCodec:String = null
  var requiredNumAcks: Int = Int.MaxValue

  private var producer: KafkaProducer = null

  def getTopic:String = topic
  def setTopic(topic: String) { this.topic = topic }

  def getBrokerList:String = brokerList
  def setBrokerList(brokerList: String) { this.brokerList = brokerList }

  def getCompressionCodec:String = compressionCodec
  def setCompressionCodec(compressionCodec:String) { this.compressionCodec = compressionCodec }

  def getRequiredNumAcks:Int = requiredNumAcks
  def setRequiredNumAcks(requiredNumAcks:Int) { this.requiredNumAcks = requiredNumAcks }

  override def activateOptions() {
    // check for config parameter validity
    val props = new Properties()
    if(brokerList != null)
      props.put("metadata.broker.list", brokerList)
    if(props.isEmpty)
      throw new MissingConfigException("The metadata.broker.list property should be specified")
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(compressionCodec != null) props.put("compression.codec", compressionCodec)
    if(requiredNumAcks != Int.MaxValue) props.put("request.required.acks", requiredNumAcks.toString)
    producer = new KafkaProducer(props)
    LogLog.debug("Kafka producer connected to " +  brokerList)
    LogLog.debug("Logging for topic: " + topic)
  }

  override def append(event: LoggingEvent)  {
    val message = subAppend(event)
    LogLog.debug("[" + new Date(event.getTimeStamp).toString + "]" + message)
    producer.send(new ProducerRecord(topic, message.getBytes()));
  }

  def subAppend(event: LoggingEvent): String = {
    if(this.layout == null)
      event.getRenderedMessage
    else
      this.layout.format(event)
  }

  override def close() {
    if(!this.closed) {
      this.closed = true
      producer.close()
    }
  }

  override def requiresLayout: Boolean = false
}
