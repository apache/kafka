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
import org.apache.log4j.{Logger, AppenderSkeleton}
import kafka.utils.{Utils, Logging}
import kafka.serializer.Encoder
import java.util.{Properties, Date}
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}

class KafkaLog4jAppender extends AppenderSkeleton with Logging {
  var port:Int = 0
  var host:String = null
  var topic:String = null
  var encoderClass:String = null
  
  private var producer:SyncProducer = null
  private var encoder: Encoder[AnyRef] = null
  
  def getPort:Int = port
  def setPort(port: Int) = { this.port = port }

  def getHost:String = host
  def setHost(host: String) = { this.host = host }

  def getTopic:String = topic
  def setTopic(topic: String) = { this.topic = topic }

  def getEncoder:String = encoderClass
  def setEncoder(encoder: String) = { this.encoderClass = encoder }
  
  override def activateOptions = {
    // check for config parameter validity
    if(host == null)
      throw new MissingConfigException("Broker Host must be specified by the Kafka log4j appender")
    if(port == 0)
      throw new MissingConfigException("Broker Port must be specified by the Kafka log4j appender") 
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(encoderClass == null) {
      info("Using default encoder - kafka.producer.DefaultStringEncoder")
      encoder = Utils.getObject("kafka.producer.DefaultStringEncoder")
    }else // instantiate the encoder, if present
      encoder = Utils.getObject(encoderClass)
    val props = new Properties()
    props.put("host", host)
    props.put("port", port.toString)
    producer = new SyncProducer(new SyncProducerConfig(props))
    info("Kafka producer connected to " + host + "," + port)
    info("Logging for topic: " + topic)
  }
  
  override def append(event: LoggingEvent) = {
    debug("[" + new Date(event.getTimeStamp).toString + "]" + event.getRenderedMessage +
            " for " + host + "," + port)
    val message = encoder.toMessage(event)
    producer.send(topic, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = message))
  }

  override def close = {
    if(!this.closed) {
      this.closed = true
      producer.close
    }
  }

  override def requiresLayout: Boolean = false
}

class DefaultStringEncoder extends Encoder[LoggingEvent] {
  override def toMessage(event: LoggingEvent):Message = new Message(event.getMessage.asInstanceOf[String].getBytes)
}
