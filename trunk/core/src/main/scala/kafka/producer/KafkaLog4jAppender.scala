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
import scala.collection._

class KafkaLog4jAppender extends AppenderSkeleton with Logging {
  var port:Int = 0
  var host:String = null
  var topic:String = null
  var serializerClass:String = null
  var zkConnect:String = null
  var brokerList:String = null
  
  private var producer: Producer[String, String] = null

  def getTopic:String = topic
  def setTopic(topic: String) { this.topic = topic }

  def getZkConnect:String = zkConnect
  def setZkConnect(zkConnect: String) { this.zkConnect = zkConnect }
  
  def getBrokerList:String = brokerList
  def setBrokerList(brokerList: String) { this.brokerList = brokerList }
  
  def getSerializerClass:String = serializerClass
  def setSerializerClass(serializerClass:String) { this.serializerClass = serializerClass }

  override def activateOptions() {
    val connectDiagnostic : mutable.ListBuffer[String] = mutable.ListBuffer();
    // check for config parameter validity
    val props = new Properties()
    if( zkConnect == null) connectDiagnostic += "zkConnect"
    else props.put("zk.connect", zkConnect);
    if( brokerList == null) connectDiagnostic += "brokerList"
    else if( props.isEmpty) props.put("broker.list", brokerList)
    if(props.isEmpty )
      throw new MissingConfigException(
        connectDiagnostic mkString ("One of these connection properties must be specified: ", ", ", ".")
      )
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(serializerClass == null) {
      serializerClass = "kafka.serializer.StringEncoder"
      LogLog.warn("Using default encoder - kafka.serializer.StringEncoder")
    }
    props.put("serializer.class", serializerClass)
    val config : ProducerConfig = new ProducerConfig(props)
    producer = new Producer[String, String](config)
    LogLog.debug("Kafka producer connected to " + (if(config.zkConnect == null) config.brokerList else config.zkConnect))
    LogLog.debug("Logging for topic: " + topic)
  }
  
  override def append(event: LoggingEvent)  {
    val message : String = if( this.layout == null) {
      event.getRenderedMessage
    }
    else this.layout.format(event)
    LogLog.debug("[" + new Date(event.getTimeStamp).toString + "]" + message)
    val messageData : ProducerData[String, String] =
      new ProducerData[String, String](topic, message)
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
