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

import scala.collection.JavaConversions._
import joptsimple._
import java.util.Properties
import java.io._
import kafka.common._
import kafka.message._
import kafka.serializer._

object ConsoleProducer { 

  def main(args: Array[String]) { 
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                           .withRequiredArg
                           .describedAs("broker-list")
                           .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressOpt = parser.accepts("compress", "If set, messages batches are sent compressed")
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
                             .withRequiredArg
                             .describedAs("size")
                             .ofType(classOf[java.lang.Integer])
                             .defaultsTo(200)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" + 
                                                   " a message will queue awaiting suffient batch size. The value is given in ms.")
                               .withRequiredArg
                               .describedAs("timeout_ms")
                               .ofType(classOf[java.lang.Long])
                               .defaultsTo(1000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " + 
                                                   " messages will queue awaiting suffient batch size.")
                               .withRequiredArg
                               .describedAs("queue_size")
                               .ofType(classOf[java.lang.Long])
                               .defaultsTo(10000)
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
                               .withRequiredArg
                               .describedAs("queue enqueuetimeout ms")
                               .ofType(classOf[java.lang.Long])
                               .defaultsTo(0)
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
                               .withRequiredArg
                               .describedAs("request required acks")
                               .ofType(classOf[java.lang.Integer])
                               .defaultsTo(0)
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
                               .withRequiredArg
                               .describedAs("request timeout ms")
                               .ofType(classOf[java.lang.Integer])
                               .defaultsTo(1500)
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
                                 .withRequiredArg
                                 .describedAs("encoder_class")
                                 .ofType(classOf[java.lang.String])
                                 .defaultsTo(classOf[StringEncoder].getName)
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
                                 .withRequiredArg
                                 .describedAs("encoder_class")
                                 .ofType(classOf[java.lang.String])
                                 .defaultsTo(classOf[StringEncoder].getName)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " + 
                                                          "By default each line is read as a separate message.")
                                  .withRequiredArg
                                  .describedAs("reader_class")
                                  .ofType(classOf[java.lang.String])
                                  .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                                  .withRequiredArg
                                  .describedAs("size")
                                  .ofType(classOf[java.lang.Integer])
                                  .defaultsTo(1024*100)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
                                                 "This allows custom configuration for a user-defined message reader.")
                            .withRequiredArg
                            .describedAs("prop")
                            .ofType(classOf[String])


    val options = parser.parse(args : _*)
    for(arg <- List(topicOpt, brokerListOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    val sync = options.has(syncOpt)
    val compress = options.has(compressOpt)
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val queueSize = options.valueOf(queueSizeOpt)
    val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
    val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
    val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
    val keyEncoderClass = options.valueOf(keyEncoderOpt)
    val valueEncoderClass = options.valueOf(valueEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val socketBuffer = options.valueOf(socketBufferSizeOpt)
    val cmdLineProps = parseLineReaderArgs(options.valuesOf(propertyOpt))
    cmdLineProps.put("topic", topic)

    val props = new Properties()
    props.put("broker.list", brokerList)
    val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
    props.put("compression.codec", codec.toString)
    props.put("producer.type", if(sync) "sync" else "async")
    if(options.has(batchSizeOpt))
      props.put("batch.num.messages", batchSize.toString)
    props.put("queue.buffering.max.ms", sendTimeout.toString)
    props.put("queue.buffering.max.messages", queueSize.toString)
    props.put("queue.enqueue.timeout.ms", queueEnqueueTimeoutMs.toString)
    props.put("request.required.acks", requestRequiredAcks.toString)
    props.put("request.timeout.ms", requestTimeoutMs.toString)
    props.put("key.serializer.class", keyEncoderClass)
    props.put("serializer.class", valueEncoderClass)
    props.put("send.buffer.bytes", socketBuffer.toString)
    val reader = Class.forName(readerClass).newInstance().asInstanceOf[MessageReader[AnyRef, AnyRef]]
    reader.init(System.in, cmdLineProps)

    try {
        val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run() {
            producer.close()
          }
        })

        var message: KeyedMessage[AnyRef, AnyRef] = null
        do {
          message = reader.readMessage()
          if(message != null)
            producer.send(message)
        } while(message != null)
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
    System.exit(0)
  }

  def parseLineReaderArgs(args: Iterable[String]): Properties = {
    val splits = args.map(_ split "=").filterNot(_ == null).filterNot(_.length == 0)
    if(!splits.forall(_.length == 2)) {
      System.err.println("Invalid line reader properties: " + args.mkString(" "))
      System.exit(1)
    }
    val props = new Properties
    for(a <- splits)
      props.put(a(0), a(1))
    props
  }

  trait MessageReader[K,V] { 
    def init(inputStream: InputStream, props: Properties) {}
    def readMessage(): KeyedMessage[K,V]
    def close() {}
  }

  class LineMessageReader extends MessageReader[String, String] {
    var topic: String = null
    var reader: BufferedReader = null
    var parseKey = false
    var keySeparator = "\t"
    var ignoreError = false
    var lineNumber = 0

    override def init(inputStream: InputStream, props: Properties) {
      topic = props.getProperty("topic")
      if(props.containsKey("parse.key"))
        parseKey = props.getProperty("parse.key").trim.toLowerCase.equals("true")
      if(props.containsKey("key.seperator"))
        keySeparator = props.getProperty("key.separator")
      if(props.containsKey("ignore.error"))
        ignoreError = props.getProperty("ignore.error").trim.toLowerCase.equals("true")
      reader = new BufferedReader(new InputStreamReader(inputStream))
    }

    override def readMessage() = {
      lineNumber += 1
      (reader.readLine(), parseKey) match {
        case (null, _) => null
        case (line, true) =>
          line.indexOf(keySeparator) match {
            case -1 =>
              if(ignoreError)
                new KeyedMessage(topic, line)
              else
                throw new KafkaException("No key found on line " + lineNumber + ": " + line)
            case n =>
              new KeyedMessage(topic,
                             line.substring(0, n), 
                             if(n + keySeparator.size > line.size) "" else line.substring(n + keySeparator.size))
          }
        case (line, false) =>
          new KeyedMessage(topic, line)
      }
    }
  }
}
