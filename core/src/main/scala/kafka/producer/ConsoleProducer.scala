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
import kafka.message._
import kafka.serializer._

object ConsoleProducer { 

  def main(args: Array[String]) { 
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The zookeeper connection string for the kafka zookeeper instance in the form HOST:PORT[/CHROOT].")
                           .withRequiredArg
                           .describedAs("connection_string")
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
    val messageEncoderOpt = parser.accepts("message-encoder", "The class name of the message encoder implementation to use.")
                                 .withRequiredArg
                                 .describedAs("encoder_class")
                                 .ofType(classOf[java.lang.String])
                                 .defaultsTo(classOf[StringEncoder].getName)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " + 
                                                          "By default each line is read as a seperate message.")
                                  .withRequiredArg
                                  .describedAs("reader_class")
                                  .ofType(classOf[java.lang.String])
                                  .defaultsTo(classOf[LineMessageReader].getName)
    val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " + 
                                                 "This allows custom configuration for a user-defined message reader.")
                            .withRequiredArg
                            .describedAs("prop")
                            .ofType(classOf[String])


    val options = parser.parse(args : _*)
    for(arg <- List(topicOpt, zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val sync = options.has(syncOpt)
    val compress = options.has(compressOpt)
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val encoderClass = options.valueOf(messageEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val cmdLineProps = parseLineReaderArgs(options.valuesOf(propertyOpt))

    val props = new Properties()
    props.put("zk.connect", zkConnect)
    props.put("compression.codec", DefaultCompressionCodec.codec.toString)
    props.put("producer.type", if(sync) "sync" else "async")
    if(options.has(batchSizeOpt))
      props.put("batch.size", batchSize.toString)
    props.put("queue.time", sendTimeout.toString)
    props.put("serializer.class", encoderClass)

    val reader = Class.forName(readerClass).newInstance().asInstanceOf[MessageReader]
    reader.init(System.in, cmdLineProps)

    val producer = new Producer[Any, Any](new ProducerConfig(props))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        producer.close()
      }
    })

    var message: AnyRef = null
    do { 
      message = reader.readMessage()
      if(message != null)
        producer.send(new ProducerData(topic, message))
    } while(message != null)
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

  trait MessageReader { 
    def init(inputStream: InputStream, props: Properties) {}
    def readMessage(): AnyRef
    def close() {}
  }

  class LineMessageReader extends MessageReader { 
    var reader: BufferedReader = null

    override def init(inputStream: InputStream, props: Properties) { 
      reader = new BufferedReader(new InputStreamReader(inputStream))
    }

    override def readMessage() = reader.readLine()
  }
}
