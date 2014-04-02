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

import joptsimple._
import java.util.Properties
import java.io._
import kafka.common._
import kafka.message._
import kafka.serializer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import kafka.utils.{CommandLineUtils, Utils}

object ConsoleProducer { 

  def main(args: Array[String]) { 

    val config = new ProducerConfig(args)
    val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[MessageReader]
    reader.init(System.in, config.cmdLineProps)

    try {
        val producer =
          if(config.useNewProducer) new NewShinyProducer(config)
          else new OldProducer(config)

        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run() {
            producer.close()
          }
        })

        var message: KeyedMessage[Array[Byte], Array[Byte]] = null
        do {
          message = reader.readMessage()
          if(message != null)
            producer.send(message.topic, message.key, message.message)
        } while(message != null)
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
    System.exit(0)
  }

  class ProducerConfig(args: Array[String]) {
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
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'gzip' or 'snappy'." +
                                                                  "If specified without value, than it defaults to 'gzip'")
                                    .withOptionalArg()
                                    .describedAs("compression-codec")
                                    .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
      .withRequiredArg
      .ofType(classOf[java.lang.Long])
      .defaultsTo(100)
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
      .defaultsTo(Int.MaxValue)
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
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5*60*1000L)
    val metadataFetchTimeoutMsOpt = parser.accepts("metadata-fetch-timeout-ms",
      "The amount of time to block waiting to fetch metadata about a topic the first time a record is sent to that topic.")
      .withRequiredArg
      .describedAs("metadata fetch timeout")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60*1000L)
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(16 * 1024L)
    val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
    val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
      .withRequiredArg
      .describedAs("encoder_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[DefaultEncoder].getName)
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
    val useNewProducerOpt = parser.accepts("new-producer", "Use the new producer implementation.")

    val options = parser.parse(args : _*)
    for(arg <- List(topicOpt, brokerListOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    import scala.collection.JavaConversions._
    val useNewProducer = options.has(useNewProducerOpt)
    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    val sync = options.has(syncOpt)
    val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec = if (options.has(compressionCodecOpt))
                             if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
                               DefaultCompressionCodec.name
                             else compressionCodecOptionValue
                           else NoCompressionCodec.name
    val batchSize = options.valueOf(batchSizeOpt)
    val sendTimeout = options.valueOf(sendTimeoutOpt)
    val queueSize = options.valueOf(queueSizeOpt)
    val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
    val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
    val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
    val messageSendMaxRetries = options.valueOf(messageSendMaxRetriesOpt)
    val retryBackoffMs = options.valueOf(retryBackoffMsOpt)
    val keyEncoderClass = options.valueOf(keyEncoderOpt)
    val valueEncoderClass = options.valueOf(valueEncoderOpt)
    val readerClass = options.valueOf(messageReaderOpt)
    val socketBuffer = options.valueOf(socketBufferSizeOpt)
    val cmdLineProps = CommandLineUtils.parseCommandLineArgs(options.valuesOf(propertyOpt))
    cmdLineProps.put("topic", topic)
    /* new producer related configs */
    val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
    val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
    val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
    val metadataFetchTimeoutMs = options.valueOf(metadataFetchTimeoutMsOpt)
  }

  trait MessageReader {
    def init(inputStream: InputStream, props: Properties) {}
    def readMessage(): KeyedMessage[Array[Byte], Array[Byte]]
    def close() {}
  }

  class LineMessageReader extends MessageReader {
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
      if(props.containsKey("key.separator"))
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
                new KeyedMessage[Array[Byte], Array[Byte]](topic, line.getBytes())
              else
                throw new KafkaException("No key found on line " + lineNumber + ": " + line)
            case n =>
              new KeyedMessage[Array[Byte], Array[Byte]](topic,
                             line.substring(0, n).getBytes,
                             (if(n + keySeparator.size > line.size) "" else line.substring(n + keySeparator.size)).getBytes())
          }
        case (line, false) =>
          new KeyedMessage[Array[Byte], Array[Byte]](topic, line.getBytes())
      }
    }
  }

  trait Producer {
    def send(topic: String, key: Array[Byte], bytes: Array[Byte])
    def close()
  }

  class NewShinyProducer(producerConfig: ProducerConfig) extends Producer {
    val props = new Properties()
    props.put("metadata.broker.list", producerConfig.brokerList)
    props.put("compression.type", producerConfig.compressionCodec)
    props.put("send.buffer.bytes", producerConfig.socketBuffer.toString)
    props.put("metadata.fetch.backoff.ms", producerConfig.retryBackoffMs.toString)
    props.put("metadata.expiry.ms", producerConfig.metadataExpiryMs.toString)
    props.put("metadata.fetch.timeout.ms", producerConfig.metadataFetchTimeoutMs.toString)
    props.put("request.required.acks", producerConfig.requestRequiredAcks.toString)
    props.put("request.timeout.ms", producerConfig.requestTimeoutMs.toString)
    props.put("request.retries", producerConfig.messageSendMaxRetries.toString)
    props.put("linger.ms", producerConfig.sendTimeout.toString)
    if(producerConfig.queueEnqueueTimeoutMs != -1)
      props.put("block.on.buffer.full", "false")
    props.put("total.memory.bytes", producerConfig.maxMemoryBytes.toString)
    props.put("max.partition.bytes", producerConfig.maxPartitionMemoryBytes.toString)
    props.put("client.id", "console-producer")
    val producer = new KafkaProducer(props)

    def send(topic: String, key: Array[Byte], bytes: Array[Byte]) {
      val response = this.producer.send(new ProducerRecord(topic, key, bytes))
      if(producerConfig.sync) {
        response.get()
      }
    }

    def close() {
      this.producer.close()
    }
  }

  class OldProducer(producerConfig: ConsoleProducer.ProducerConfig) extends Producer {
    val props = new Properties()
    props.put("metadata.broker.list", producerConfig.brokerList)
    props.put("compression.codec", producerConfig.compressionCodec)
    props.put("producer.type", if(producerConfig.sync) "sync" else "async")
    props.put("batch.num.messages", producerConfig.batchSize.toString)
    props.put("message.send.max.retries", producerConfig.messageSendMaxRetries.toString)
    props.put("retry.backoff.ms", producerConfig.retryBackoffMs.toString)
    props.put("queue.buffering.max.ms", producerConfig.sendTimeout.toString)
    props.put("queue.buffering.max.messages", producerConfig.queueSize.toString)
    props.put("queue.enqueue.timeout.ms", producerConfig.queueEnqueueTimeoutMs.toString)
    props.put("request.required.acks", producerConfig.requestRequiredAcks.toString)
    props.put("request.timeout.ms", producerConfig.requestTimeoutMs.toString)
    props.put("key.serializer.class", producerConfig.keyEncoderClass)
    props.put("serializer.class", producerConfig.valueEncoderClass)
    props.put("send.buffer.bytes", producerConfig.socketBuffer.toString)
    props.put("topic.metadata.refresh.interval.ms", producerConfig.metadataExpiryMs.toString)
    props.put("client.id", "console-producer")
    val producer = new kafka.producer.Producer[Array[Byte], Array[Byte]](new kafka.producer.ProducerConfig(props))

    def send(topic: String, key: Array[Byte], bytes: Array[Byte]) {
      this.producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, key, bytes))
    }

    def close() {
      this.producer.close()
    }
  }
}
