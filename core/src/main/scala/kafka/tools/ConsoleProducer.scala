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

package kafka.tools

import kafka.common._
import kafka.message._
import kafka.serializer._
import kafka.utils.{CommandLineUtils, ToolsUtils}
import kafka.producer.{NewShinyProducer, OldProducer}
import java.util.Properties
import java.io._

import joptsimple._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.utils.Utils

object ConsoleProducer {

  def main(args: Array[String]) {

    try {
        val config = new ProducerConfig(args)
        val reader = Class.forName(config.readerClass).newInstance().asInstanceOf[MessageReader]
        reader.init(System.in, getReaderProps(config))

        val producer =
          if(config.useOldProducer) {
            new OldProducer(getOldProducerProps(config))
          } else {
            new NewShinyProducer(getNewProducerProps(config))
          }

        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run() {
            producer.close()
          }
        })

        var message: ProducerRecord[Array[Byte], Array[Byte]] = null
        do {
          message = reader.readMessage()
          if (message != null)
            producer.send(message.topic, message.key, message.value)
        } while (message != null)
    } catch {
      case e: joptsimple.OptionException =>
        System.err.println(e.getMessage)
        System.exit(1)
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
    System.exit(0)
  }

  def getReaderProps(config: ProducerConfig): Properties = {
    val props = new Properties
    props.put("topic",config.topic)
    props.putAll(config.cmdLineProps)
    props
  }

  def getOldProducerProps(config: ProducerConfig): Properties = {
    val props = producerProps(config)

    props.put("metadata.broker.list", config.brokerList)
    props.put("compression.codec", config.compressionCodec)
    props.put("producer.type", if(config.sync) "sync" else "async")
    props.put("batch.num.messages", config.batchSize.toString)
    props.put("message.send.max.retries", config.messageSendMaxRetries.toString)
    props.put("retry.backoff.ms", config.retryBackoffMs.toString)
    props.put("queue.buffering.max.ms", config.sendTimeout.toString)
    props.put("queue.buffering.max.messages", config.queueSize.toString)
    props.put("queue.enqueue.timeout.ms", config.queueEnqueueTimeoutMs.toString)
    props.put("request.required.acks", config.requestRequiredAcks.toString)
    props.put("request.timeout.ms", config.requestTimeoutMs.toString)
    props.put("key.serializer.class", config.keyEncoderClass)
    props.put("serializer.class", config.valueEncoderClass)
    props.put("send.buffer.bytes", config.socketBuffer.toString)
    props.put("topic.metadata.refresh.interval.ms", config.metadataExpiryMs.toString)
    props.put("client.id", "console-producer")

    props
  }

  private def producerProps(config: ProducerConfig): Properties = {
    val props =
      if (config.options.has(config.producerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.producerConfigOpt))
      else new Properties
    props.putAll(config.extraProducerProps)
    props
  }

  def getNewProducerProps(config: ProducerConfig): Properties = {
    val props = producerProps(config)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.socketBuffer.toString)
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs.toString)
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.metadataExpiryMs.toString)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.maxBlockMs.toString)
    props.put(ProducerConfig.ACKS_CONFIG, config.requestRequiredAcks.toString)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs.toString)
    props.put(ProducerConfig.RETRIES_CONFIG, config.messageSendMaxRetries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.sendTimeout.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.maxMemoryBytes.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.maxPartitionMemoryBytes.toString)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    props
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
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
                                                                  "If specified without value, then it defaults to 'gzip'")
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
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)
    val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
      " messages will queue awaiting sufficient batch size.")
      .withRequiredArg
      .describedAs("queue_size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000)
    val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
      .withRequiredArg
      .describedAs("queue enqueuetimeout ms")
      .ofType(classOf[java.lang.Integer])
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
    val maxBlockMsOpt = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request")
      .withRequiredArg
      .describedAs("max block on send")
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
    val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
            .withRequiredArg
            .describedAs("producer_prop")
            .ofType(classOf[String])
    val producerConfigOpt = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val useOldProducerOpt = parser.accepts("old-producer", "Use the old producer implementation.")

    val options = parser.parse(args : _*)
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, brokerListOpt)

    import scala.collection.JavaConversions._
    val useOldProducer = options.has(useOldProducerOpt)
    val topic = options.valueOf(topicOpt)
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser,brokerList)
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
    val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt))
    val extraProducerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt))
    /* new producer related configs */
    val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
    val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
    val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
    val maxBlockMs = options.valueOf(maxBlockMsOpt)
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
      if (props.containsKey("parse.key"))
        parseKey = props.getProperty("parse.key").trim.equalsIgnoreCase("true")
      if (props.containsKey("key.separator"))
        keySeparator = props.getProperty("key.separator")
      if (props.containsKey("ignore.error"))
        ignoreError = props.getProperty("ignore.error").trim.equalsIgnoreCase("true")
      reader = new BufferedReader(new InputStreamReader(inputStream))
    }

    override def readMessage() = {
      lineNumber += 1
      (reader.readLine(), parseKey) match {
        case (null, _) => null
        case (line, true) =>
          line.indexOf(keySeparator) match {
            case -1 =>
              if (ignoreError) new ProducerRecord(topic, line.getBytes)
              else throw new KafkaException(s"No key found on line ${lineNumber}: $line")
            case n =>
              val value = (if (n + keySeparator.size > line.size) "" else line.substring(n + keySeparator.size)).getBytes
              new ProducerRecord(topic, line.substring(0, n).getBytes, value)
          }
        case (line, false) =>
          new ProducerRecord(topic, line.getBytes)
      }
    }
  }
}
