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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.regex.Pattern
import joptsimple.{OptionException, OptionParser, OptionSet}
import kafka.common._
import kafka.message._
import kafka.utils.Implicits._
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, ToolsUtils}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils
import scala.jdk.CollectionConverters._

object ConsoleProducer {

  def main(args: Array[String]): Unit = {

    try {
        val config = new ProducerConfig(args)
        val reader = Class.forName(config.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]
        reader.init(System.in, getReaderProps(config))

        val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps(config))

    Exit.addShutdownHook("producer-shutdown-hook", producer.close)

        var record: ProducerRecord[Array[Byte], Array[Byte]] = null
        do {
          record = reader.readMessage()
          if (record != null)
            send(producer, record, config.sync)
        } while (record != null)
    } catch {
      case e: joptsimple.OptionException =>
        System.err.println(e.getMessage)
        Exit.exit(1)
      case e: Exception =>
        e.printStackTrace
        Exit.exit(1)
    }
    Exit.exit(0)
  }

  private def send(producer: KafkaProducer[Array[Byte], Array[Byte]],
                         record: ProducerRecord[Array[Byte], Array[Byte]], sync: Boolean): Unit = {
    if (sync)
      producer.send(record).get()
    else
      producer.send(record, new ErrorLoggingCallback(record.topic, record.key, record.value, false))
  }

  def getReaderProps(config: ProducerConfig): Properties = {
    val props = new Properties
    props.put("topic", config.topic)
    props ++= config.cmdLineProps
    props
  }

  def producerProps(config: ProducerConfig): Properties = {
    val props =
      if (config.options.has(config.producerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.producerConfigOpt))
      else new Properties

    props ++= config.extraProducerProps

    if (config.bootstrapServer != null)
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    else
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)

    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null)
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.LINGER_MS_CONFIG, config.options, config.sendTimeoutOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.ACKS_CONFIG, config.options, config.requestRequiredAcksOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.options, config.requestTimeoutMsOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.RETRIES_CONFIG, config.options, config.messageSendMaxRetriesOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.options, config.retryBackoffMsOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.SEND_BUFFER_CONFIG, config.options, config.socketBufferSizeOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.BUFFER_MEMORY_CONFIG, config.options, config.maxMemoryBytesOpt)
    // We currently have 2 options to set the batch.size value. We'll deprecate/remove one of them in KIP-717.
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.BATCH_SIZE_CONFIG, config.options, config.batchSizeOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.BATCH_SIZE_CONFIG, config.options, config.maxPartitionMemoryBytesOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.METADATA_MAX_AGE_CONFIG, config.options, config.metadataExpiryMsOpt)
    CommandLineUtils.maybeMergeOptions(
      props, ProducerConfig.MAX_BLOCK_MS_CONFIG, config.options, config.maxBlockMsOpt)

    props
  }

  class ProducerConfig(args: Array[String]) extends CommandDefaultOptions(args) {
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified.  The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to. The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .requiredUnless("broker-list")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or 'zstd'." +
                                                                  "If specified without value, then it defaults to 'gzip'")
                                    .withOptionalArg()
                                    .describedAs("compression-codec")
                                    .ofType(classOf[String])
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously. "+
       "please note that this option will be replaced if max-partition-memory-bytes is also set")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(16 * 1024)
    val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, " +
      "and being unavailable transiently is just one of them. This property specifies the number of retries before the producer give up and drop this message. " +
      "This is the option to control `retries` in producer configs.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. " +
      "Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata. " +
      "This is the option to control `retry.backoff.ms` in producer configs.")
      .withRequiredArg
      .ofType(classOf[java.lang.Long])
      .defaultsTo(100)
    val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms. " +
      "This is the option to control `linger.ms` in producer configs.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(1000)
    val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required `acks` of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.String])
      .defaultsTo("-1")
    val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero.")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500)
    val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes. " +
        "This is the option to control `metadata.max.age.ms` in producer configs.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5*60*1000L)
    val maxBlockMsOpt = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request.")
      .withRequiredArg
      .describedAs("max block on send")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60*1000L)
    val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server. " +
        "This is the option to control `buffer.memory` in producer configs.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached. " +
        "This is the option to control `batch.size` in producer configs.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(16 * 1024)
    val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size. " +
      "This is the option to control `send.buffer.bytes` in producer configs.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024*100)
    val propertyOpt = parser.accepts("property",
      """A mechanism to pass user-defined properties in the form key=value to the message reader. This allows custom configuration for a user-defined message reader.
        |Default properties include:
        | parse.key=false
        | parse.headers=false
        | ignore.error=false
        | key.separator=\t
        | headers.delimiter=\t
        | headers.separator=,
        | headers.key.separator=:
        | null.marker=   When set, any fields (key, value and headers) equal to this will be replaced by null
        |Default parsing pattern when:
        | parse.headers=true and parse.key=true:
        |  "h1:v1,h2:v2...\tkey\tvalue"
        | parse.key=true:
        |  "key\tvalue"
        | parse.headers=true:
        |  "h1:v1,h2:v2...\tvalue"
      """.stripMargin
      )
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

    options = tryParse(parser, args)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to read data from standard input and publish it to Kafka.")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

    val topic = options.valueOf(topicOpt)

    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val brokerList = options.valueOf(brokerListOpt)

    val brokerHostsAndPorts = options.valueOf(if (options.has(bootstrapServerOpt)) bootstrapServerOpt else brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerHostsAndPorts)

    val sync = options.has(syncOpt)
    val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec = if (options.has(compressionCodecOpt))
                             if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
                               DefaultCompressionCodec.name
                             else compressionCodecOptionValue
                           else NoCompressionCodec.name
    val readerClass = options.valueOf(messageReaderOpt)
    val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt).asScala)
    val extraProducerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt).asScala)

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }
  }

  class LineMessageReader extends MessageReader {
    var topic: String = null
    var reader: BufferedReader = null
    var parseKey = false
    var keySeparator = "\t"
    var parseHeaders = false
    var headersDelimiter = "\t"
    var headersSeparator = ","
    var headersKeySeparator = ":"
    var ignoreError = false
    var lineNumber = 0
    var printPrompt = System.console != null
    var headersSeparatorPattern: Pattern = _
    var nullMarker: String = _

    override def init(inputStream: InputStream, props: Properties): Unit = {
      topic = props.getProperty("topic")
      if (props.containsKey("parse.key"))
        parseKey = props.getProperty("parse.key").trim.equalsIgnoreCase("true")
      if (props.containsKey("key.separator"))
        keySeparator = props.getProperty("key.separator")
      if (props.containsKey("parse.headers"))
        parseHeaders = props.getProperty("parse.headers").trim.equalsIgnoreCase("true")
      if (props.containsKey("headers.delimiter"))
        headersDelimiter = props.getProperty("headers.delimiter")
      if (props.containsKey("headers.separator"))
        headersSeparator = props.getProperty("headers.separator")
      headersSeparatorPattern = Pattern.compile(headersSeparator)
      if (props.containsKey("headers.key.separator"))
        headersKeySeparator = props.getProperty("headers.key.separator")
      if (props.containsKey("ignore.error"))
        ignoreError = props.getProperty("ignore.error").trim.equalsIgnoreCase("true")
      if (headersDelimiter == headersSeparator)
        throw new KafkaException("headers.delimiter and headers.separator may not be equal")
      if (headersDelimiter == headersKeySeparator)
        throw new KafkaException("headers.delimiter and headers.key.separator may not be equal")
      if (headersSeparator == headersKeySeparator)
        throw new KafkaException("headers.separator and headers.key.separator may not be equal")
      if (props.containsKey("null.marker"))
        nullMarker = props.getProperty("null.marker")
      if (nullMarker == keySeparator)
        throw new KafkaException("null.marker and key.separator may not be equal")
      if (nullMarker == headersSeparator)
        throw new KafkaException("null.marker and headers.separator may not be equal")
      if (nullMarker == headersDelimiter)
        throw new KafkaException("null.marker and headers.delimiter may not be equal")
      if (nullMarker == headersKeySeparator)
        throw new KafkaException("null.marker and headers.key.separator may not be equal")
      reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    }

    override def readMessage(): ProducerRecord[Array[Byte], Array[Byte]] = {
      lineNumber += 1
      if (printPrompt) print(">")
      val line = reader.readLine()
      line match {
        case null => null
        case line =>
          val headers = parse(parseHeaders, line, 0, headersDelimiter, "headers delimiter")
          val headerOffset = if (headers == null) 0 else headers.length + headersDelimiter.length

          val key = parse(parseKey, line, headerOffset, keySeparator, "key separator")
          val keyOffset = if (key == null) 0 else key.length + keySeparator.length

          val value = line.substring(headerOffset + keyOffset)

          val record = new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            if (key != null && key != nullMarker) key.getBytes(StandardCharsets.UTF_8) else null,
            if (value != null && value != nullMarker) value.getBytes(StandardCharsets.UTF_8) else null,
          )

          if (headers != null && headers != nullMarker) {
            splitHeaders(headers)
              .foreach(header => record.headers.add(header._1, header._2))
          }

          record
      }
    }

    private def parse(enabled: Boolean, line: String, startIndex: Int, demarcation: String, demarcationName: String): String = {
      (enabled, line.indexOf(demarcation, startIndex)) match {
        case (false, _) => null
        case (_, -1) =>
          if (ignoreError) null
          else throw new KafkaException(s"No $demarcationName found on line number $lineNumber: '$line'")
        case (_, index) => line.substring(startIndex, index)
      }
    }

    private def splitHeaders(headers: String): Array[(String, Array[Byte])] = {
      headersSeparatorPattern.split(headers).map { pair =>
        (pair.indexOf(headersKeySeparator), ignoreError) match {
          case (-1, false) => throw new KafkaException(s"No header key separator found in pair '$pair' on line number $lineNumber")
          case (-1, true) => (pair, null)
          case (i, _) =>
            val headerKey = pair.substring(0, i) match {
              case k if k == nullMarker =>
                throw new KafkaException(s"Header keys should not be equal to the null marker '$nullMarker' as they can't be null")
              case k => k
            }
            val headerValue = pair.substring(i + headersKeySeparator.length) match {
              case v if v == nullMarker => null
              case v => v.getBytes(StandardCharsets.UTF_8)
            }
            (headerKey, headerValue)
        }
      }
    }
  }
}
