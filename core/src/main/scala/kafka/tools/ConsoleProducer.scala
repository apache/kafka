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
import joptsimple.{OptionException, OptionParser, OptionSet, OptionSpec}
import kafka.common.MessageReader
import kafka.utils.Implicits._
import kafka.utils.{Exit, Logging, ToolsUtils}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}
import org.apache.kafka.tools.api.RecordReader

import java.lang
import scala.annotation.nowarn

@nowarn("cat=deprecation")
object ConsoleProducer extends Logging {

  private[tools] def newReader(className: String, prop: Properties): RecordReader = {
    val reader = Class.forName(className).getDeclaredConstructor().newInstance()
    reader match {
      case r: RecordReader =>
        r.configure(prop.asInstanceOf[java.util.Map[String, _]])
        r
      case r: MessageReader =>
        logger.warn("MessageReader is deprecated. Please use org.apache.kafka.tools.api.RecordReader instead")
        new RecordReader {
          private[this] var initialized = false

          override def readRecords(inputStream: InputStream): java.util.Iterator[ProducerRecord[Array[Byte], Array[Byte]]] = {
            if (initialized) throw new IllegalStateException("It is invalid to call readRecords again when the reader is based on deprecated MessageReader")
            if (!initialized) {
              r.init(inputStream, prop)
              initialized = true
            }
            new java.util.Iterator[ProducerRecord[Array[Byte], Array[Byte]]] {
              private[this] var current: ProducerRecord[Array[Byte], Array[Byte]] = _
              // a flag used to avoid accessing readMessage again after it does return null
              private[this] var done: Boolean = false

              override def hasNext: Boolean = {
                if (current != null) true
                else if (done) false
                else {
                  current = r.readMessage()
                  done = current == null
                  !done
                }
              }

              override def next(): ProducerRecord[Array[Byte], Array[Byte]] =
                try if (hasNext) current
                else throw new NoSuchElementException("no more records from input stream")
                finally current = null
            }
          }
          override def close(): Unit = r.close()
        }
      case _ => throw new IllegalArgumentException(f"the reader must extend ${classOf[RecordReader].getName}")
    }
  }

  private[tools] def loopReader(producer: Producer[Array[Byte], Array[Byte]],
                               reader: RecordReader,
                                inputStream: InputStream,
                               sync: Boolean): Unit = {
    val iter = reader.readRecords(inputStream)
    try while (iter.hasNext) send(producer, iter.next(), sync) finally reader.close()
  }

  def main(args: Array[String]): Unit = {

    try {
      val config = new ProducerConfig(args)
      val input = System.in
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps(config))
      try loopReader(producer, newReader(config.readerClass, getReaderProps(config)), input, config.sync)
      finally producer.close()
      Exit.exit(0)
    } catch {
      case e: joptsimple.OptionException =>
        System.err.println(e.getMessage)
        Exit.exit(1)
      case e: Exception =>
        e.printStackTrace()
        Exit.exit(1)
    }
  }

  private def send(producer: Producer[Array[Byte], Array[Byte]],
                         record: ProducerRecord[Array[Byte], Array[Byte]], sync: Boolean): Unit = {
    if (sync)
      producer.send(record).get()
    else
      producer.send(record, new ErrorLoggingCallback(record.topic, record.key, record.value, false))
  }

  def getReaderProps(config: ProducerConfig): Properties = {
    val props =
      if (config.options.has(config.readerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.readerConfigOpt))
      else new Properties
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
    val topicOpt: OptionSpec[String] = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    private val brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified.  The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val bootstrapServerOpt: OptionSpec[String]  = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to. The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .requiredUnless("broker-list")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    private val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
    val compressionCodecOpt: OptionSpec[String]  = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or 'zstd'." +
                                                                  "If specified without value, then it defaults to 'gzip'")
                                    .withOptionalArg()
                                    .describedAs("compression-codec")
                                    .ofType(classOf[String])
    val batchSizeOpt: OptionSpec[Integer] = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously. "+
       "please note that this option will be replaced if max-partition-memory-bytes is also set")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(16 * 1024)
    val messageSendMaxRetriesOpt: OptionSpec[Integer] = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, " +
      "and being unavailable transiently is just one of them. This property specifies the number of retries before the producer give up and drop this message. " +
      "This is the option to control `retries` in producer configs.")
      .withRequiredArg
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3)
    val retryBackoffMsOpt: OptionSpec[lang.Long] = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. " +
      "Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata. " +
      "This is the option to control `retry.backoff.ms` in producer configs.")
      .withRequiredArg
      .ofType(classOf[java.lang.Long])
      .defaultsTo(100)
    val sendTimeoutOpt: OptionSpec[lang.Long] = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
      " a message will queue awaiting sufficient batch size. The value is given in ms. " +
      "This is the option to control `linger.ms` in producer configs.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(1000)
    val requestRequiredAcksOpt: OptionSpec[String] = parser.accepts("request-required-acks", "The required `acks` of the producer requests")
      .withRequiredArg
      .describedAs("request required acks")
      .ofType(classOf[java.lang.String])
      .defaultsTo("-1")
    val requestTimeoutMsOpt: OptionSpec[Integer] = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero.")
      .withRequiredArg
      .describedAs("request timeout ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1500)
    val metadataExpiryMsOpt: OptionSpec[lang.Long] = parser.accepts("metadata-expiry-ms",
      "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes. " +
        "This is the option to control `metadata.max.age.ms` in producer configs.")
      .withRequiredArg
      .describedAs("metadata expiration interval")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(5*60*1000L)
    val maxBlockMsOpt: OptionSpec[lang.Long] = parser.accepts("max-block-ms",
      "The max time that the producer will block for during a send request.")
      .withRequiredArg
      .describedAs("max block on send")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(60*1000L)
    val maxMemoryBytesOpt: OptionSpec[lang.Long] = parser.accepts("max-memory-bytes",
      "The total memory used by the producer to buffer records waiting to be sent to the server. " +
        "This is the option to control `buffer.memory` in producer configs.")
      .withRequiredArg
      .describedAs("total memory in bytes")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(32 * 1024 * 1024L)
    val maxPartitionMemoryBytesOpt: OptionSpec[Integer] = parser.accepts("max-partition-memory-bytes",
      "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
        "will attempt to optimistically group them together until this size is reached. " +
        "This is the option to control `batch.size` in producer configs.")
      .withRequiredArg
      .describedAs("memory in bytes per partition")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(16 * 1024)
    private val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
      "By default each line is read as a separate message.")
      .withRequiredArg
      .describedAs("reader_class")
      .ofType(classOf[java.lang.String])
      .defaultsTo(classOf[LineMessageReader].getName)
    val socketBufferSizeOpt: OptionSpec[Integer] = parser.accepts("socket-buffer-size", "The size of the tcp RECV size. " +
      "This is the option to control `send.buffer.bytes` in producer configs.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024*100)
    private val propertyOpt = parser.accepts("property",
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
    val readerConfigOpt: OptionSpec[String] = parser.accepts("reader-config", s"Config properties file for the message reader. Note that $propertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    private val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
            .withRequiredArg
            .describedAs("producer_prop")
            .ofType(classOf[String])
    val producerConfigOpt: OptionSpec[String]  = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])

    options = tryParse(parser, args)

    CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from standard input and publish it to Kafka.")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

    val topic: String = options.valueOf(topicOpt)

    val bootstrapServer: String = options.valueOf(bootstrapServerOpt)
    val brokerList: String = options.valueOf(brokerListOpt)

    val brokerHostsAndPorts: String = options.valueOf(if (options.has(bootstrapServerOpt)) bootstrapServerOpt else brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerHostsAndPorts)

    val sync: Boolean = options.has(syncOpt)
    private val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
    val compressionCodec: String = if (options.has(compressionCodecOpt))
                             if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
                               CompressionType.GZIP.name
                             else compressionCodecOptionValue
                           else CompressionType.NONE.name
    val readerClass: String = options.valueOf(messageReaderOpt)
    val cmdLineProps: Properties = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt))
    val extraProducerProps: Properties = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt))

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          ToolsUtils.printUsageAndExit(parser, e.getMessage)
      }
    }
  }

  class LineMessageReader extends RecordReader {
    var topic: String = _
    var parseKey: Boolean = false
    var keySeparator: String = "\t"
    var parseHeaders: Boolean = false
    private var headersDelimiter = "\t"
    var headersSeparator: String = ","
    private var headersKeySeparator = ":"
    private var ignoreError = false
    private var lineNumber = 0
    private val printPrompt = System.console != null
    private var headersSeparatorPattern: Pattern = _
    private var nullMarker: String = _

    override def configure(props: java.util.Map[String, _]): Unit = {
      topic = props.get("topic").toString
      if (props.containsKey("parse.key"))
        parseKey = props.get("parse.key").toString.trim.equalsIgnoreCase("true")
      if (props.containsKey("key.separator"))
        keySeparator = props.get("key.separator").toString
      if (props.containsKey("parse.headers"))
        parseHeaders = props.get("parse.headers").toString.trim.equalsIgnoreCase("true")
      if (props.containsKey("headers.delimiter"))
        headersDelimiter = props.get("headers.delimiter").toString
      if (props.containsKey("headers.separator"))
        headersSeparator = props.get("headers.separator").toString
      headersSeparatorPattern = Pattern.compile(headersSeparator)
      if (props.containsKey("headers.key.separator"))
        headersKeySeparator = props.get("headers.key.separator").toString
      if (props.containsKey("ignore.error"))
        ignoreError = props.get("ignore.error").toString.trim.equalsIgnoreCase("true")
      if (headersDelimiter == headersSeparator)
        throw new KafkaException("headers.delimiter and headers.separator may not be equal")
      if (headersDelimiter == headersKeySeparator)
        throw new KafkaException("headers.delimiter and headers.key.separator may not be equal")
      if (headersSeparator == headersKeySeparator)
        throw new KafkaException("headers.separator and headers.key.separator may not be equal")
      if (props.containsKey("null.marker"))
        nullMarker = props.get("null.marker").toString
      if (nullMarker == keySeparator)
        throw new KafkaException("null.marker and key.separator may not be equal")
      if (nullMarker == headersSeparator)
        throw new KafkaException("null.marker and headers.separator may not be equal")
      if (nullMarker == headersDelimiter)
        throw new KafkaException("null.marker and headers.delimiter may not be equal")
      if (nullMarker == headersKeySeparator)
        throw new KafkaException("null.marker and headers.key.separator may not be equal")
    }

    override def readRecords(inputStream: InputStream): java.util.Iterator[ProducerRecord[Array[Byte], Array[Byte]]] =
      new java.util.Iterator[ProducerRecord[Array[Byte], Array[Byte]]] {
        private[this] val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
        private[this] var current: ProducerRecord[Array[Byte], Array[Byte]] = _
        override def hasNext: Boolean =
          if (current != null) true
          else {
            lineNumber += 1
            if (printPrompt) print(">")
            val line = reader.readLine()
            current = line match {
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
            current != null
          }

        override def next(): ProducerRecord[Array[Byte], Array[Byte]] = if (!hasNext) throw new NoSuchElementException("no more record")
        else try current finally current = null
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
