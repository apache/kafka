/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.regex.Pattern
import java.util.{Collections, Locale, Map, Optional, Properties, Random}
import com.typesafe.scalalogging.LazyLogging
import joptsimple._
import kafka.utils.Implicits._
import kafka.utils.{Exit, _}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{MessageFormatter, TopicPartition}
import org.apache.kafka.common.errors.{AuthenticationException, TimeoutException, WakeupException}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._

/**
 * Consumer that dumps messages to standard out.
 */
object ConsoleConsumer extends Logging {

  var messageCount = 0

  private val shutdownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    val conf = new ConsumerConfig(args)
    try {
      run(conf)
    } catch {
      case e: AuthenticationException =>
        error("Authentication failed: terminating consumer process", e)
        Exit.exit(1)
      case e: Throwable =>
        error("Unknown error when running consumer: ", e)
        Exit.exit(1)
    }
  }

  def run(conf: ConsumerConfig): Unit = {
    val timeoutMs = if (conf.timeoutMs >= 0) conf.timeoutMs.toLong else Long.MaxValue
    val consumer = new KafkaConsumer(consumerProps(conf), new ByteArrayDeserializer, new ByteArrayDeserializer)

    val consumerWrapper =
      if (conf.partitionArg.isDefined)
        new ConsumerWrapper(Option(conf.topicArg), conf.partitionArg, Option(conf.offsetArg), None, consumer, timeoutMs)
      else
        new ConsumerWrapper(Option(conf.topicArg), None, None, Option(conf.includedTopicsArg), consumer, timeoutMs)

    addShutdownHook(consumerWrapper, conf)

    try process(conf.maxMessages, conf.formatter, consumerWrapper, System.out, conf.skipMessageOnError)
    finally {
      consumerWrapper.cleanup()
      conf.formatter.close()
      reportRecordCount()

      shutdownLatch.countDown()
    }
  }

  def addShutdownHook(consumer: ConsumerWrapper, conf: ConsumerConfig): Unit = {
    Exit.addShutdownHook("consumer-shutdown-hook", {
        consumer.wakeup()

        shutdownLatch.await()

        if (conf.enableSystestEventsLogging) {
          System.out.println("shutdown_complete")
        }
    })
  }

  def process(maxMessages: Integer, formatter: MessageFormatter, consumer: ConsumerWrapper, output: PrintStream,
              skipMessageOnError: Boolean): Unit = {
    while (messageCount < maxMessages || maxMessages == -1) {
      val msg: ConsumerRecord[Array[Byte], Array[Byte]] = try {
        consumer.receive()
      } catch {
        case _: WakeupException =>
          trace("Caught WakeupException because consumer is shutdown, ignore and terminate.")
          // Consumer will be closed
          return
        case e: Throwable =>
          error("Error processing message, terminating consumer process: ", e)
          // Consumer will be closed
          return
      }
      messageCount += 1
      try {
        formatter.writeTo(new ConsumerRecord(msg.topic, msg.partition, msg.offset, msg.timestamp, msg.timestampType,
          0, 0, msg.key, msg.value, msg.headers, Optional.empty[Integer]), output)
      } catch {
        case e: Throwable =>
          if (skipMessageOnError) {
            error("Error processing message, skipping this message: ", e)
          } else {
            // Consumer will be closed
            throw e
          }
      }
      if (checkErr(output, formatter)) {
        // Consumer will be closed
        return
      }
    }
  }

  def reportRecordCount(): Unit = {
    System.err.println(s"Processed a total of $messageCount messages")
  }

  def checkErr(output: PrintStream, formatter: MessageFormatter): Boolean = {
    val gotError = output.checkError()
    if (gotError) {
      // This means no one is listening to our output stream anymore, time to shutdown
      System.err.println("Unable to write to standard out, closing consumer.")
    }
    gotError
  }

  private[tools] def consumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties
    props ++= config.consumerProps
    props ++= config.extraConsumerProps
    setAutoOffsetResetValue(config, props)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, "console-consumer")
    CommandLineUtils.maybeMergeOptions(
      props, ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.options, config.isolationLevelOpt)
    props
  }

  /**
    * Used by consumerProps to retrieve the correct value for the consumer parameter 'auto.offset.reset'.
    *
    * Order of priority is:
    *   1. Explicitly set parameter via --consumer.property command line parameter
    *   2. Explicit --from-beginning given -> 'earliest'
    *   3. Default value of 'latest'
    *
    * In case both --from-beginning and an explicit value are specified an error is thrown if these
    * are conflicting.
    */
  def setAutoOffsetResetValue(config: ConsumerConfig, props: Properties): Unit = {
    val (earliestConfigValue, latestConfigValue) = ("earliest", "latest")

    if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      // auto.offset.reset parameter was specified on the command line
      val autoResetOption = props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      if (config.options.has(config.resetBeginningOpt) && earliestConfigValue != autoResetOption) {
        // conflicting options - latest und earliest, throw an error
        System.err.println(s"Can't simultaneously specify --from-beginning and 'auto.offset.reset=$autoResetOption', " +
          "please remove one option")
        Exit.exit(1)
      }
      // nothing to do, checking for valid parameter values happens later and the specified
      // value was already copied during .putall operation
    } else {
      // no explicit value for auto.offset.reset was specified
      // if --from-beginning was specified use earliest, otherwise default to latest
      val autoResetOption = if (config.options.has(config.resetBeginningOpt)) earliestConfigValue else latestConfigValue
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoResetOption)
    }
  }

  class ConsumerConfig(args: Array[String]) extends CommandDefaultOptions(args) {
    val topicOpt = parser.accepts("topic", "The topic to consume on.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val whitelistOpt = parser.accepts("whitelist",
      "DEPRECATED, use --include instead; ignored if --include specified. Regular expression specifying list of topics to include for consumption.")
      .withRequiredArg
      .describedAs("Java regex (String)")
      .ofType(classOf[String])
    val includeOpt = parser.accepts("include",
      "Regular expression specifying list of topics to include for consumption.")
      .withRequiredArg
      .describedAs("Java regex (String)")
      .ofType(classOf[String])
    val partitionIdOpt = parser.accepts("partition", "The partition to consume from. Consumption " +
      "starts from the end of the partition unless '--offset' is specified.")
      .withRequiredArg
      .describedAs("partition")
      .ofType(classOf[java.lang.Integer])
    val offsetOpt = parser.accepts("offset", "The offset to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end")
      .withRequiredArg
      .describedAs("consume offset")
      .ofType(classOf[String])
      .defaultsTo("latest")
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("consumer_prop")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file. Note that $consumerPropertyOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property",
    """The properties to initialize the message formatter. Default properties include:
      | print.timestamp=true|false
      | print.key=true|false
      | print.offset=true|false
      | print.partition=true|false
      | print.headers=true|false
      | print.value=true|false
      | key.separator=<key.separator>
      | line.separator=<line.separator>
      | headers.separator=<line.separator>
      | null.literal=<null.literal>
      | key.deserializer=<key.deserializer>
      | value.deserializer=<value.deserializer>
      | header.deserializer=<header.deserializer>
      |
      |Users can also pass in customized properties for their formatter; more specifically, users can pass in properties keyed with 'key.deserializer.', 'value.deserializer.' and 'headers.deserializer.' prefixes to configure their deserializers."""
      .stripMargin)
      .withRequiredArg
      .describedAs("prop")
      .ofType(classOf[String])
    val messageFormatterConfigOpt = parser.accepts("formatter-config", s"Config properties file to initialize the message formatter. Note that $messageFormatterArgOpt takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
      "start with the earliest message present in the log rather than the latest message.")
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
      .withRequiredArg
      .describedAs("num_messages")
      .ofType(classOf[java.lang.Integer])
    val timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
      .withRequiredArg
      .describedAs("timeout_ms")
      .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
      "skip it instead of halt.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val keyDeserializerOpt = parser.accepts("key-deserializer")
      .withRequiredArg
      .describedAs("deserializer for key")
      .ofType(classOf[String])
    val valueDeserializerOpt = parser.accepts("value-deserializer")
      .withRequiredArg
      .describedAs("deserializer for values")
      .ofType(classOf[String])
    val enableSystestEventsLoggingOpt = parser.accepts("enable-systest-events",
      "Log lifecycle events of the consumer in addition to logging consumed " +
        "messages. (This is specific for system tests.)")
    val isolationLevelOpt = parser.accepts("isolation-level",
      "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted " +
        "to read all messages.")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo("read_uncommitted")

    val groupIdOpt = parser.accepts("group", "The consumer group id of the consumer.")
      .withRequiredArg
      .describedAs("consumer group id")
      .ofType(classOf[String])

    options = tryParse(parser, args)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to read data from Kafka topics and outputs it to standard output.")

    var groupIdPassed = true
    val enableSystestEventsLogging = options.has(enableSystestEventsLoggingOpt)

    // topic must be specified.
    var topicArg: String = _
    var includedTopicsArg: String = _
    var filterSpec: TopicFilter = _
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt).asScala)
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val fromBeginning = options.has(resetBeginningOpt)
    val partitionArg = if (options.has(partitionIdOpt)) Some(options.valueOf(partitionIdOpt).intValue) else None
    val skipMessageOnError = options.has(skipMessageOnErrorOpt)
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = if (options.has(messageFormatterConfigOpt))
      Utils.loadProps(options.valueOf(messageFormatterConfigOpt))
    else
      new Properties()
    formatterArgs ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt).asScala)
    val maxMessages = if (options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val keyDeserializer = options.valueOf(keyDeserializerOpt)
    val valueDeserializer = options.valueOf(valueDeserializerOpt)
    val formatter: MessageFormatter = messageFormatterClass.getDeclaredConstructor().newInstance().asInstanceOf[MessageFormatter]

    if (keyDeserializer != null && keyDeserializer.nonEmpty) {
      formatterArgs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    }
    if (valueDeserializer != null && valueDeserializer.nonEmpty) {
      formatterArgs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    }

    formatter.configure(formatterArgs.asScala.asJava)

    topicArg = options.valueOf(topicOpt)
    includedTopicsArg = if (options.has(includeOpt))
      options.valueOf(includeOpt)
    else
      options.valueOf(whitelistOpt)

    val topicOrFilterArgs = List(topicArg, includedTopicsArg).filterNot(_ == null)
    // user need to specify value for either --topic or one of the include filters options (--include or --whitelist)
    if (topicOrFilterArgs.size != 1)
      CommandLineUtils.printUsageAndDie(parser, s"Exactly one of --include/--topic is required. " +
        s"${if (options.has(whitelistOpt)) "--whitelist is DEPRECATED use --include instead; ignored if --include specified."}")

    if (partitionArg.isDefined) {
      if (!options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "The topic is required when partition is specified.")
      if (fromBeginning && options.has(offsetOpt))
        CommandLineUtils.printUsageAndDie(parser, "Options from-beginning and offset cannot be specified together.")
    } else if (options.has(offsetOpt))
      CommandLineUtils.printUsageAndDie(parser, "The partition is required when offset is specified.")

    def invalidOffset(offset: String): Nothing =
      CommandLineUtils.printUsageAndDie(parser, s"The provided offset value '$offset' is incorrect. Valid values are " +
        "'earliest', 'latest', or a non-negative long.")

    val offsetArg =
      if (options.has(offsetOpt)) {
        options.valueOf(offsetOpt).toLowerCase(Locale.ROOT) match {
          case "earliest" => ListOffsetsRequest.EARLIEST_TIMESTAMP
          case "latest" => ListOffsetsRequest.LATEST_TIMESTAMP
          case offsetString =>
            try {
              val offset = offsetString.toLong
              if (offset < 0)
                invalidOffset(offsetString)
              offset
            } catch {
              case _: NumberFormatException => invalidOffset(offsetString)
            }
        }
      }
      else if (fromBeginning) ListOffsetsRequest.EARLIEST_TIMESTAMP
      else ListOffsetsRequest.LATEST_TIMESTAMP

    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

    // if the group id is provided in more than place (through different means) all values must be the same
    val groupIdsProvided = Set(
      Option(options.valueOf(groupIdOpt)), // via --group
      Option(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)), // via --consumer-property
      Option(extraConsumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)) // via --consumer.config
    ).flatten

    if (groupIdsProvided.size > 1) {
      CommandLineUtils.printUsageAndDie(parser, "The group ids provided in different places (directly using '--group', "
        + "via '--consumer-property', or via '--consumer.config') do not match. "
        + s"Detected group ids: ${groupIdsProvided.mkString("'", "', '", "'")}")
    }

    groupIdsProvided.headOption match {
      case Some(group) =>
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group)
      case None =>
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, s"console-consumer-${new Random().nextInt(100000)}")
        // By default, avoid unnecessary expansion of the coordinator cache since
        // the auto-generated group and its offsets is not intended to be used again
        if (!consumerProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        groupIdPassed = false
    }

    if (groupIdPassed && partitionArg.isDefined)
      CommandLineUtils.printUsageAndDie(parser, "Options group and partition cannot be specified together.")

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }
  }

  private[tools] class ConsumerWrapper(
    topic: Option[String],
    partitionId: Option[Int],
    offset: Option[Long],
    includedTopics: Option[String],
    consumer: Consumer[Array[Byte], Array[Byte]],
    timeoutMs: Long = Long.MaxValue,
    time: Time = Time.SYSTEM
  ) {
    consumerInit()
    var recordIter = Collections.emptyList[ConsumerRecord[Array[Byte], Array[Byte]]]().iterator()

    def consumerInit(): Unit = {
      (topic, partitionId, offset, includedTopics) match {
        case (Some(topic), Some(partitionId), Some(offset), None) =>
          seek(topic, partitionId, offset)
        case (Some(topic), Some(partitionId), None, None) =>
          // default to latest if no offset is provided
          seek(topic, partitionId, ListOffsetsRequest.LATEST_TIMESTAMP)
        case (Some(topic), None, None, None) =>
          consumer.subscribe(Collections.singletonList(topic))
        case (None, None, None, Some(include)) =>
          consumer.subscribe(Pattern.compile(include))
        case _ =>
          throw new IllegalArgumentException("An invalid combination of arguments is provided. " +
            "Exactly one of 'topic' or 'include' must be provided. " +
            "If 'topic' is provided, an optional 'partition' may also be provided. " +
            "If 'partition' is provided, an optional 'offset' may also be provided, otherwise, consumption starts from the end of the partition.")
      }
    }

    def seek(topic: String, partitionId: Int, offset: Long): Unit = {
      val topicPartition = new TopicPartition(topic, partitionId)
      consumer.assign(Collections.singletonList(topicPartition))
      offset match {
        case ListOffsetsRequest.EARLIEST_TIMESTAMP => consumer.seekToBeginning(Collections.singletonList(topicPartition))
        case ListOffsetsRequest.LATEST_TIMESTAMP => consumer.seekToEnd(Collections.singletonList(topicPartition))
        case _ => consumer.seek(topicPartition, offset)
      }
    }

    def resetUnconsumedOffsets(): Unit = {
      val smallestUnconsumedOffsets = collection.mutable.Map[TopicPartition, Long]()
      while (recordIter.hasNext) {
        val record = recordIter.next()
        val tp = new TopicPartition(record.topic, record.partition)
        // avoid auto-committing offsets which haven't been consumed
        smallestUnconsumedOffsets.getOrElseUpdate(tp, record.offset)
      }
      smallestUnconsumedOffsets.forKeyValue { (tp, offset) => consumer.seek(tp, offset) }
    }

    def receive(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      val startTimeMs = time.milliseconds
      while (!recordIter.hasNext) {
        recordIter = consumer.poll(Duration.ofMillis(timeoutMs)).iterator
        if (!recordIter.hasNext && (time.milliseconds - startTimeMs > timeoutMs)) {
          throw new TimeoutException()
        }
      }

      recordIter.next
    }

    def wakeup(): Unit = {
      this.consumer.wakeup()
    }

    def cleanup(): Unit = {
      resetUnconsumedOffsets()
      this.consumer.close()
    }

  }
}

class DefaultMessageFormatter extends MessageFormatter {
  var printTimestamp = false
  var printKey = false
  var printValue = true
  var printPartition = false
  var printOffset = false
  var printHeaders = false
  var keySeparator = utfBytes("\t")
  var lineSeparator = utfBytes("\n")
  var headersSeparator = utfBytes(",")
  var nullLiteral = utfBytes("null")

  var keyDeserializer: Option[Deserializer[_]] = None
  var valueDeserializer: Option[Deserializer[_]] = None
  var headersDeserializer: Option[Deserializer[_]] = None

  override def configure(configs: Map[String, _]): Unit = {
    getPropertyIfExists(configs, "print.timestamp", getBoolProperty).foreach(printTimestamp = _)
    getPropertyIfExists(configs, "print.key", getBoolProperty).foreach(printKey = _)
    getPropertyIfExists(configs, "print.offset", getBoolProperty).foreach(printOffset = _)
    getPropertyIfExists(configs, "print.partition", getBoolProperty).foreach(printPartition = _)
    getPropertyIfExists(configs, "print.headers", getBoolProperty).foreach(printHeaders = _)
    getPropertyIfExists(configs, "print.value", getBoolProperty).foreach(printValue = _)
    getPropertyIfExists(configs, "key.separator", getByteProperty).foreach(keySeparator = _)
    getPropertyIfExists(configs, "line.separator", getByteProperty).foreach(lineSeparator = _)
    getPropertyIfExists(configs, "headers.separator", getByteProperty).foreach(headersSeparator = _)
    getPropertyIfExists(configs, "null.literal", getByteProperty).foreach(nullLiteral = _)

    keyDeserializer = getPropertyIfExists(configs, "key.deserializer", getDeserializerProperty(true))
    valueDeserializer = getPropertyIfExists(configs, "value.deserializer", getDeserializerProperty(false))
    headersDeserializer = getPropertyIfExists(configs, "headers.deserializer", getDeserializerProperty(false))
  }

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {

    def writeSeparator(columnSeparator: Boolean): Unit = {
      if (columnSeparator)
        output.write(keySeparator)
      else
        output.write(lineSeparator)
    }

    def deserialize(deserializer: Option[Deserializer[_]], sourceBytes: Array[Byte], topic: String) = {
      val nonNullBytes = Option(sourceBytes).getOrElse(nullLiteral)
      val convertedBytes = deserializer
        .map(d => utfBytes(d.deserialize(topic, consumerRecord.headers, nonNullBytes).toString))
        .getOrElse(nonNullBytes)
      convertedBytes
    }

    import consumerRecord._

    if (printTimestamp) {
      if (timestampType != TimestampType.NO_TIMESTAMP_TYPE)
        output.write(utfBytes(s"$timestampType:$timestamp"))
      else
        output.write(utfBytes("NO_TIMESTAMP"))
      writeSeparator(columnSeparator =  printOffset || printPartition || printHeaders || printKey || printValue)
    }

    if (printPartition) {
      output.write(utfBytes("Partition:"))
      output.write(utfBytes(partition().toString))
      writeSeparator(columnSeparator = printOffset || printHeaders || printKey || printValue)
    }

    if (printOffset) {
      output.write(utfBytes("Offset:"))
      output.write(utfBytes(offset().toString))
      writeSeparator(columnSeparator = printHeaders || printKey || printValue)
    }

    if (printHeaders) {
      val headersIt = headers().iterator.asScala
      if (headersIt.hasNext) {
        headersIt.foreach { header =>
          output.write(utfBytes(header.key() + ":"))
          output.write(deserialize(headersDeserializer, header.value(), topic))
          if (headersIt.hasNext) {
            output.write(headersSeparator)
          }
        }
      } else {
        output.write(utfBytes("NO_HEADERS"))
      }
      writeSeparator(columnSeparator = printKey || printValue)
    }

    if (printKey) {
      output.write(deserialize(keyDeserializer, key, topic))
      writeSeparator(columnSeparator = printValue)
    }

    if (printValue) {
      output.write(deserialize(valueDeserializer, value, topic))
      output.write(lineSeparator)
    }
  }

  private def propertiesWithKeyPrefixStripped(prefix: String, configs: Map[String, _]): Map[String, _] = {
    val newConfigs = collection.mutable.Map[String, Any]()
    configs.asScala.foreach { case (key, value) =>
      if (key.startsWith(prefix) && key.length > prefix.length)
        newConfigs.put(key.substring(prefix.length), value)
    }
    newConfigs.asJava
  }

  private def utfBytes(str: String) = str.getBytes(StandardCharsets.UTF_8)

  private def getByteProperty(configs: Map[String, _], key: String): Array[Byte] = {
    utfBytes(configs.get(key).asInstanceOf[String])
  }

  private def getBoolProperty(configs: Map[String, _], key: String): Boolean = {
    configs.get(key).asInstanceOf[String].trim.equalsIgnoreCase("true")
  }

  private def getDeserializerProperty(isKey: Boolean)(configs: Map[String, _], propertyName: String): Deserializer[_] = {
    val deserializer = Class.forName(configs.get(propertyName).asInstanceOf[String]).getDeclaredConstructor().newInstance().asInstanceOf[Deserializer[_]]
    val deserializerConfig = propertiesWithKeyPrefixStripped(propertyName + ".", configs)
      .asScala
      .asJava
    deserializer.configure(deserializerConfig, isKey)
    deserializer
  }

  private def getPropertyIfExists[T](configs: Map[String, _], key: String, getter: (Map[String, _], String) => T): Option[T] = {
    if (configs.containsKey(key))
      Some(getter(configs, key))
    else
      None
  }
}

class LoggingMessageFormatter extends MessageFormatter with LazyLogging {
  private val defaultWriter: DefaultMessageFormatter = new DefaultMessageFormatter

  override def configure(configs: Map[String, _]): Unit = defaultWriter.configure(configs)

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    import consumerRecord._
    defaultWriter.writeTo(consumerRecord, output)
    logger.info({if (timestampType != TimestampType.NO_TIMESTAMP_TYPE) s"$timestampType:$timestamp, " else ""} +
                  s"key:${if (key == null) "null" else new String(key, StandardCharsets.UTF_8)}, " +
                  s"value:${if (value == null) "null" else new String(value, StandardCharsets.UTF_8)}")
  }
}

class NoOpMessageFormatter extends MessageFormatter {

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {}
}

