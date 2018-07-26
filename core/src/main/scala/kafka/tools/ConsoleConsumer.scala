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
import java.util.concurrent.CountDownLatch
import java.util.regex.Pattern
import java.util.{Collections, Locale, Properties, Random}

import com.typesafe.scalalogging.LazyLogging
import joptsimple._
import kafka.common.MessageFormatter
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{AuthenticationException, TimeoutException, WakeupException}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

/**
 * Consumer that dumps messages to standard out.
 */
object ConsoleConsumer extends Logging {

  var messageCount = 0

  private val shutdownLatch = new CountDownLatch(1)

  def main(args: Array[String]) {
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

  def run(conf: ConsumerConfig) {
    val timeoutMs = if (conf.timeoutMs >= 0) conf.timeoutMs else Long.MaxValue
    val consumer = new KafkaConsumer(consumerProps(conf), new ByteArrayDeserializer, new ByteArrayDeserializer)
    val consumerWrapper =
      if (conf.partitionArg.isDefined)
        new ConsumerWrapper(Option(conf.topicArg), conf.partitionArg, Option(conf.offsetArg), None, consumer, timeoutMs)
      else
        new ConsumerWrapper(Option(conf.topicArg), None, None, Option(conf.whitelistArg), consumer, timeoutMs)

    addShutdownHook(consumerWrapper, conf)

    try process(conf.maxMessages, conf.formatter, consumerWrapper, System.out, conf.skipMessageOnError)
    finally {
      consumerWrapper.cleanup()
      conf.formatter.close()
      reportRecordCount()

      shutdownLatch.countDown()
    }
  }

  def addShutdownHook(consumer: ConsumerWrapper, conf: ConsumerConfig) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        consumer.wakeup()

        shutdownLatch.await()

        if (conf.enableSystestEventsLogging) {
          System.out.println("shutdown_complete")
        }
      }
    })
  }

  def process(maxMessages: Integer, formatter: MessageFormatter, consumer: ConsumerWrapper, output: PrintStream,
              skipMessageOnError: Boolean) {
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
        formatter.writeTo(new ConsumerRecord(msg.topic, msg.partition, msg.offset, msg.timestamp,
                                             msg.timestampType, 0, 0, 0, msg.key, msg.value, msg.headers), output)
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

  def reportRecordCount() {
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
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.isolationLevel)
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
  def setAutoOffsetResetValue(config: ConsumerConfig, props: Properties) {
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

  class ConsumerConfig(args: Array[String]) {
    val parser = new OptionParser(false)
    val topicIdOpt = parser.accepts("topic", "The topic id to consume on.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val whitelistOpt = parser.accepts("whitelist", "Whitelist of topics to include for consumption.")
      .withRequiredArg
      .describedAs("whitelist")
      .ofType(classOf[String])
    val partitionIdOpt = parser.accepts("partition", "The partition to consume from. Consumption " +
      "starts from the end of the partition unless '--offset' is specified.")
      .withRequiredArg
      .describedAs("partition")
      .ofType(classOf[java.lang.Integer])
    val offsetOpt = parser.accepts("offset", "The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end")
      .withRequiredArg
      .describedAs("consume offset")
      .ofType(classOf[String])
      .defaultsTo("latest")
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("consumer_prop")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file. Note that ${consumerPropertyOpt} takes precedence over this config.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property",
      "The properties to initialize the message formatter. Default properties include:\n" +
        "\tprint.timestamp=true|false\n" +
        "\tprint.key=true|false\n" +
        "\tprint.value=true|false\n" +
        "\tkey.separator=<key.separator>\n" +
        "\tline.separator=<line.separator>\n" +
        "\tkey.deserializer=<key.deserializer>\n" +
        "\tvalue.deserializer=<value.deserializer>\n" +
        "\nUsers can also pass in customized properties for their formatter; more specifically, users " +
        "can pass in properties keyed with \'key.deserializer.\' and \'value.deserializer.\' prefixes to configure their deserializers.")
      .withRequiredArg
      .describedAs("prop")
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
      "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted" +
        "to read all messages.")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo("read_uncommitted")

    val groupIdOpt = parser.accepts("group", "The consumer group id of the consumer.")
      .withRequiredArg
      .describedAs("consumer group id")
      .ofType(classOf[String])

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "The console consumer is a tool that reads data from Kafka and outputs it to standard output.")

    var groupIdPassed = true
    val options: OptionSet = tryParse(parser, args)
    val enableSystestEventsLogging = options.has(enableSystestEventsLoggingOpt)

    // topic must be specified.
    var topicArg: String = null
    var whitelistArg: String = null
    var filterSpec: TopicFilter = null
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt).asScala)
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val fromBeginning = options.has(resetBeginningOpt)
    val partitionArg = if (options.has(partitionIdOpt)) Some(options.valueOf(partitionIdOpt).intValue) else None
    val skipMessageOnError = options.has(skipMessageOnErrorOpt)
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt).asScala)
    val maxMessages = if (options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val keyDeserializer = options.valueOf(keyDeserializerOpt)
    val valueDeserializer = options.valueOf(valueDeserializerOpt)
    val isolationLevel = options.valueOf(isolationLevelOpt).toString
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]

    if (keyDeserializer != null && !keyDeserializer.isEmpty) {
      formatterArgs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    }
    if (valueDeserializer != null && !valueDeserializer.isEmpty) {
      formatterArgs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    }

    formatter.init(formatterArgs)

    val topicOrFilterOpt = List(topicIdOpt, whitelistOpt).filter(options.has)
    if (topicOrFilterOpt.size != 1)
      CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/topic is required.")
    topicArg = options.valueOf(topicIdOpt)
    whitelistArg = options.valueOf(whitelistOpt)

    if (partitionArg.isDefined) {
      if (!options.has(topicIdOpt))
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
          case "earliest" => ListOffsetRequest.EARLIEST_TIMESTAMP
          case "latest" => ListOffsetRequest.LATEST_TIMESTAMP
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
      else if (fromBeginning) ListOffsetRequest.EARLIEST_TIMESTAMP
      else ListOffsetRequest.LATEST_TIMESTAMP

    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

    // if the group id is provided in more than place (through different means) all values must be the same
    val groupIdsProvided = Set(
      Option(options.valueOf(groupIdOpt)), // via --group
      Option(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)), // via --consumer-property
      Option(extraConsumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)) // via --cosumer.config
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

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }
  }

  private[tools] class ConsumerWrapper(topic: Option[String], partitionId: Option[Int], offset: Option[Long], whitelist: Option[String],
                                       consumer: Consumer[Array[Byte], Array[Byte]], val timeoutMs: Long = Long.MaxValue) {
    consumerInit()
    var recordIter = consumer.poll(0).iterator

    def consumerInit() {
      (topic, partitionId, offset, whitelist) match {
        case (Some(topic), Some(partitionId), Some(offset), None) =>
          seek(topic, partitionId, offset)
        case (Some(topic), Some(partitionId), None, None) =>
          // default to latest if no offset is provided
          seek(topic, partitionId, ListOffsetRequest.LATEST_TIMESTAMP)
        case (Some(topic), None, None, None) =>
          consumer.subscribe(Collections.singletonList(topic))
        case (None, None, None, Some(whitelist)) =>
          consumer.subscribe(Pattern.compile(whitelist))
        case _ =>
          throw new IllegalArgumentException("An invalid combination of arguments is provided. " +
            "Exactly one of 'topic' or 'whitelist' must be provided. " +
            "If 'topic' is provided, an optional 'partition' may also be provided. " +
            "If 'partition' is provided, an optional 'offset' may also be provided, otherwise, consumption starts from the end of the partition.")
      }
    }

    def seek(topic: String, partitionId: Int, offset: Long) {
      val topicPartition = new TopicPartition(topic, partitionId)
      consumer.assign(Collections.singletonList(topicPartition))
      offset match {
        case ListOffsetRequest.EARLIEST_TIMESTAMP => consumer.seekToBeginning(Collections.singletonList(topicPartition))
        case ListOffsetRequest.LATEST_TIMESTAMP => consumer.seekToEnd(Collections.singletonList(topicPartition))
        case _ => consumer.seek(topicPartition, offset)
      }
    }

    def resetUnconsumedOffsets() {
      val smallestUnconsumedOffsets = collection.mutable.Map[TopicPartition, Long]()
      while (recordIter.hasNext) {
        val record = recordIter.next()
        val tp = new TopicPartition(record.topic, record.partition)
        // avoid auto-committing offsets which haven't been consumed
        smallestUnconsumedOffsets.getOrElseUpdate(tp, record.offset)
      }
      smallestUnconsumedOffsets.foreach { case (tp, offset) => consumer.seek(tp, offset) }
    }

    def receive(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      if (!recordIter.hasNext) {
        recordIter = consumer.poll(timeoutMs).iterator
        if (!recordIter.hasNext)
          throw new TimeoutException()
      }

      recordIter.next
    }

    def wakeup(): Unit = {
      this.consumer.wakeup()
    }

    def cleanup() {
      resetUnconsumedOffsets()
      this.consumer.close()
    }

    def commitSync() {
      this.consumer.commitSync()
    }
  }
}

class DefaultMessageFormatter extends MessageFormatter {
  var printKey = false
  var printValue = true
  var printTimestamp = false
  var keySeparator = "\t".getBytes(StandardCharsets.UTF_8)
  var lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)

  var keyDeserializer: Option[Deserializer[_]] = None
  var valueDeserializer: Option[Deserializer[_]] = None

  override def init(props: Properties) {
    if (props.containsKey("print.timestamp"))
      printTimestamp = props.getProperty("print.timestamp").trim.equalsIgnoreCase("true")
    if (props.containsKey("print.key"))
      printKey = props.getProperty("print.key").trim.equalsIgnoreCase("true")
    if (props.containsKey("print.value"))
      printValue = props.getProperty("print.value").trim.equalsIgnoreCase("true")
    if (props.containsKey("key.separator"))
      keySeparator = props.getProperty("key.separator").getBytes(StandardCharsets.UTF_8)
    if (props.containsKey("line.separator"))
      lineSeparator = props.getProperty("line.separator").getBytes(StandardCharsets.UTF_8)
    // Note that `toString` will be called on the instance returned by `Deserializer.deserialize`
    if (props.containsKey("key.deserializer")) {
      keyDeserializer = Some(Class.forName(props.getProperty("key.deserializer")).newInstance().asInstanceOf[Deserializer[_]])
      keyDeserializer.get.configure(propertiesWithKeyPrefixStripped("key.deserializer.", props).asScala.asJava, true)
    }
    // Note that `toString` will be called on the instance returned by `Deserializer.deserialize`
    if (props.containsKey("value.deserializer")) {
      valueDeserializer = Some(Class.forName(props.getProperty("value.deserializer")).newInstance().asInstanceOf[Deserializer[_]])
      valueDeserializer.get.configure(propertiesWithKeyPrefixStripped("value.deserializer.", props).asScala.asJava, false)
    }
  }

  private def propertiesWithKeyPrefixStripped(prefix: String, props: Properties): Properties = {
    val newProps = new Properties()
    props.asScala.foreach { case (key, value) =>
      if (key.startsWith(prefix) && key.length > prefix.length)
        newProps.put(key.substring(prefix.length), value)
    }
    newProps
  }

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {

    def writeSeparator(columnSeparator: Boolean): Unit = {
      if (columnSeparator)
        output.write(keySeparator)
      else
        output.write(lineSeparator)
    }

    def write(deserializer: Option[Deserializer[_]], sourceBytes: Array[Byte]) {
      val nonNullBytes = Option(sourceBytes).getOrElse("null".getBytes(StandardCharsets.UTF_8))
      val convertedBytes = deserializer.map(_.deserialize(null, nonNullBytes).toString.
        getBytes(StandardCharsets.UTF_8)).getOrElse(nonNullBytes)
      output.write(convertedBytes)
    }

    import consumerRecord._

    if (printTimestamp) {
      if (timestampType != TimestampType.NO_TIMESTAMP_TYPE)
        output.write(s"$timestampType:$timestamp".getBytes(StandardCharsets.UTF_8))
      else
        output.write(s"NO_TIMESTAMP".getBytes(StandardCharsets.UTF_8))
      writeSeparator(printKey || printValue)
    }

    if (printKey) {
      write(keyDeserializer, key)
      writeSeparator(printValue)
    }

    if (printValue) {
      write(valueDeserializer, value)
      output.write(lineSeparator)
    }
  }
}

class LoggingMessageFormatter extends MessageFormatter with LazyLogging {
  private val defaultWriter: DefaultMessageFormatter = new DefaultMessageFormatter

  override def init(props: Properties): Unit = defaultWriter.init(props)

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    import consumerRecord._
    defaultWriter.writeTo(consumerRecord, output)
    logger.info({if (timestampType != TimestampType.NO_TIMESTAMP_TYPE) s"$timestampType:$timestamp, " else ""} +
                  s"key:${if (key == null) "null" else new String(key, StandardCharsets.UTF_8)}, " +
                  s"value:${if (value == null) "null" else new String(value, StandardCharsets.UTF_8)}")
  }
}

class NoOpMessageFormatter extends MessageFormatter {
  override def init(props: Properties) {}

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream){}
}

class ChecksumMessageFormatter extends MessageFormatter {
  private var topicStr: String = _

  override def init(props: Properties) {
    topicStr = props.getProperty("topic")
    if (topicStr != null)
      topicStr = topicStr + ":"
    else
      topicStr = ""
  }

  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
    output.println(topicStr + "checksum:" + consumerRecord.checksum)
  }
}
