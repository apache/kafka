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
import java.util
import java.util.concurrent.CountDownLatch
import java.util.{Locale, Map, Properties, Random}

import com.typesafe.scalalogging.LazyLogging
import joptsimple._
import kafka.api.OffsetRequest
import kafka.common.{MessageFormatter, StreamEndException}
import kafka.consumer._
import kafka.message._
import kafka.metrics.KafkaMetricsReporter
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.errors.{AuthenticationException, WakeupException}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions
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

    val consumer =
      if (conf.useOldConsumer) {
        checkZk(conf)
        val props = getOldConsumerProps(conf)
        checkAndMaybeDeleteOldPath(conf, props)
        new OldConsumer(conf.filterSpec, props)
      } else {
        val timeoutMs = if (conf.timeoutMs >= 0) conf.timeoutMs else Long.MaxValue
        val consumer = new KafkaConsumer(getNewConsumerProps(conf), new ByteArrayDeserializer, new ByteArrayDeserializer)
        if (conf.partitionArg.isDefined)
          new NewShinyConsumer(Option(conf.topicArg), conf.partitionArg, Option(conf.offsetArg), None, consumer, timeoutMs)
        else
          new NewShinyConsumer(Option(conf.topicArg), None, None, Option(conf.whitelistArg), consumer, timeoutMs)
      }

    addShutdownHook(consumer, conf)

    try {
      process(conf.maxMessages, conf.formatter, consumer, System.out, conf.skipMessageOnError)
    } finally {
      consumer.cleanup()
      conf.formatter.close()
      reportRecordCount()

      // if we generated a random group id (as none specified explicitly) then avoid polluting zookeeper with persistent group data, this is a hack
      if (conf.useOldConsumer && !conf.groupIdPassed)
        ZkUtils.maybeDeletePath(conf.options.valueOf(conf.zkConnectOpt), "/consumers/" + conf.consumerProps.get("group.id"))

      shutdownLatch.countDown()
    }
  }

  def checkZk(config: ConsumerConfig) {
    if (!checkZkPathExists(config.options.valueOf(config.zkConnectOpt), "/brokers/ids")) {
      System.err.println("No brokers found in ZK.")
      Exit.exit(1)
    }

    if (!config.options.has(config.deleteConsumerOffsetsOpt) && config.options.has(config.resetBeginningOpt) &&
      checkZkPathExists(config.options.valueOf(config.zkConnectOpt), "/consumers/" + config.consumerProps.getProperty("group.id") + "/offsets")) {
      System.err.println("Found previous offset information for this group " + config.consumerProps.getProperty("group.id")
        + ". Please use --delete-consumer-offsets to delete previous offsets metadata")
      Exit.exit(1)
    }
  }

  def addShutdownHook(consumer: BaseConsumer, conf: ConsumerConfig) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        consumer.stop()

        shutdownLatch.await()

        if (conf.enableSystestEventsLogging) {
          System.out.println("shutdown_complete")
        }
      }
    })
  }

  def process(maxMessages: Integer, formatter: MessageFormatter, consumer: BaseConsumer, output: PrintStream, skipMessageOnError: Boolean) {
    while (messageCount < maxMessages || maxMessages == -1) {
      val msg: BaseConsumerRecord = try {
        consumer.receive()
      } catch {
        case _: StreamEndException =>
          trace("Caught StreamEndException because consumer is shutdown, ignore and terminate.")
          // Consumer is already closed
          return
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
      // This means no one is listening to our output stream any more, time to shutdown
      System.err.println("Unable to write to standard out, closing consumer.")
    }
    gotError
  }

  def getOldConsumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties

    props ++= config.consumerProps
    props ++= config.extraConsumerProps
    setAutoOffsetResetValue(config, props)
    props.put("zookeeper.connect", config.zkConnectionStr)

    if (config.timeoutMs >= 0)
      props.put("consumer.timeout.ms", config.timeoutMs.toString)

    props
  }

  def checkAndMaybeDeleteOldPath(config: ConsumerConfig, props: Properties) = {
    val consumerGroupBasePath = "/consumers/" + props.getProperty("group.id")
    if (config.options.has(config.deleteConsumerOffsetsOpt)) {
      ZkUtils.maybeDeletePath(config.options.valueOf(config.zkConnectOpt), consumerGroupBasePath)
    } else {
      val resetToBeginning = OffsetRequest.SmallestTimeString == props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      if (resetToBeginning && checkZkPathExists(config.options.valueOf(config.zkConnectOpt), consumerGroupBasePath + "/offsets")) {
        System.err.println("Found previous offset information for this group " + props.getProperty("group.id")
          + ". Please use --delete-consumer-offsets to delete previous offsets metadata")
        Exit.exit(1)
      }
    }
  }

  private[tools] def getNewConsumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties
    props ++= config.consumerProps
    props ++= config.extraConsumerProps
    setAutoOffsetResetValue(config, props)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.isolationLevel)
    props
  }

  /**
    * Used by both getNewConsumerProps and getOldConsumerProps to retrieve the correct value for the
    * consumer parameter 'auto.offset.reset'.
    * Order of priority is:
    *   1. Explicitly set parameter via --consumer.property command line parameter
    *   2. Explicit --from-beginning given -> 'earliest'
    *   3. Default value of 'latest'
    *
    * In case both --from-beginning and an explicit value are specified an error is thrown if these
    * are conflicting.
    */
  def setAutoOffsetResetValue(config: ConsumerConfig, props: Properties) {
    val (earliestConfigValue, latestConfigValue) = if (config.useOldConsumer)
      (OffsetRequest.SmallestTimeString, OffsetRequest.LargestTimeString)
    else
      ("earliest", "latest")

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
    val blacklistOpt = parser.accepts("blacklist", "Blacklist of topics to exclude from consumption.")
      .withRequiredArg
      .describedAs("blacklist")
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
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
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
    val deleteConsumerOffsetsOpt = parser.accepts("delete-consumer-offsets", "If specified, the consumer path in zookeeper is deleted when starting up")
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
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
      "set, the csv metrics will be output here")
      .withRequiredArg
      .describedAs("metrics directory")
      .ofType(classOf[java.lang.String])
    val newConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation. This is the default, so " +
      "this option is deprecated and will be removed in a future release.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED (unless old consumer is used): The server to connect to.")
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
    val useOldConsumer = options.has(zkConnectOpt)
    val enableSystestEventsLogging = options.has(enableSystestEventsLoggingOpt)

    // If using old consumer, exactly one of whitelist/blacklist/topic is required.
    // If using new consumer, topic must be specified.
    var topicArg: String = null
    var whitelistArg: String = null
    var filterSpec: TopicFilter = null
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt).asScala)
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val zkConnectionStr = options.valueOf(zkConnectOpt)
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

    if (useOldConsumer) {
      if (options.has(bootstrapServerOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $bootstrapServerOpt is not valid with $zkConnectOpt.")
      else if (options.has(newConsumerOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $newConsumerOpt is not valid with $zkConnectOpt.")
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/blacklist/topic is required.")
      topicArg = options.valueOf(topicOrFilterOpt.head)
      filterSpec = if (options.has(blacklistOpt)) new Blacklist(topicArg) else new Whitelist(topicArg)
      Console.err.println("Using the ConsoleConsumer with old consumer is deprecated and will be removed " +
        s"in a future major release. Consider using the new consumer by passing $bootstrapServerOpt instead of ${zkConnectOpt}.")
    } else {
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/topic is required.")
      topicArg = options.valueOf(topicIdOpt)
      whitelistArg = options.valueOf(whitelistOpt)
    }

    if (useOldConsumer && (partitionArg.isDefined || options.has(offsetOpt)))
      CommandLineUtils.printUsageAndDie(parser, "Partition-offset based consumption is supported in the new consumer only.")

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
          case "earliest" => OffsetRequest.EarliestTime
          case "latest" => OffsetRequest.LatestTime
          case offsetString =>
            val offset =
              try offsetString.toLong
              catch {
                case _: NumberFormatException => invalidOffset(offsetString)
              }
            if (offset < 0) invalidOffset(offsetString)
            offset
        }
      }
      else if (fromBeginning) OffsetRequest.EarliestTime
      else OffsetRequest.LatestTime

    if (!useOldConsumer) {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(newConsumerOpt)) {
        Console.err.println("The --new-consumer option is deprecated and will be removed in a future major release. " +
          "The new consumer is used by default if the --bootstrap-server option is provided.")
      }
    }

    if (options.has(csvMetricsReporterEnabledOpt)) {
      val csvReporterProps = new Properties()
      csvReporterProps.put("kafka.metrics.polling.interval.secs", "5")
      csvReporterProps.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter")
      if (options.has(metricsDirectoryOpt))
        csvReporterProps.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt))
      else
        csvReporterProps.put("kafka.csv.metrics.dir", "kafka_metrics")
      csvReporterProps.put("kafka.csv.metrics.reporter.enabled", "true")
      val verifiableProps = new VerifiableProperties(csvReporterProps)
      KafkaMetricsReporter.startReporters(verifiableProps)
    }

    // if the group id is provided in more than place (through different means) all values must be the same
    val groupIdsProvided = Set(
        Option(options.valueOf(groupIdOpt)),                           // via --group
        Option(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG)),     // via --consumer-property
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

  def checkZkPathExists(zkUrl: String, path: String): Boolean = {
    try {
      val zk = ZkUtils.createZkClient(zkUrl, 30 * 1000, 30 * 1000)
      zk.exists(path)
    } catch {
      case _: Throwable => false
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
      keyDeserializer.get.configure(JavaConversions.propertiesAsScalaMap(stripWithPrefix("key.deserializer.", props)).asJava, true)
    }
    // Note that `toString` will be called on the instance returned by `Deserializer.deserialize`
    if (props.containsKey("value.deserializer")) {
      valueDeserializer = Some(Class.forName(props.getProperty("value.deserializer")).newInstance().asInstanceOf[Deserializer[_]])
      valueDeserializer.get.configure(JavaConversions.propertiesAsScalaMap(stripWithPrefix("value.deserializer.", props)).asJava, false)
    }
  }

  def stripWithPrefix(prefix: String, props: Properties): Properties = {
    val newProps = new Properties()
    import scala.collection.JavaConversions._
    for (entry <- props) {
      val key: String = entry._1
      val value: String = entry._2

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
    import consumerRecord._
    val chksum =
      if (timestampType != TimestampType.NO_TIMESTAMP_TYPE)
        new Message(value, key, timestamp, timestampType, NoCompressionCodec, 0, -1, Message.MagicValue_V1).checksum
      else
        new Message(value, key, Message.NoTimestamp, Message.MagicValue_V0).checksum
    output.println(topicStr + "checksum:" + chksum)
  }
}
