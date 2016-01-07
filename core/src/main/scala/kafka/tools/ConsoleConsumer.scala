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
import java.util.concurrent.CountDownLatch
import java.util.{Properties, Random}
import joptsimple._
import kafka.common.StreamEndException
import kafka.consumer._
import kafka.message._
import kafka.metrics.KafkaMetricsReporter
import kafka.utils._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.utils.Utils
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

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
      case e: Throwable =>
        error("Unknown error when running consumer: ", e)
        System.exit(1);
    }
  }

  def run(conf: ConsumerConfig) {

    val consumer =
      if (conf.useNewConsumer) {
        val timeoutMs = if (conf.timeoutMs >= 0) conf.timeoutMs else Long.MaxValue
        new NewShinyConsumer(Option(conf.topicArg), Option(conf.whitelistArg), getNewConsumerProps(conf), timeoutMs)
      } else {
        checkZk(conf)
        new OldConsumer(conf.filterSpec, getOldConsumerProps(conf))
      }

    addShutdownHook(consumer, conf)

    try {
      process(conf.maxMessages, conf.formatter, consumer, conf.skipMessageOnError)
    } finally {
      consumer.cleanup()
      reportRecordCount()

      // if we generated a random group id (as none specified explicitly) then avoid polluting zookeeper with persistent group data, this is a hack
      if (!conf.groupIdPassed)
        ZkUtils.maybeDeletePath(conf.options.valueOf(conf.zkConnectOpt), "/consumers/" + conf.consumerProps.get("group.id"))

      shutdownLatch.countDown()
    }
  }

  def checkZk(config: ConsumerConfig) {
    if (!checkZkPathExists(config.options.valueOf(config.zkConnectOpt), "/brokers/ids")) {
      System.err.println("No brokers found in ZK.")
      System.exit(1)
    }

    if (!config.options.has(config.deleteConsumerOffsetsOpt) && config.options.has(config.resetBeginningOpt) &&
      checkZkPathExists(config.options.valueOf(config.zkConnectOpt), "/consumers/" + config.consumerProps.getProperty("group.id") + "/offsets")) {
      System.err.println("Found previous offset information for this group " + config.consumerProps.getProperty("group.id")
        + ". Please use --delete-consumer-offsets to delete previous offsets metadata")
      System.exit(1)
    }
  }

  def addShutdownHook(consumer: BaseConsumer, conf: ConsumerConfig) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        consumer.stop()

        shutdownLatch.await()
      }
    })
  }

  def process(maxMessages: Integer, formatter: MessageFormatter, consumer: BaseConsumer, skipMessageOnError: Boolean) {
    while (messageCount < maxMessages || maxMessages == -1) {
      val msg: BaseConsumerRecord = try {
        consumer.receive()
      } catch {
        case nse: StreamEndException =>
          trace("Caught StreamEndException because consumer is shutdown, ignore and terminate.")
          // Consumer is already closed
          return
        case nse: WakeupException =>
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
        formatter.writeTo(msg.key, msg.value, System.out)
      } catch {
        case e: Throwable =>
          if (skipMessageOnError) {
            error("Error processing message, skipping this message: ", e)
          } else {
            // Consumer will be closed
            throw e
          }
      }
      checkErr(formatter)
    }
  }

  def reportRecordCount() {
    System.err.println(s"Processed a total of $messageCount messages")
  }

  def checkErr(formatter: MessageFormatter) {
    if (System.out.checkError()) {
      // This means no one is listening to our output stream any more, time to shutdown
      System.err.println("Unable to write to standard out, closing consumer.")
      formatter.close()
      System.exit(1)
    }
  }

  def getOldConsumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties

    props.putAll(config.consumerProps)
    props.put("auto.offset.reset", if (config.fromBeginning) "smallest" else "largest")
    props.put("zookeeper.connect", config.zkConnectionStr)

    if (!config.options.has(config.deleteConsumerOffsetsOpt) && config.options.has(config.resetBeginningOpt) &&
      checkZkPathExists(config.options.valueOf(config.zkConnectOpt), "/consumers/" + props.getProperty("group.id") + "/offsets")) {
      System.err.println("Found previous offset information for this group " + props.getProperty("group.id")
        + ". Please use --delete-consumer-offsets to delete previous offsets metadata")
      System.exit(1)
    }

    if (config.options.has(config.deleteConsumerOffsetsOpt))
      ZkUtils.maybeDeletePath(config.options.valueOf(config.zkConnectOpt), "/consumers/" + config.consumerProps.getProperty("group.id"))
    if (config.timeoutMs >= 0)
      props.put("consumer.timeout.ms", config.timeoutMs.toString)

    props
  }

  def getNewConsumerProps(config: ConsumerConfig): Properties = {
    val props = new Properties

    props.putAll(config.consumerProps)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (config.options.has(config.resetBeginningOpt)) "earliest" else "latest")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, if (config.keyDeserializer != null) config.keyDeserializer else "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, if (config.valueDeserializer != null) config.valueDeserializer else "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    props
  }

  class ConsumerConfig(args: Array[String]) {
    val parser = new OptionParser
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
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
      .withRequiredArg
      .describedAs("class")
      .ofType(classOf[String])
      .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property")
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
      "set, the csv metrics will be outputed here")
      .withRequiredArg
      .describedAs("metrics directory")
      .ofType(classOf[java.lang.String])
    val useNewConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server")
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

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "The console consumer is a tool that reads data from Kafka and outputs it to standard output.")

    var groupIdPassed = true
    val options: OptionSet = tryParse(parser, args)
    val useNewConsumer = options.has(useNewConsumerOpt)

    // If using old consumer, exactly one of whitelist/blacklist/topic is required.
    // If using new consumer, topic must be specified.
    var topicArg: String = null
    var whitelistArg: String = null
    var filterSpec: TopicFilter = null
    if (useNewConsumer) {
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/topic is required.")
      topicArg = options.valueOf(topicIdOpt)
      whitelistArg = options.valueOf(whitelistOpt)
    } else {
      val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
      if (topicOrFilterOpt.size != 1)
        CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/blacklist/topic is required.")
      topicArg = options.valueOf(topicOrFilterOpt.head)
      filterSpec = if (options.has(blacklistOpt)) new Blacklist(topicArg) else new Whitelist(topicArg)
    }
    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val zkConnectionStr = options.valueOf(zkConnectOpt)
    val fromBeginning = options.has(resetBeginningOpt)
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt))
    val maxMessages = if (options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
    val bootstrapServer = options.valueOf(bootstrapServerOpt)
    val keyDeserializer = options.valueOf(keyDeserializerOpt)
    val valueDeserializer = options.valueOf(valueDeserializerOpt)
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]
    formatter.init(formatterArgs)

    CommandLineUtils.checkRequiredArgs(parser, options, if (useNewConsumer) bootstrapServerOpt else zkConnectOpt)

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

    //Provide the consumer with a randomly assigned group id
    if(!consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,"console-consumer-" + new Random().nextInt(100000))
      groupIdPassed=false
    }

    def tryParse(parser: OptionParser, args: Array[String]) = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          Utils.croak(e.getMessage)
          null
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

trait MessageFormatter{
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream)

  def init(props: Properties) {}

  def close() {}
}

class DefaultMessageFormatter extends MessageFormatter {
  var printKey = false
  var keySeparator = "\t".getBytes
  var lineSeparator = "\n".getBytes

  override def init(props: Properties) {
    if (props.containsKey("print.key"))
      printKey = props.getProperty("print.key").trim.toLowerCase.equals("true")
    if (props.containsKey("key.separator"))
      keySeparator = props.getProperty("key.separator").getBytes
    if (props.containsKey("line.separator"))
      lineSeparator = props.getProperty("line.separator").getBytes
  }

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
    if (printKey) {
      output.write(if (key == null) "null".getBytes() else key)
      output.write(keySeparator)
    }
    output.write(if (value == null) "null".getBytes() else value)
    output.write(lineSeparator)
  }
}

class LoggingMessageFormatter extends MessageFormatter   {
  private val defaultWriter: DefaultMessageFormatter = new DefaultMessageFormatter
  val logger = Logger.getLogger(getClass().getName)

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream): Unit = {
    defaultWriter.writeTo(key, value, output)
    if(logger.isInfoEnabled)
      logger.info(s"key:${if (key == null) "null" else new String(key)}, value:${if (value == null) "null" else new String(value)}")
  }
}

class NoOpMessageFormatter extends MessageFormatter {
  override def init(props: Properties) {}

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {}
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

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
    val chksum = new Message(value, key, Message.NoTimestamp, Message.MagicValue_V0).checksum
    output.println(topicStr + "checksum:" + chksum)
  }
}
