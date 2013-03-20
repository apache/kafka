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

package kafka.consumer

import scala.collection.JavaConversions._
import org.I0Itec.zkclient._
import joptsimple._
import java.util.Properties
import java.util.Random
import java.io.PrintStream
import kafka.message._
import kafka.serializer._
import kafka.utils._
import kafka.metrics.KafkaMetricsReporter


/**
 * Consumer that dumps messages out to standard out.
 *
 */
object ConsoleConsumer extends Logging {

  def main(args: Array[String]) {
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
    val groupIdOpt = parser.accepts("group", "The group id to consume on.")
            .withRequiredArg
            .describedAs("gid")
            .defaultsTo("console-consumer-" + new Random().nextInt(100000))
            .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
            .withRequiredArg
            .describedAs("size")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(1024 * 1024)
    val minFetchBytesOpt = parser.accepts("min-fetch-bytes", "The min number of bytes each fetch request waits for.")
            .withRequiredArg
            .describedAs("bytes")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(1)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
            .withRequiredArg
            .describedAs("ms")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(100)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
            .withRequiredArg
            .describedAs("size")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(2 * 1024 * 1024)
    val socketTimeoutMsOpt = parser.accepts("socket-timeout-ms", "The socket timeout used for the connection to the broker")
            .withRequiredArg
            .describedAs("ms")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(ConsumerConfig.SocketTimeout)
    val consumerTimeoutMsOpt = parser.accepts("consumer-timeout-ms", "consumer throws timeout exception after waiting this much " +
            "of time without incoming messages")
            .withRequiredArg
            .describedAs("prop")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(-1)
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
            .withRequiredArg
            .describedAs("class")
            .ofType(classOf[String])
            .defaultsTo(classOf[DefaultMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property")
            .withRequiredArg
            .describedAs("prop")
            .ofType(classOf[String])
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
            "start with the earliest message present in the log rather than the latest message.")
    val autoCommitIntervalOpt = parser.accepts("autocommit.interval.ms", "The time interval at which to save the current offset in ms")
            .withRequiredArg
            .describedAs("ms")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(10*1000)
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
            .withRequiredArg
            .describedAs("num_messages")
            .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
            "skip it instead of halt.")
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
            "set, the csv metrics will be outputed here")
      .withRequiredArg
      .describedAs("metrics dictory")
      .ofType(classOf[java.lang.String])


    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
    val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
    if (topicOrFilterOpt.size != 1) {
      error("Exactly one of whitelist/blacklist/topic is required.")
      parser.printHelpOn(System.err)
      System.exit(1)
    }
    val topicArg = options.valueOf(topicOrFilterOpt.head)
    val filterSpec = if (options.has(blacklistOpt))
      new Blacklist(topicArg)
    else
      new Whitelist(topicArg)

    val csvMetricsReporterEnabled = options.has(csvMetricsReporterEnabledOpt)
    if (csvMetricsReporterEnabled) {
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

    val props = new Properties()
    props.put("group.id", options.valueOf(groupIdOpt))
    props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString)
    props.put("socket.timeout.ms", options.valueOf(socketTimeoutMsOpt).toString)
    props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString)
    props.put("fetch.min.bytes", options.valueOf(minFetchBytesOpt).toString)
    props.put("fetch.wait.max.ms", options.valueOf(maxWaitMsOpt).toString)
    props.put("auto.commit.enable", "true")
    props.put("auto.commit.interval.ms", options.valueOf(autoCommitIntervalOpt).toString)
    props.put("auto.offset.reset", if(options.has(resetBeginningOpt)) "smallest" else "largest")
    props.put("zk.connect", options.valueOf(zkConnectOpt))
    props.put("consumer.timeout.ms", options.valueOf(consumerTimeoutMsOpt).toString)
    val config = new ConsumerConfig(props)
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false

    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = MessageFormatter.tryParseFormatterArgs(options.valuesOf(messageFormatterArgOpt))

    val maxMessages = if(options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1

    val connector = Consumer.create(config)

    if(options.has(resetBeginningOpt))
      ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + options.valueOf(groupIdOpt))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        connector.shutdown()
        // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
        if(!options.has(groupIdOpt))  
          ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + options.valueOf(groupIdOpt))
      }
    })

    var numMessages = 0L
    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]
    formatter.init(formatterArgs)
    try {
      val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)
      val iter = if(maxMessages >= 0)
        stream.slice(0, maxMessages)
      else
        stream

      for(messageAndTopic <- iter) {
        try {
          formatter.writeTo(messageAndTopic.key, messageAndTopic.message, System.out)
          numMessages += 1
        } catch {
          case e =>
            if (skipMessageOnError)
              error("Error processing message, skipping this message: ", e)
            else
              throw e
        }
        if(System.out.checkError()) {
          // This means no one is listening to our output stream any more, time to shutdown
          System.err.println("Unable to write to standard out, closing consumer.")
          System.err.println("Consumed %d messages".format(numMessages))
          formatter.close()
          connector.shutdown()
          System.exit(1)
        }
      }
    } catch {
      case e => error("Error processing message, stopping consumer: ", e)
    }
    System.err.println("Consumed %d messages".format(numMessages))
    System.out.flush()
    formatter.close()
    connector.shutdown()
  }

  def tryParse(parser: OptionParser, args: Array[String]) = {
    try {
      parser.parse(args : _*)
    } catch {
      case e: OptionException => {
        Utils.croak(e.getMessage)
        null
      }
    }
  }

  def tryCleanupZookeeper(zkUrl: String, groupId: String) {
    try {
      val dir = "/consumers/" + groupId
      info("Cleaning up temporary zookeeper data under " + dir + ".")
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _ => // swallow
    }
  }
}


object MessageFormatter {
  def tryParseFormatterArgs(args: Iterable[String]): Properties = {
    val splits = args.map(_ split "=").filterNot(_ == null).filterNot(_.length == 0)
    if(!splits.forall(_.length == 2)) {
      System.err.println("Invalid parser arguments: " + args.mkString(" "))
      System.exit(1)
    }
    val props = new Properties
    for(a <- splits)
      props.put(a(0), a(1))
    props
  }
}

trait MessageFormatter {
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream)
  def init(props: Properties) {}
  def close() {}
}

class DefaultMessageFormatter extends MessageFormatter {
  var printKey = false
  var keySeparator = "\t".getBytes
  var lineSeparator = "\n".getBytes
  
  override def init(props: Properties) {
    if(props.containsKey("print.key"))
      printKey = props.getProperty("print.key").trim.toLowerCase.equals("true")
    if(props.containsKey("key.separator"))
      keySeparator = props.getProperty("key.separator").getBytes
    if(props.containsKey("line.separator"))
      lineSeparator = props.getProperty("line.separator").getBytes
  }
  
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream) {
    if(printKey) {
      output.write(key)
      output.write(keySeparator)
    }
    output.write(value)
    output.write(lineSeparator)
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
    val chksum = new Message(value, key).checksum
    output.println(topicStr + "checksum:" + chksum)
  }
}
