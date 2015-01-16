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
import kafka.consumer.{Blacklist,Whitelist,ConsumerConfig,Consumer}

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
    val deleteConsumerOffsetsOpt = parser.accepts("delete-consumer-offsets", "If specified, the consumer path in zookeeper is deleted when starting up");
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
            "start with the earliest message present in the log rather than the latest message.")
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

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "The console consumer is a tool that reads data from Kafka and outputs it to standard output.")
      
    var groupIdPassed = true
    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
    val topicOrFilterOpt = List(topicIdOpt, whitelistOpt, blacklistOpt).filter(options.has)
    if (topicOrFilterOpt.size != 1)
      CommandLineUtils.printUsageAndDie(parser, "Exactly one of whitelist/blacklist/topic is required.")
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



    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()

    if(!consumerProps.containsKey("group.id")) {
      consumerProps.put("group.id","console-consumer-" + new Random().nextInt(100000))
      groupIdPassed=false
    }
    consumerProps.put("auto.offset.reset", if(options.has(resetBeginningOpt)) "smallest" else "largest")
    consumerProps.put("zookeeper.connect", options.valueOf(zkConnectOpt))

    if (!options.has(deleteConsumerOffsetsOpt) && options.has(resetBeginningOpt) &&
       checkZkPathExists(options.valueOf(zkConnectOpt),"/consumers/" + consumerProps.getProperty("group.id")+ "/offsets")) {
      System.err.println("Found previous offset information for this group "+consumerProps.getProperty("group.id")
        +". Please use --delete-consumer-offsets to delete previous offsets metadata")
      System.exit(1)
    }

    if(options.has(deleteConsumerOffsetsOpt))
      ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + consumerProps.getProperty("group.id"))

    val config = new ConsumerConfig(consumerProps)
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt))
    val maxMessages = if(options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1
    val connector = Consumer.create(config)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        connector.shutdown()
        // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
        if(!groupIdPassed)
          ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + consumerProps.get("group.id"))
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
          case e: Throwable =>
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
      case e: Throwable => error("Error processing message, stopping consumer: ", e)
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

  def checkZkPathExists(zkUrl: String, path: String): Boolean = {
    try {
      val zk = new ZkClient(zkUrl, 30*1000,30*1000, ZKStringSerializer);
      zk.exists(path)
    } catch {
      case _: Throwable => false
    }
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
      output.write(if (key == null) "null".getBytes() else key)
      output.write(keySeparator)
    }
    output.write(if (value == null) "null".getBytes() else value)
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
