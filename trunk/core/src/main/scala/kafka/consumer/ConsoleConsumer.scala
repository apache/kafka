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

import scala.collection.mutable._
import scala.collection.JavaConversions._
import org.I0Itec.zkclient._
import joptsimple._
import java.util.Properties
import java.util.Random
import java.io.PrintStream
import kafka.message._
import kafka.utils.{Utils, Logging}
import kafka.utils.ZKStringSerializer

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
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(2 * 1024 * 1024)
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
                           .defaultsTo(classOf[NewlineMessageFormatter].getName)
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

    val options: OptionSet = tryParse(parser, args)
    Utils.checkRequiredArgs(parser, options, zkConnectOpt)
    
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

    val props = new Properties()
    props.put("groupid", options.valueOf(groupIdOpt))
    props.put("socket.buffersize", options.valueOf(socketBufferSizeOpt).toString)
    props.put("fetch.size", options.valueOf(fetchSizeOpt).toString)
    props.put("auto.commit", "true")
    props.put("autocommit.interval.ms", options.valueOf(autoCommitIntervalOpt).toString)
    props.put("autooffset.reset", if(options.has(resetBeginningOpt)) "smallest" else "largest")
    props.put("zk.connect", options.valueOf(zkConnectOpt))
    props.put("consumer.timeout.ms", options.valueOf(consumerTimeoutMsOpt).toString)
    val config = new ConsumerConfig(props)
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false
    
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs = tryParseFormatterArgs(options.valuesOf(messageFormatterArgOpt))
    
    val maxMessages = if(options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1

    val connector = Consumer.create(config)

    if(options.has(resetBeginningOpt))
      tryCleanupZookeeper(options.valueOf(zkConnectOpt), options.valueOf(groupIdOpt))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        connector.shutdown()
        // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
        if(!options.has(groupIdOpt))  
          tryCleanupZookeeper(options.valueOf(zkConnectOpt), options.valueOf(groupIdOpt))
      }
    })

    val stream = connector.createMessageStreamsByFilter(filterSpec).get(0)
    val iter = if(maxMessages >= 0)
      stream.slice(0, maxMessages)
    else
      stream

    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]
    formatter.init(formatterArgs)

    try {
      for(messageAndTopic <- iter) {
        try {
          formatter.writeTo(messageAndTopic.message, System.out)
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
          formatter.close()
          connector.shutdown()
          System.exit(1)
        }
      }
    } catch {
      case e => error("Error processing message, stopping consumer: ", e)
    }
      
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
  
  trait MessageFormatter {
    def writeTo(message: Message, output: PrintStream)
    def init(props: Properties) {}
    def close() {}
  }
  
  class NewlineMessageFormatter extends MessageFormatter {
    def writeTo(message: Message, output: PrintStream) {
      val payload = message.payload
      output.write(payload.array, payload.arrayOffset, payload.limit)
      output.write('\n')
    }
  }

  class ChecksumMessageFormatter extends MessageFormatter {
    private var topicStr: String = _
    
    override def init(props: Properties) {
      topicStr = props.getProperty("topic")
      if (topicStr != null) 
        topicStr = topicStr + "-"
      else
        topicStr = ""
    }
    
    def writeTo(message: Message, output: PrintStream) {
      val chksum = message.checksum
      output.println(topicStr + "checksum:" + chksum)
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
