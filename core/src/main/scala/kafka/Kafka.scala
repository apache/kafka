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

package kafka

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import sun.misc.{Signal, SignalHandler}
import joptsimple.OptionParser
import kafka.utils.Implicits._
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{OperatingSystem, Utils}

import scala.collection.JavaConverters._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  private def registerLoggingSignalHandler(): Unit = {
    val jvmSignalHandlers = new ConcurrentHashMap[String, SignalHandler]().asScala
    val handler = new SignalHandler() {
      override def handle(signal: Signal) {
        info(s"Terminating process due to signal $signal")
        jvmSignalHandlers.get(signal.getName).foreach(_.handle(signal))
      }
    }
    def registerHandler(signalName: String) {
      val oldHandler = Signal.handle(new Signal(signalName), handler)
      if (oldHandler != null)
        jvmSignalHandlers.put(signalName, oldHandler)
    }

    if (!OperatingSystem.IS_WINDOWS) {
      registerHandler("TERM")
      registerHandler("INT")
      registerHandler("HUP")
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args)
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      // register signal handler to log termination due to SIGTERM, SIGHUP and SIGINT (control-c)
      registerLoggingSignalHandler()

      // attach shutdown handler to catch terminating signals as well as normal termination
      Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
        override def run(): Unit = kafkaServerStartable.shutdown()
      })

      kafkaServerStartable.startup()
      kafkaServerStartable.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal(e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
