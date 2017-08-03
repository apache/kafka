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

package kafka.admin

import java.io.PrintStream
import java.util.Properties

import kafka.utils.CommandLineUtils
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.CommonClientConfigs
import joptsimple._

import scala.util.{Failure, Success}

/**
 * A command for retrieving broker version information.
 */
object BrokerApiVersionsCommand {

  def main(args: Array[String]): Unit = {
    execute(args, System.out)
  }

  def execute(args: Array[String], out: PrintStream): Unit = {
    val opts = new BrokerVersionCommandOptions(args)
    val adminClient = createAdminClient(opts)
    adminClient.awaitBrokers()
    val brokerMap = adminClient.listAllBrokerVersionInfo()
    brokerMap.foreach { case (broker, versionInfoOrError) =>
      versionInfoOrError match {
        case Success(v) => out.print(s"${broker} -> ${v.toString(true)}\n")
        case Failure(v) => out.print(s"${broker} -> ERROR: ${v}\n")
      }
    }
    adminClient.close()
  }

  private def createAdminClient(opts: BrokerVersionCommandOptions): AdminClient = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    AdminClient.create(props)
  }

  class BrokerVersionCommandOptions(args: Array[String]) {
    val BootstrapServerDoc = "REQUIRED: The server to connect to."
    val CommandConfigDoc = "A property file containing configs to be passed to Admin Client."

    val parser = new OptionParser(false)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                 .withRequiredArg
                                 .describedAs("command config property file")
                                 .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server(s) to use for bootstrapping")
                                   .ofType(classOf[String])
    val options = parser.parse(args : _*)
    checkArgs()

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
    }
  }
}
