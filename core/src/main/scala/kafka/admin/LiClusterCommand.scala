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

import joptsimple.OptionSpec
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, Logging}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, MoveControllerOptions, AdminClient => JAdminClient}
import org.apache.kafka.common.utils.Utils
import java.util.Properties

object LiClusterCommand extends Logging {
  def createAdminClient(commandConfig: Properties, bootstrapServer: Option[String]): Admin = {
    bootstrapServer match {
      case Some(serverList) => commandConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverList)
      case None =>
    }
    JAdminClient.create(commandConfig)
  }

  def main(args: Array[String]): Unit = {
    val opts = new ControllerCommandOptions(args)
    opts.checkArgs()

    val adminClient = createAdminClient(opts.commandConfig, opts.bootstrapServer)

    var exitCode = 0
    try {
      if (opts.hasMoveControllerOption) {
        val moveControllerResult = adminClient.moveController(new MoveControllerOptions())
        moveControllerResult.all().get()
        println("The move-controller request has completed successfully.")
      }
    } catch {
      case throwable: Throwable =>
        println("Error while executing cluster command: " + throwable.getMessage)
        error(Utils.stackTrace(throwable))
        exitCode = 1
    } finally {
      adminClient.close()
      Exit.exit(exitCode)
    }
  }

  class ControllerCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    private val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to. In case of providing this, a direct Zookeeper connection won't be required.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    private val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])
    private val moveControllerOpt = parser.accepts("move-controller", "Move the controller to a different host.")

    options = parser.parse(args : _*)
    def has(builder: OptionSpec[_]): Boolean = options.has(builder)
    def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue
    def hasMoveControllerOption: Boolean = has(moveControllerOpt)
    def bootstrapServer: Option[String] = valueAsOption(bootstrapServerOpt)
    def commandConfig: Properties = if (has(commandConfigOpt)) Utils.loadProps(options.valueOf(commandConfigOpt)) else new Properties()

    def checkArgs(): Unit = {
      if (args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "move a controller")

      CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to move a controller.")

      // should have exactly one action
      val actions = Seq(moveControllerOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --move-controller")
      if (!has(bootstrapServerOpt)) {
        throw new IllegalArgumentException("The --bootstrap-server option must be set")
      }
    }
  }
}
