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

import joptsimple.OptionSpecBuilder
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Logging}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, SkipShutdownSafetyCheckOptions}
import org.apache.kafka.common.utils.Utils

import java.util.Properties

object SkipShutdownSafetyCheckCommand extends Logging {
  def main(args: Array[String]): Unit = {
    val commandOpts = new SkipShutdownSafetyCheckCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(commandOpts, "Skip shutdown safety check for a given (brokerId, brokerEpoch)")
    CommandLineUtils.checkRequiredArgs(commandOpts.parser, commandOpts.options)

    //noinspection DuplicatedCode
    val adminProps = if (commandOpts.options.has(commandOpts.adminClientConfigOpt))
      Utils.loadProps(commandOpts.options.valueOf(commandOpts.adminClientConfigOpt))
    else
      new Properties()

    adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, commandOpts.options.valueOf(commandOpts.bootstrapServerOpt))
    adminProps.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000.toString)

    val adminClient = AdminClient.create(adminProps)

    val options = new SkipShutdownSafetyCheckOptions()
      .brokerId(commandOpts.options.valueOf(commandOpts.brokerIdOpt))
      .brokerEpoch(commandOpts.options.valueOf(commandOpts.brokerEpochOpt))
    val result = adminClient.skipShutdownSafetyCheck(options)

    result.all();

    println(s"Shutdown safety check will be skipped for broker ${options.brokerId()} with epoch ${options.brokerEpoch()}")
  }

  class SkipShutdownSafetyCheckCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val brokerIdOpt = parser.accepts("broker-id", "Broker ID that should skip the shutdown safety check.")
      .withRequiredArg()
      .describedAs("Broker ID that should skip the shutdown safety check.")
      .ofType(classOf[Int])

    val brokerEpochOpt = parser.accepts("broker-epoch", "Broker epoch that should skip the shutdown safety check.")
      .withRequiredArg()
      .describedAs("Broker epoch that should skip the shutdown safety check.")
      .ofType(classOf[Long])

    private val bootstrapOptBuilder: OptionSpecBuilder = parser.accepts("bootstrap-server",
      "A hostname and port for the broker to connect to, " +
        "in the form host:port. Multiple comma-separated URLs can be given. REQUIRED unless --zookeeper is given.")
    val bootstrapServerOpt = bootstrapOptBuilder
      .withRequiredArg
      .describedAs("host:port")
      .ofType(classOf[String])

    val adminClientConfigOpt = parser.accepts("admin.config",
      "Admin client config properties file to pass to the admin client when --bootstrap-server is given.")
      .availableIf(bootstrapServerOpt)
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])

    options = parser.parse(args: _*)
  }
}
