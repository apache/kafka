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
package kafka.utils

import joptsimple.OptionParser
import org.apache.kafka.server.util.CommandLineUtils

object ToolsUtils {

  def validatePortOrDie(parser: OptionParser, hostPort: String): Unit = {
    val hostPorts: Array[String] = if (hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
    val validHostPort = hostPorts.filter { hostPortData =>
      org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null
    }
    val isValid = validHostPort.nonEmpty && validHostPort.length == hostPorts.length
    if (!isValid)
      CommandLineUtils.printUsageAndExit(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")
  }

  /**
   * This is a simple wrapper around `CommandLineUtils.printUsageAndExit`.
   * It is needed for tools migration (KAFKA-14525), as there is no Java equivalent for return type `Nothing`.
   * Can be removed once [[kafka.tools.ConsoleProducer]] are migrated.
   *
   * @param parser Command line options parser.
   * @param message Error message.
   */
  def printUsageAndExit(parser: OptionParser, message: String): Nothing = {
    CommandLineUtils.printUsageAndExit(parser, message)
    throw new AssertionError("printUsageAndExit should not return, but it did.")
  }
}
