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

import java.util.Properties

import joptsimple.{OptionParser, OptionSet, OptionSpec}

import scala.collection.Set

/**
  * Helper functions for dealing with command line utilities
  */
object CommandLineUtils extends Logging {
  /**
    * Check if there are no options or `--help` option from command line
    *
    * @param commandOpts Acceptable options for a command
    * @return true on matching the help check condition
    */
  def isPrintHelpNeeded(commandOpts: CommandDefaultOptions): Boolean = {
    return commandOpts.args.length == 0 || commandOpts.options.has(commandOpts.helpOpt)
  }

  /**
    * Check and print help message if there is no options or `--help` option
    * from command line
    *
    * @param commandOpts Acceptable options for a command
    * @param message     Message to display on successful check
    */
  def printHelpAndExitIfNeeded(commandOpts: CommandDefaultOptions, message: String) = {
    if (isPrintHelpNeeded(commandOpts))
      printUsageAndDie(commandOpts.parser, message)
  }

  /**
   * Check that all the listed options are present
   */
  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
    for (arg <- required) {
      if (!options.has(arg))
        printUsageAndDie(parser, "Missing required argument \"" + arg + "\"")
    }
  }

  /**
   * Check that none of the listed options are present
   */
  def checkInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]) {
    if (options.has(usedOption)) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          printUsageAndDie(parser, "Option \"" + usedOption + "\" can't be used with option\"" + arg + "\"")
      }
    }
  }

  /**
   * Print usage and exit
   */
  def printUsageAndDie(parser: OptionParser, message: String): Nothing = {
    System.err.println(message)
    parser.printHelpOn(System.err)
    Exit.exit(1, Some(message))
  }

  /**
   * Parse key-value pairs in the form key=value
   * value may contain equals sign
   */
  def parseKeyValueArgs(args: Iterable[String], acceptMissingValue: Boolean = true): Properties = {
    val splits = args.map(_.split("=", 2)).filterNot(_.length == 0)

    val props = new Properties
    for (a <- splits) {
      if (a.length == 1 || (a.length == 2 && a(1).isEmpty())) {
        if (acceptMissingValue) props.put(a(0), "")
        else throw new IllegalArgumentException(s"Missing value for key ${a(0)}")
      }
      else props.put(a(0), a(1))
    }
    props
  }
}
