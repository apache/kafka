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

import joptsimple.{OptionSpec, OptionSet, OptionParser}
import scala.collection.Set

 /**
 * Helper functions for dealing with command line utilities
 */
object CommandLineUtils extends Logging {

  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
    for(arg <- required) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
  }
  
  def checkInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]) {
    if(options.has(usedOption)) {
      for(arg <- invalidOptions) {
        if(options.has(arg)) {
          System.err.println("Option \"" + usedOption + "\" can't be used with option\"" + arg + "\"")
          parser.printHelpOn(System.err)
          System.exit(1)
        }
      }
    }
  }
}