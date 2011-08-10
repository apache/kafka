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

import java.io._
import kafka.message._
import kafka.utils._

object DumpLogSegments {

  def main(args: Array[String]) {
    var isNoPrint = false;
    for(arg <- args)
      if ("-noprint".compareToIgnoreCase(arg) == 0)
        isNoPrint = true;

    for(arg <- args) {
      if (! ("-noprint".compareToIgnoreCase(arg) == 0) ) {
        val file = new File(arg)
        println("Dumping " + file)
        var offset = file.getName().split("\\.")(0).toLong
        println("Starting offset: " + offset)
        val messageSet = new FileMessageSet(file, false)
        for(messageAndOffset <- messageSet) {
          println("----------------------------------------------")
         if (messageAndOffset.message.isValid)
            println("offset:\t" + offset)
          else
            println("offset:\t %d \t invalid".format(offset))
          if (!isNoPrint)
            println("payload:\t" + Utils.toString(messageAndOffset.message.payload, "UTF-8"))
          offset += messageAndOffset.offset
        }
      }
    }
  }
  
}
