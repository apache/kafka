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
        val startOffset = file.getName().split("\\.")(0).toLong
        var offset = 0L
        println("Starting offset: " + startOffset)
        val messageSet = new FileMessageSet(file, false)
        for(messageAndOffset <- messageSet) {
          val msg = messageAndOffset.message
          println("offset: " + (startOffset + offset) + " isvalid: " + msg.isValid +
                  " payloadsize: " + msg.payloadSize + " magic: " + msg.magic + " compresscodec: " + msg.compressionCodec)
          if (!isNoPrint)
            println("payload:\t" + Utils.toString(messageAndOffset.message.payload, "UTF-8"))
          offset = messageAndOffset.offset
        }
        println("tail of the log is at offset: " + (startOffset + offset)) 
      }
    }
  }
  
}
