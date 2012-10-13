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
import kafka.log._
import kafka.utils._

object DumpLogSegments {

  def main(args: Array[String]) {
    val print = args.contains("--print")
    val files = args.filter(_ != "--print")

    for(arg <- files) {
      val file = new File(arg)
      if(file.getName.endsWith(Log.LogFileSuffix)) {
        println("Dumping " + file)
        dumpLog(file, print)
      } else if(file.getName.endsWith(Log.IndexFileSuffix)) {
        println("Dumping " + file)
        dumpIndex(file)
      }
    }
  }
  
  /* print out the contents of the index */
  def dumpIndex(file: File) {
    val startOffset = file.getName().split("\\.")(0).toLong
    val index = new OffsetIndex(file = file, baseOffset = startOffset)
    for(i <- 0 until index.entries) {
      val entry = index.entry(i)
      // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
      if(entry.offset <= startOffset)
        return
      println("offset: %d position: %d".format(entry.offset, entry.position))
    }
  }
  
  /* print out the contents of the log */
  def dumpLog(file: File, printContents: Boolean) {
    val startOffset = file.getName().split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val messageSet = new FileMessageSet(file)
    var validBytes = 0L
    for(messageAndOffset <- messageSet) {
      val msg = messageAndOffset.message
      validBytes += MessageSet.entrySize(msg)
      print("offset: " + messageAndOffset.offset + " isvalid: " + msg.isValid +
            " payloadsize: " + msg.payloadSize + " magic: " + msg.magic + 
            " compresscodec: " + msg.compressionCodec + " crc: " + msg.checksum)
      if(msg.hasKey)
        print(" keysize: " + msg.keySize)
      if(printContents) {
        if(msg.hasKey)
          print(" key: " + Utils.readString(messageAndOffset.message.payload, "UTF-8"))
        print(" payload: " + Utils.readString(messageAndOffset.message.payload, "UTF-8"))
      }
      println()
    }
    val trailingBytes = messageSet.sizeInBytes - validBytes
    if(trailingBytes > 0)
      println("Found %d invalid bytes at the end of %s".format(trailingBytes, file.getName))
  }
  
}
