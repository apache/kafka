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
import collection.mutable
import joptsimple.OptionParser


object DumpLogSegments {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val printOpt = parser.accepts("print-data-log", "if set, printing the messages content when dumping data logs")
                           .withOptionalArg
                           .describedAs("print data log content")
                           .ofType(classOf[java.lang.Boolean])
                           .defaultsTo(false)

    val verifyOpt = parser.accepts("verify-index-only", "if set, just verify the index log without printing its content")
                           .withOptionalArg
                           .describedAs("just verify the index log")
                           .ofType(classOf[java.lang.Boolean])
                           .defaultsTo(false)

    val filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped")
                           .withRequiredArg
                           .describedAs("file1, file2, ...")
                           .ofType(classOf[String])

    val options = parser.parse(args : _*)
    if(!options.has(filesOpt)) {
      System.err.println("Missing required argument \"" + filesOpt + "\"")
      parser.printHelpOn(System.err)
      System.exit(1)
    }

    val print = if(options.has(printOpt)) true else false
    val verifyOnly = if(options.has(verifyOpt)) true else false
    val files = options.valueOf(filesOpt).split(",")

    val misMatchesForIndexFilesMap = new mutable.HashMap[String, List[(Long, Long)]]
    val nonConsecutivePairsForLogFilesMap = new mutable.HashMap[String, List[(Long, Long)]]

    for(arg <- files) {
      val file = new File(arg)
      if(file.getName.endsWith(Log.LogFileSuffix)) {
        println("Dumping " + file)
        dumpLog(file, print, nonConsecutivePairsForLogFilesMap)
      } else if(file.getName.endsWith(Log.IndexFileSuffix)) {
        println("Dumping " + file)
        dumpIndex(file, verifyOnly, misMatchesForIndexFilesMap)
      }
    }
    misMatchesForIndexFilesMap.foreach {
      case (fileName, listOfMismatches) => {
        System.err.println("Mismatches in :" + fileName)
        listOfMismatches.foreach(m => {
          System.err.println("  Index position: %d, log position: %d".format(m._1, m._2))
        })
      }
    }
    nonConsecutivePairsForLogFilesMap.foreach {
      case (fileName, listOfNonSecutivePairs) => {
        System.err.println("Non-secutive offsets in :" + fileName)
        listOfNonSecutivePairs.foreach(m => {
          System.err.println("  %d is followed by %d".format(m._1, m._2))
        })
      }
    }
  }
  
  /* print out the contents of the index */
  private def dumpIndex(file: File, verifyOnly: Boolean, misMatchesForIndexFilesMap: mutable.HashMap[String, List[(Long, Long)]]) {
    val startOffset = file.getName().split("\\.")(0).toLong
    val logFileName = file.getAbsolutePath.split("\\.")(0) + Log.LogFileSuffix
    val logFile = new File(logFileName)
    val messageSet = new FileMessageSet(logFile)
    val index = new OffsetIndex(file = file, baseOffset = startOffset)
    for(i <- 0 until index.entries) {
      val entry = index.entry(i)
      val partialFileMessageSet: FileMessageSet = messageSet.read(entry.position, messageSet.sizeInBytes())
      val messageAndOffset = partialFileMessageSet.head
      if(messageAndOffset.offset != entry.offset + index.baseOffset) {
        var misMatchesSeq = misMatchesForIndexFilesMap.getOrElse(file.getName, List[(Long, Long)]())
        misMatchesSeq ::=(entry.offset + index.baseOffset, messageAndOffset.offset)
        misMatchesForIndexFilesMap.put(file.getName, misMatchesSeq)
      }
      // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
      if(entry.offset == 0 && i > 0)
        return
      if (!verifyOnly)
        println("offset: %d position: %d".format(entry.offset + index.baseOffset, entry.position))
    }
  }
  
  /* print out the contents of the log */
  private def dumpLog(file: File, printContents: Boolean, nonConsecutivePairsForLogFilesMap: mutable.HashMap[String, List[(Long, Long)]]) {
    val startOffset = file.getName().split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val messageSet = new FileMessageSet(file)
    var validBytes = 0L
    var lastOffset = -1l
    for(messageAndOffset <- messageSet) {
      val msg = messageAndOffset.message

      if(lastOffset == -1)
        lastOffset = messageAndOffset.offset
      // If it's uncompressed message, its offset must be lastOffset + 1 no matter last message is compressed or uncompressed
      else if (msg.compressionCodec == NoCompressionCodec && messageAndOffset.offset != lastOffset +1) {
        var nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.getOrElse(file.getName, List[(Long, Long)]())
        nonConsecutivePairsSeq ::=(lastOffset, messageAndOffset.offset)
        nonConsecutivePairsForLogFilesMap.put(file.getName, nonConsecutivePairsSeq)
      }
      lastOffset = messageAndOffset.offset

      print("offset: " + messageAndOffset.offset + " position: " + validBytes + " isvalid: " + msg.isValid +
            " payloadsize: " + msg.payloadSize + " magic: " + msg.magic +
            " compresscodec: " + msg.compressionCodec + " crc: " + msg.checksum)
      validBytes += MessageSet.entrySize(msg)
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
