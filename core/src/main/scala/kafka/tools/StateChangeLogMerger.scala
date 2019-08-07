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

import joptsimple._

import scala.util.matching.Regex
import collection.mutable
import java.util.Date
import java.text.SimpleDateFormat

import kafka.utils.{CommandLineUtils, CoreUtils, Exit, Logging}
import java.io.{BufferedOutputStream, OutputStream}
import java.nio.charset.StandardCharsets

import org.apache.kafka.common.internals.Topic

/**
 * A utility that merges the state change logs (possibly obtained from different brokers and over multiple days).
 *
 * This utility expects at least one of the following two arguments -
 * 1. A list of state change log files
 * 2. A regex to specify state change log file names.
 *
 * This utility optionally also accepts the following arguments -
 * 1. The topic whose state change logs should be merged
 * 2. A list of partitions whose state change logs should be merged (can be specified only when the topic argument
 * is explicitly specified)
 * 3. Start time from when the logs should be merged
 * 4. End time until when the logs should be merged
 */

object StateChangeLogMerger extends Logging {

  val dateFormatString = "yyyy-MM-dd HH:mm:ss,SSS"
  val topicPartitionRegex = new Regex("\\[(" + Topic.LEGAL_CHARS + "+),( )*([0-9]+)\\]")
  val dateRegex = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}")
  val dateFormat = new SimpleDateFormat(dateFormatString)
  var files: List[String] = List()
  var topic: String = null
  var partitions: List[Int] = List()
  var startDate: Date = null
  var endDate: Date = null

  def main(args: Array[String]) {

    // Parse input arguments.
    val parser = new OptionParser(false)
    val filesOpt = parser.accepts("logs", "Comma separated list of state change logs or a regex for the log file names")
                              .withRequiredArg
                              .describedAs("file1,file2,...")
                              .ofType(classOf[String])
    val regexOpt = parser.accepts("logs-regex", "Regex to match the state change log files to be merged")
                              .withRequiredArg
                              .describedAs("for example: /tmp/state-change.log*")
                              .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "The topic whose state change logs should be merged")
                              .withRequiredArg
                              .describedAs("topic")
                              .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "Comma separated list of partition ids whose state change logs should be merged")
                              .withRequiredArg
                              .describedAs("0,1,2,...")
                              .ofType(classOf[String])
    val startTimeOpt = parser.accepts("start-time", "The earliest timestamp of state change log entries to be merged")
                              .withRequiredArg
                              .describedAs("start timestamp in the format " + dateFormat)
                              .ofType(classOf[String])
                              .defaultsTo("0000-00-00 00:00:00,000")
    val endTimeOpt = parser.accepts("end-time", "The latest timestamp of state change log entries to be merged")
                              .withRequiredArg
                              .describedAs("end timestamp in the format " + dateFormat)
                              .ofType(classOf[String])
                              .defaultsTo("9999-12-31 23:59:59,999")
                              
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "A tool for merging the log files from several brokers to reconnstruct a unified history of what happened.")


    val options = parser.parse(args : _*)
    if ((!options.has(filesOpt) && !options.has(regexOpt)) || (options.has(filesOpt) && options.has(regexOpt))) {
      System.err.println("Provide arguments to exactly one of the two options \"" + filesOpt + "\" or \"" + regexOpt + "\"")
      parser.printHelpOn(System.err)
      Exit.exit(1)
    }
    if (options.has(partitionsOpt) && !options.has(topicOpt)) {
      System.err.println("The option \"" + topicOpt + "\" needs to be provided an argument when specifying partition ids")
      parser.printHelpOn(System.err)
      Exit.exit(1)
    }

    // Populate data structures.
    if (options.has(filesOpt)) {
      files :::= options.valueOf(filesOpt).split(",").toList
    } else if (options.has(regexOpt)) {
      val regex = options.valueOf(regexOpt)
      val fileNameIndex = regex.lastIndexOf('/') + 1
      val dirName = if (fileNameIndex == 0) "." else regex.substring(0, fileNameIndex - 1)
      val fileNameRegex = new Regex(regex.substring(fileNameIndex))
      files :::= new java.io.File(dirName).listFiles.filter(f => fileNameRegex.findFirstIn(f.getName).isDefined).map(dirName + "/" + _.getName).toList
    }
    if (options.has(topicOpt)) {
      topic = options.valueOf(topicOpt)
    }
    if (options.has(partitionsOpt)) {
      partitions = options.valueOf(partitionsOpt).split(",").toList.map(_.toInt)
      val duplicatePartitions = CoreUtils.duplicates(partitions)
      if (duplicatePartitions.nonEmpty) {
        System.err.println("The list of partitions contains repeated entries: %s".format(duplicatePartitions.mkString(",")))
        Exit.exit(1)
      }
    }
    startDate = dateFormat.parse(options.valueOf(startTimeOpt).replace('\"', ' ').trim)
    endDate = dateFormat.parse(options.valueOf(endTimeOpt).replace('\"', ' ').trim)

    /**
     * n-way merge from m input files:
     * 1. Read a line that matches the specified topic/partitions and date range from every input file in a priority queue.
     * 2. Take the line from the file with the earliest date and add it to a buffered output stream.
     * 3. Add another line from the file selected in step 2 in the priority queue.
     * 4. Flush the output buffer at the end. (The buffer will also be automatically flushed every K bytes.)
     */
    val pqueue = new mutable.PriorityQueue[LineIterator]()(dateBasedOrdering)
    val output: OutputStream = new BufferedOutputStream(System.out, 1024*1024)
    val lineIterators = files.map(scala.io.Source.fromFile(_).getLines)
    var lines: List[LineIterator] = List()

    for (itr <- lineIterators) {
      val lineItr = getNextLine(itr)
      if (!lineItr.isEmpty)
        lines ::= lineItr
    }
    if (lines.nonEmpty) pqueue.enqueue(lines:_*)

    while (pqueue.nonEmpty) {
      val lineItr = pqueue.dequeue()
      output.write((lineItr.line + "\n").getBytes(StandardCharsets.UTF_8))
      val nextLineItr = getNextLine(lineItr.itr)
      if (!nextLineItr.isEmpty)
        pqueue.enqueue(nextLineItr)
    }

    output.flush()
  }

  /**
   * Returns the next line that matches the specified topic/partitions from the file that has the earliest date
   * from the specified date range.
   * @param itr Line iterator of a file
   * @return (line from a file, line iterator for the same file)
   */
  def getNextLine(itr: Iterator[String]): LineIterator = {
    while (itr != null && itr.hasNext) {
      val nextLine = itr.next
      dateRegex.findFirstIn(nextLine).foreach { d =>
        val date = dateFormat.parse(d)
        if ((date.equals(startDate) || date.after(startDate)) && (date.equals(endDate) || date.before(endDate))) {
          topicPartitionRegex.findFirstMatchIn(nextLine).foreach { matcher =>
            if ((topic == null || topic == matcher.group(1)) && (partitions.isEmpty || partitions.contains(matcher.group(3).toInt)))
              return new LineIterator(nextLine, itr)
          }
        }
      }
    }
    new LineIterator()
  }

  class LineIterator(val line: String, val itr: Iterator[String]) {
    def this() = this("", null)
    def isEmpty = line == "" && itr == null
  }

  implicit object dateBasedOrdering extends Ordering[LineIterator] {
    def compare(first: LineIterator, second: LineIterator) = {
      val firstDate = dateRegex.findFirstIn(first.line).get
      val secondDate = dateRegex.findFirstIn(second.line).get
      secondDate.compareTo(firstDate)
    }
  }

}
