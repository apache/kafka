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
package kafka.server

import kafka.utils.Logging
import kafka.common._
import java.util.concurrent.locks.ReentrantLock
import java.io._

/**
 * This class handles the read/write to the highwaterMark checkpoint file. The file stores the high watermark value for
 * all topics and partitions that this broker hosts. The format of this file is as follows -
 * version
 * number of entries
 * topic partition highwatermark
 */

object HighwaterMarkCheckpoint {
  val highWatermarkFileName = "replication-offset-checkpoint"
  val currentHighwaterMarkFileVersion = 0
}

class HighwaterMarkCheckpoint(val path: String) extends Logging {
  /* create the highwatermark file handle for all partitions */
  val name = path + "/" + HighwaterMarkCheckpoint.highWatermarkFileName
  private val hwFile = new File(name)
  private val hwFileLock = new ReentrantLock()
  // recover from previous tmp file, if required

  def write(highwaterMarksPerPartition: Map[TopicAndPartition, Long]) {
    hwFileLock.lock()
    try {
      // write to temp file and then swap with the highwatermark file
      val tempHwFile = new File(hwFile + ".tmp")

      val hwFileWriter = new BufferedWriter(new FileWriter(tempHwFile))
      // checkpoint highwatermark for all partitions
      // write the current version
      hwFileWriter.write(HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion.toString)
      hwFileWriter.newLine()
      // write the number of entries in the highwatermark file
      hwFileWriter.write(highwaterMarksPerPartition.size.toString)
      hwFileWriter.newLine()

      highwaterMarksPerPartition.foreach { partitionAndHw =>
        hwFileWriter.write("%s %s %s".format(partitionAndHw._1.topic, partitionAndHw._1.partition, partitionAndHw._2))
        hwFileWriter.newLine()
      }
      hwFileWriter.flush()
      hwFileWriter.close()
      // swap new high watermark file with previous one
      if(!tempHwFile.renameTo(hwFile)) {
        fatal("Attempt to swap the new high watermark file with the old one failed")
        System.exit(1)
      }
    }finally {
      hwFileLock.unlock()
    }
  }

  def read(topic: String, partition: Int): Long = {
    hwFileLock.lock()
    try {
      hwFile.length() match {
        case 0 => 
          warn("No highwatermark file is found. Returning 0 as the highwatermark for partition [%s,%d]".format(topic, partition))
          0L
        case _ =>
          val hwFileReader = new BufferedReader(new FileReader(hwFile))
          val version = hwFileReader.readLine().toShort
          version match {
            case HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion =>
              val numberOfHighWatermarks = hwFileReader.readLine().toInt
              val partitionHighWatermarks =
                for(i <- 0 until numberOfHighWatermarks) yield {
                  val nextHwEntry = hwFileReader.readLine()
                  val partitionHwInfo = nextHwEntry.split(" ")
                  val topic = partitionHwInfo(0)
                  val partitionId = partitionHwInfo(1).toInt
                  val highWatermark = partitionHwInfo(2).toLong
                  (TopicAndPartition(topic, partitionId) -> highWatermark)
                }
              hwFileReader.close()
              val hwOpt = partitionHighWatermarks.toMap.get(TopicAndPartition(topic, partition))
              hwOpt match {
                case Some(hw) => 
                  debug("Read hw %d for partition [%s,%d] from highwatermark checkpoint file".format(hw, topic, partition))
                  hw
                case None => 
                  warn("No previously checkpointed highwatermark value found for topic %s ".format(topic) +
                       "partition %d. Returning 0 as the highwatermark".format(partition))
                  0L
              }
            case _ => fatal("Unrecognized version of the highwatermark checkpoint file " + version)
            System.exit(1)
            -1L
          }
      }
    }finally {
      hwFileLock.unlock()
    }
  }
}