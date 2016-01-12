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

import scala.collection._
import kafka.utils.Logging
import kafka.common._
import java.io._

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
class OffsetCheckpoint(val file: File) extends Logging {
  private val lock = new Object()
  file.createNewFile() // in case the file doesn't exist

  def write(offsets: Map[TopicAndPartition, Long]) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      val temp = new File(file.getAbsolutePath + ".tmp")

      val fileOutputStream = new FileOutputStream(temp)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        // write the current version
        writer.write(0.toString)
        writer.newLine()
      
        // write the number of entries
        writer.write(offsets.size.toString)
        writer.newLine()

        // write the entries
        offsets.foreach { case (topicPart, offset) =>
          writer.write("%s %d %d".format(topicPart.topic, topicPart.partition, offset))
          writer.newLine()
        }
      
        // flush the buffer and then fsync the underlying file
        writer.flush()
        fileOutputStream.getFD().sync()
      } finally {
        writer.close()
      }
      
      // swap new offset checkpoint file with previous one
      if(!temp.renameTo(file)) {
        // renameTo() fails on Windows if the destination file exists.
        file.delete()
        if(!temp.renameTo(file))
          throw new IOException("File rename from %s to %s failed.".format(temp.getAbsolutePath, file.getAbsolutePath))
      }
    }
  }

  def read(): Map[TopicAndPartition, Long] = {
    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      try {
        var line = reader.readLine()
        if(line == null)
          return Map.empty
        val version = line.toInt
        version match {
          case 0 =>
            line = reader.readLine()
            if(line == null)
              return Map.empty
            val expectedSize = line.toInt
            var offsets = Map[TopicAndPartition, Long]()
            line = reader.readLine()
            while(line != null) {
              val pieces = line.split("\\s+")
              if(pieces.length != 3)
                throw new IOException("Malformed line in offset checkpoint file: '%s'.".format(line))
              
              val topic = pieces(0)
              val partition = pieces(1).toInt
              val offset = pieces(2).toLong
              offsets += (TopicAndPartition(topic, partition) -> offset)
              line = reader.readLine()
            }
            if(offsets.size != expectedSize)
              throw new IOException("Expected %d entries but found only %d".format(expectedSize, offsets.size))
            offsets
          case _ => 
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } finally {
        reader.close()
      }
    }
  }
  
}