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

import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils

import scala.collection._
import kafka.utils.{Exit, Logging}
import kafka.common._
import java.io._

import org.apache.kafka.common.TopicPartition

object OffsetCheckpoint {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
class OffsetCheckpoint(val file: File) extends Logging {
  import OffsetCheckpoint._
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  file.createNewFile() // in case the file doesn't exist

  def write(offsets: Map[TopicPartition, Long]) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        writer.write(CurrentVersion.toString)
        writer.newLine()

        writer.write(offsets.size.toString)
        writer.newLine()

        offsets.foreach { case (topicPart, offset) =>
          writer.write(s"${topicPart.topic} ${topicPart.partition} $offset")
          writer.newLine()
        }

        writer.flush()
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Exit.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }

      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  def read(): Map[TopicPartition, Long] = {

    def malformedLineException(line: String) =
      new IOException(s"Malformed line in offset checkpoint file: $line'")

    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        line = reader.readLine()
        if (line == null)
          return Map.empty
        val version = line.toInt
        version match {
          case CurrentVersion =>
            line = reader.readLine()
            if (line == null)
              return Map.empty
            val expectedSize = line.toInt
            val offsets = mutable.Map[TopicPartition, Long]()
            line = reader.readLine()
            while (line != null) {
              WhiteSpacesPattern.split(line) match {
                case Array(topic, partition, offset) =>
                  offsets += new TopicPartition(topic, partition.toInt) -> offset.toLong
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            if (offsets.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
            offsets
          case _ =>
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
  
}
