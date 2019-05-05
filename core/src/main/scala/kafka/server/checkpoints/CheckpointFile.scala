/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server.checkpoints

import java.io.{BufferedReader, File, FileOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.util

import kafka.server.LogDirFailureChannel
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

final case class CheckpointFile[T <: CheckpointFileEntry](file: File,
                                                          version: Int,
                                                          logDirFailureChannel: LogDirFailureChannel,
                                                          logDir: String,
                                                          checkpointFileEntryBuilder: String => T) {
  val path: Path = file.toPath.toAbsolutePath
  val tempPath: Path = Paths.get(path.toString + ".tmp")
  val stringBuilder: StringBuilder = new StringBuilder()

  try {
    Files.createFile(file.toPath)
  } catch {
    case _: FileAlreadyExistsException =>
  }

  def write(entries: Seq[T]): Unit = {
    this.synchronized {
      try {
        val fileOutputStream = new FileOutputStream(tempPath.toFile)
        try {
          stringBuilder.setLength(0)
          stringBuilder
            .append(version)
            .append('\n')
            .append(entries.size)

          for (entry <- entries) {
            stringBuilder.append('\n')
            entry.writeLine(stringBuilder)
          }
          fileOutputStream.write(stringBuilder.mkString.getBytes(StandardCharsets.UTF_8))
          fileOutputStream.flush()
          fileOutputStream.getFD.sync()
          Utils.atomicMoveWithFallback(tempPath, path);
        } finally {
          fileOutputStream.close()
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while writing to checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  def read(): Seq[T] = {
    this.synchronized {
      try {
        val bufferedReader: BufferedReader = Files.newBufferedReader(path)
        try {
          val checkpointFileReader: CheckpointFileReader[T] = CheckpointFileReader(version, file.getAbsolutePath, bufferedReader, checkpointFileEntryBuilder)
          checkpointFileReader.read()
        } finally {
          bufferedReader.close()
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while reading checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }
}

final case class CheckpointFileReader[T <: CheckpointFileEntry](version: Int,
                                                                filePath: String,
                                                                bufferedReader: BufferedReader,
                                                                checkpointFileEntryBuilder: String => T) {
  def malformedLineException(line: String) =
    new IOException(s"Malformed line in checkpoint file ($filePath): $line'")

  def readInt(): Option[Int] = {
    val intStr = bufferedReader.readLine()
    if (intStr == null)
      None
    else
      Some(Integer.parseInt(intStr))
  }

  def read(): Seq[T] = {
    val maybeFileVersion = readInt()
    val maybeExpectedSize = readInt()
    if (maybeFileVersion.isEmpty || maybeExpectedSize.isEmpty)
      return Seq[T]()

    val fileVersion: Int = maybeFileVersion.get
    val expectedSize: Int = maybeExpectedSize.get

    if (version != fileVersion)
      throw new IOException(s"Unrecognized version of the checkpoint file ($filePath): " + version)

    val entries: util.ArrayList[T] = new util.ArrayList[T]()
    for (line <- bufferedReader.lines().iterator().asScala) {
      try {
        entries.add(checkpointFileEntryBuilder(line))
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      }
    }
    if (entries.size != expectedSize)
      throw new IOException(s"Expected $expectedSize entries in checkpoint file ($filePath), but found only ${entries.size}")
    else
      entries.asScala
  }
}
