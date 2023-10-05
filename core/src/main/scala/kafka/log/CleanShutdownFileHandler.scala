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

package kafka.log

import kafka.utils.Logging

import java.io.File
import java.io.FileOutputStream
import java.io.BufferedWriter
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.regex.Pattern

/**
 * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
 * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
 * avoided by passing in the recovery point, however finding the correct position to do this
 * requires accessing the offset index which may not be safe in an unclean shutdown.
 * For more information see the discussion in PR#2104
 *
 * Also, the clean shutdown file can also store the broker epoch, this can be used in the broker registration to
 * demonstrate the last reboot is a clean shutdown. (KIP-966)
 */

object CleanShutdownFileHandler {
  val CleanShutdownFileName = ".kafka_cleanshutdown"
}

class CleanShutdownFileHandler(dirPath: String) extends Logging {
  val cleanShutdownFile = new File(dirPath, CleanShutdownFileHandler.CleanShutdownFileName)
  val currentVersion = 0

  @throws[Exception]
  def write(brokerEpoch: Long): Unit = {
    write(brokerEpoch, currentVersion)
  }

  // visible to test.
  @throws[Exception]
  def write(brokerEpoch: Long, version: Int): Unit = {
    val os = new FileOutputStream(cleanShutdownFile)
    val bw = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))
    try {
      bw.write("version: " + version + '\n')
      bw.write("brokerEpoch: " + brokerEpoch)
      bw.flush()
      os.getFD.sync()
    } finally bw.close()
  }

  @throws[Exception]
  def read: Long = {
    val br = Files.newBufferedReader(cleanShutdownFile.toPath, StandardCharsets.UTF_8)
    val whiteSpacesPattern = Pattern.compile(":\\s+")
    var brokerEpoch = -1L
    try {
      val versionString = br.readLine
      val versionArray = whiteSpacesPattern.split(versionString)
      if (versionArray.length != 2) {
        throwIOException("can't parse version from \"" + versionString + "\"")
      }
      if (versionArray(1).toInt != currentVersion) {
        throwIOException("Wrong version for the clean shutdown file. version=" + versionArray(1).toInt + "vs expected=" + currentVersion)
      }

      val brokerEpochString = br.readLine
      val brokerEpochArray = whiteSpacesPattern.split(brokerEpochString)
      if (brokerEpochArray.length != 2) {
        throwIOException("can't parse broker epoch from \"" + brokerEpochString + "\"")
      }
      brokerEpoch =brokerEpochArray(1).toLong
    } catch {
      case e: Exception =>
        throw e
    } finally br.close()
    brokerEpoch
  }

  @throws[IOException]
  private def throwIOException(msg: String): Unit = {
    throw new IOException("Fail to parse clean shutdown file: " + msg)
  }

  @throws[Exception]
  def delete(): Unit = {
    Files.deleteIfExists(cleanShutdownFile.toPath)
  }

  def exists(): Boolean = {
    cleanShutdownFile.exists
  }

  override def toString: String = "CleanShutdownFile=(" + "file=" + cleanShutdownFile.toString + ')'
}


