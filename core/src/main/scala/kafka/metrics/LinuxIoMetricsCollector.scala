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

package kafka.metrics

import java.nio.file.{Files, Paths}

import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

import scala.jdk.CollectionConverters._

/**
 * Retrieves Linux /proc/self/io metrics.
 */
class LinuxIoMetricsCollector(procRoot: String, val time: Time, val logger: Logger) {
  import LinuxIoMetricsCollector._
  var lastUpdateMs = -1L
  var cachedReadBytes = 0L
  var cachedWriteBytes = 0L
  val path = Paths.get(procRoot, "self", "io")

  def readBytes(): Long = this.synchronized {
    val curMs = time.milliseconds()
    if (curMs != lastUpdateMs) {
      updateValues(curMs)
    }
    cachedReadBytes
  }

  def writeBytes(): Long = this.synchronized {
    val curMs = time.milliseconds()
    if (curMs != lastUpdateMs) {
      updateValues(curMs)
    }
    cachedWriteBytes
  }

  /**
   * Read /proc/self/io.
   *
   * Generally, each line in this file contains a prefix followed by a colon and a number.
   *
   * For example, it might contain this:
   * rchar: 4052
   * wchar: 0
   * syscr: 13
   * syscw: 0
   * read_bytes: 0
   * write_bytes: 0
   * cancelled_write_bytes: 0
   */
  def updateValues(now: Long): Boolean = this.synchronized {
    try {
      cachedReadBytes = -1
      cachedWriteBytes = -1
      val lines = Files.readAllLines(path).asScala
      lines.foreach(line => {
        if (line.startsWith(READ_BYTES_PREFIX)) {
          cachedReadBytes = line.substring(READ_BYTES_PREFIX.size).toLong
        } else if (line.startsWith(WRITE_BYTES_PREFIX)) {
          cachedWriteBytes = line.substring(WRITE_BYTES_PREFIX.size).toLong
        }
      })
      lastUpdateMs = now
      true
    } catch {
      case t: Throwable => {
        logger.warn("Unable to update IO metrics", t)
        false
      }
    }
  }

  def usable(): Boolean = {
    if (path.toFile().exists()) {
      updateValues(time.milliseconds())
    } else {
      logger.debug(s"disabling IO metrics collection because ${path} does not exist.")
      false
    }
  }
}

object LinuxIoMetricsCollector {
  val READ_BYTES_PREFIX = "read_bytes: "
  val WRITE_BYTES_PREFIX = "write_bytes: "
}
