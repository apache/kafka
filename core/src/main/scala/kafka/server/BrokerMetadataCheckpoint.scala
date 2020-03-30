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

import java.io._
import java.nio.file.{Files, NoSuchFileException}
import java.util.Properties

import kafka.utils._
import org.apache.kafka.common.utils.Utils

case class BrokerMetadata(brokerId: Int,
                          clusterId: Option[String]) {

  override def toString: String  = {
    s"BrokerMetadata(brokerId=$brokerId, clusterId=${clusterId.map(_.toString).getOrElse("None")})"
  }
}

/**
  * This class saves broker's metadata to a file
  */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  def write(brokerMetadata: BrokerMetadata) = {
    lock synchronized {
      try {
        val brokerMetaProps = new Properties()
        brokerMetaProps.setProperty("version", 0.toString)
        brokerMetaProps.setProperty("broker.id", brokerMetadata.brokerId.toString)
        brokerMetadata.clusterId.foreach { clusterId =>
          brokerMetaProps.setProperty("cluster.id", clusterId)
        }
        val temp = new File(file.getAbsolutePath + ".tmp")
        val fileOutputStream = new FileOutputStream(temp)
        try {
          brokerMetaProps.store(fileOutputStream, "")
          fileOutputStream.flush()
          fileOutputStream.getFD().sync()
        } finally {
          Utils.closeQuietly(fileOutputStream, temp.getName)
        }
        Utils.atomicMoveWithFallback(temp.toPath, file.toPath)
      } catch {
        case ie: IOException =>
          error("Failed to write meta.properties due to", ie)
          throw ie
      }
    }
  }

  def read(): Option[BrokerMetadata] = {
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath()) // try to delete any existing temp files for cleanliness

    lock synchronized {
      try {
        val brokerMetaProps = new VerifiableProperties(Utils.loadProps(file.getAbsolutePath()))
        val version = brokerMetaProps.getIntInRange("version", (0, Int.MaxValue))
        version match {
          case 0 =>
            val brokerId = brokerMetaProps.getIntInRange("broker.id", (0, Int.MaxValue))
            val clusterId = Option(brokerMetaProps.getString("cluster.id", null))
            return Some(BrokerMetadata(brokerId, clusterId))
          case _ =>
            throw new IOException("Unrecognized version of the server meta.properties file: " + version)
        }
      } catch {
        case _: NoSuchFileException =>
          warn("No meta.properties file under dir %s".format(file.getAbsolutePath()))
          None
        case e1: Exception =>
          error("Failed to read meta.properties file under dir %s due to %s".format(file.getAbsolutePath(), e1.getMessage))
          throw e1
      }
    }
  }
}
