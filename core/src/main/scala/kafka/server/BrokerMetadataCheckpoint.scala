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
import java.util.{Properties, UUID}

import kafka.utils._
import org.apache.kafka.common.utils.Utils

object LegacyMetaProperties {
  def apply(properties: Properties): LegacyMetaProperties = {
    MetaProperties.verifyVersion(properties, 0)
    val brokerId = MetaProperties.getRequiredIntProperty(properties, "broker.id")
    val clusterId = Option(properties.getProperty("cluster.id"))
    new LegacyMetaProperties(brokerId, clusterId)
  }
}

object MetaProperties {
  def apply(properties: Properties): MetaProperties = {
    MetaProperties.verifyVersion(properties, 1)
    val clusterId = getRequiredUuidProperty(properties, "cluster.id")
    new MetaProperties(clusterId)
  }

  def version(properties: Properties): Int = {
    getRequiredIntProperty(properties, "version")
  }

  def verifyVersion(properties: Properties, expectedVersion: Int): Unit = {
    val version = getRequiredIntProperty(properties, "version")
    if (version != expectedVersion) {
      throw new RuntimeException(s"Expected version $expectedVersion, but got "+
        s"version $version")
    }
  }

  def getRequiredStringProperty(properties: Properties, key: String): String = {
    Option(properties.getProperty(key)) match {
      case None => throw new RuntimeException(s"Failed to find required property $key.")
      case Some(value) => value
    }
  }

  def getRequiredUuidProperty(properties: Properties, key: String): UUID = {
    val value = getRequiredStringProperty(properties, key)
    try {
      UUID.fromString(value)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $key property " +
        s"as a UUID: ${e.getMessage}")
    }
  }

  def getRequiredIntProperty(properties: Properties, key: String): Int = {
    val value = getRequiredStringProperty(properties, key)
    try {
      Integer.parseInt(value)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $key property " +
        s"as an int: ${e.getMessage}")
    }
  }
}

case class LegacyMetaProperties(brokerId: Int,
                                clusterId: Option[String]) {
  def toProperties(): Properties = {
    val properties = new Properties()
    properties.setProperty("version", 0.toString)
    properties.setProperty("broker.id", brokerId.toString)
    clusterId.foreach(properties.setProperty("cluster.id", _))
    properties
  }

  override def toString: String  = {
    s"LegacyMetaProperties(brokerId=$brokerId, clusterId=${clusterId.map(_.toString).getOrElse("None")})"
  }
}

case class MetaProperties(clusterId: UUID) {
  def toProperties(): Properties = {
    val properties = new Properties()
    properties.setProperty("version", 1.toString)
    properties.setProperty("cluster.id", clusterId.toString)
    properties
  }

  override def toString: String  = {
    s"MetaProperties(clusterId=$clusterId)"
  }
}

/**
  * This class saves the metadata properties to a file
  */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  def write(properties: Properties) = {
    lock synchronized {
      try {
        val temp = new File(file.getAbsolutePath + ".tmp")
        val fileOutputStream = new FileOutputStream(temp)
        try {
          properties.store(fileOutputStream, "")
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

  def read(): Option[Properties] = {
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath()) // try to delete any existing temp files for cleanliness

    lock synchronized {
      try {
        Some(Utils.loadProps(file.getAbsolutePath()))
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
