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

import kafka.common.InconsistentBrokerMetadataException
import kafka.server.KafkaServer.{BrokerRole, ControllerRole, ProcessRole}
import kafka.server.RawMetaProperties._
import kafka.utils._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object RawMetaProperties {
  val ClusterIdKey = "cluster.id"
  val BrokerIdKey = "broker.id"
  val ControllerIdKey = "controller.id"
  val VersionKey = "version"
}

case class RawMetaProperties(props: Properties = new Properties()) {

  def clusterId: Option[String] = {
    Option(props.getProperty(ClusterIdKey))
  }

  def clusterId_=(id: String): Unit = {
    props.setProperty(ClusterIdKey, id)
  }

  def brokerId: Option[Int] = {
    intValue(BrokerIdKey)
  }

  def brokerId_=(id: Int): Unit = {
    props.setProperty(BrokerIdKey, id.toString)
  }

  def controllerId: Option[Int] = {
    intValue(ControllerIdKey)
  }

  def controllerId_=(id: Int): Unit = {
    props.setProperty(ControllerIdKey, id.toString)
  }

  def version: Int = {
    intValue(VersionKey).getOrElse(0)
  }

  def version_=(ver: Int): Unit = {
    props.setProperty(VersionKey, ver.toString)
  }

  def requireVersion(expectedVersion: Int): Unit = {
    if (version != expectedVersion) {
      throw new RuntimeException(s"Expected version $expectedVersion, but got "+
        s"version $version")
    }
  }

  private def intValue(key: String): Option[Int] = {
    try {
      Option(props.getProperty(key)).map(Integer.parseInt)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $key property " +
        s"as an int: ${e.getMessage}")
    }
  }

}

object MetaProperties {
  def parse(
    properties: RawMetaProperties,
    processRoles: Set[ProcessRole]
  ): MetaProperties = {
    properties.requireVersion(expectedVersion = 1)
    val clusterId = requireClusterId(properties)

    if (processRoles.contains(BrokerRole)) {
      require(BrokerIdKey, properties.brokerId)
    }

    if (processRoles.contains(ControllerRole)) {
      require(ControllerIdKey, properties.controllerId)
    }

    new MetaProperties(clusterId, properties.brokerId, properties.controllerId)
  }

  def require[T](key: String, value: Option[T]): T = {
    value.getOrElse(throw new RuntimeException(s"Failed to find required property $key."))
  }

  def requireClusterId(properties: RawMetaProperties): Uuid = {
    val value = require(ClusterIdKey, properties.clusterId)
    try {
      Uuid.fromString(value)
    } catch {
      case e: Throwable => throw new RuntimeException(s"Failed to parse $ClusterIdKey property " +
        s"as a UUID: ${e.getMessage}")
    }
  }
}

case class LegacyMetaProperties(
  clusterId: String,
  brokerId: Int
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 0
    properties.clusterId = clusterId
    properties.brokerId = brokerId
    properties.props
  }

  override def toString: String = {
    s"LegacyMetaProperties(brokerId=$brokerId, clusterId=$clusterId)"
  }
}

case class MetaProperties(
  clusterId: Uuid,
  brokerId: Option[Int] = None,
  controllerId: Option[Int] = None
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterId.toString
    brokerId.foreach(properties.brokerId = _)
    controllerId.foreach(properties.controllerId = _)
    properties.props
  }

  override def toString: String  = {
    s"MetaProperties(clusterId=$clusterId" +
      s", brokerId=${brokerId.getOrElse("none")}" +
      s", controllerId=${controllerId.getOrElse("none")}" +
      ")"
  }
}

object BrokerMetadataCheckpoint extends Logging {
  def getBrokerMetadataAndOfflineDirs(
    logDirs: collection.Seq[String]
  ): (RawMetaProperties, collection.Seq[String]) = {
    require(logDirs.nonEmpty, "Must have at least one log dir to read meta.properties")

    val brokerMetadataMap = mutable.HashMap[String, Properties]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- logDirs) {
      val brokerCheckpointFile = new File(logDir + File.separator + "meta.properties")
      val brokerCheckpoint = new BrokerMetadataCheckpoint(brokerCheckpointFile)

      try {
        brokerCheckpoint.read().foreach(properties => {
          brokerMetadataMap += logDir -> properties
        })
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerCheckpointFile", e)
      }
    }

    if (brokerMetadataMap.isEmpty) {
      (new RawMetaProperties(), offlineDirs)
    } else {
      val numDistinctMetaProperties = brokerMetadataMap.values.toSet.size
      if (numDistinctMetaProperties > 1) {
        val builder = new StringBuilder

        for ((logDir, brokerMetadata) <- brokerMetadataMap)
          builder ++= s"- $logDir -> $brokerMetadata\n"

        throw new InconsistentBrokerMetadataException(
          s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
            s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
        )
      }

      val rawProps = new RawMetaProperties(brokerMetadataMap.head._2)
      (rawProps, offlineDirs)
    }
  }
}

/**
  * This class saves the metadata properties to a file
  */
class BrokerMetadataCheckpoint(val file: File) extends Logging {
  private val lock = new Object()

  def write(properties: Properties): Unit = {
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
