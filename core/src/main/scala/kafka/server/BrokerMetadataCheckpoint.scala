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

import kafka.common.{InconsistentBrokerMetadataException, KafkaException}
import kafka.server.RawMetaProperties._
import kafka.utils._
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object RawMetaProperties {
  val ClusterIdKey = "cluster.id"
  val BrokerIdKey = "broker.id"
  val NodeIdKey = "node.id"
  val VersionKey = "version"
}

class RawMetaProperties(val props: Properties = new Properties()) {

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

  def nodeId: Option[Int] = {
    intValue(NodeIdKey)
  }

  def nodeId_=(id: Int): Unit = {
    props.setProperty(NodeIdKey, id.toString)
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

  override def toString: String = {
    "{" + props.keySet().asScala.toList.asInstanceOf[List[String]].sorted.map {
      key => key + "=" + props.get(key)
    }.mkString(", ") + "}"
  }
}

object MetaProperties {
  def parse(properties: RawMetaProperties): MetaProperties = {
    properties.requireVersion(expectedVersion = 1)
    val clusterId = require(ClusterIdKey, properties.clusterId)
    val nodeId = require(NodeIdKey, properties.nodeId)
    new MetaProperties(clusterId, nodeId)
  }

  def require[T](key: String, value: Option[T]): T = {
    value.getOrElse(throw new RuntimeException(s"Failed to find required property $key."))
  }
}

case class ZkMetaProperties(
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
    s"ZkMetaProperties(brokerId=$brokerId, clusterId=$clusterId)"
  }
}

case class MetaProperties(
  clusterId: String,
  nodeId: Int,
) {
  def toProperties: Properties = {
    val properties = new RawMetaProperties()
    properties.version = 1
    properties.clusterId = clusterId
    properties.nodeId = nodeId
    properties.props
  }

  override def toString: String  = {
    s"MetaProperties(clusterId=$clusterId, nodeId=$nodeId)"
  }
}

object BrokerMetadataCheckpoint extends Logging {
  def getBrokerMetadataAndOfflineDirs(
    logDirs: collection.Seq[String],
    ignoreMissing: Boolean
  ): (RawMetaProperties, collection.Seq[String]) = {
    require(logDirs.nonEmpty, "Must have at least one log dir to read meta.properties")

    val brokerMetadataMap = mutable.HashMap[String, Properties]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- logDirs) {
      val brokerCheckpointFile = new File(logDir, "meta.properties")
      val brokerCheckpoint = new BrokerMetadataCheckpoint(brokerCheckpointFile)

      try {
        brokerCheckpoint.read() match {
          case Some(properties) =>
            brokerMetadataMap += logDir -> properties
          case None =>
            if (!ignoreMissing) {
              throw new KafkaException(s"No `meta.properties` found in $logDir " +
                "(have you run `kafka-storage.sh` to format the directory?)")
            }
        }
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Failed to read $brokerCheckpointFile", e)
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
          fileOutputStream.getFD.sync()
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
    Files.deleteIfExists(new File(file.getPath + ".tmp").toPath) // try to delete any existing temp files for cleanliness

    val absolutePath = file.getAbsolutePath
    lock synchronized {
      try {
        Some(Utils.loadProps(absolutePath))
      } catch {
        case _: NoSuchFileException =>
          warn(s"No meta.properties file under dir $absolutePath")
          None
        case e: Exception =>
          error(s"Failed to read meta.properties file under dir $absolutePath", e)
          throw e
      }
    }
  }
}
