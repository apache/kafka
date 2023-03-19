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
package kafka.utils

import joptsimple.OptionParser
import kafka.server.{BrokerMetadataCheckpoint, MetaProperties}
import kafka.tools.TerseFailure
import org.apache.kafka.common.{Metric, MetricName}
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.util.CommandLineUtils

import java.io.PrintStream
import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.collection.mutable

object ToolsUtils {

  def validatePortOrDie(parser: OptionParser, hostPort: String): Unit = {
    val hostPorts: Array[String] = if(hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
    val validHostPort = hostPorts.filter { hostPortData =>
      org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null
    }
    val isValid = !validHostPort.isEmpty && validHostPort.size == hostPorts.length
    if (!isValid)
      CommandLineUtils.printUsageAndExit(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")
  }

  /**
    * print out the metrics in alphabetical order
    * @param metrics  the metrics to be printed out
    */
  def printMetrics(metrics: mutable.Map[MetricName, _ <: Metric]): Unit = {
    var maxLengthOfDisplayName = 0

    val sortedMap = metrics.toSeq.sortWith( (s,t) =>
      Array(s._1.group(), s._1.name(), s._1.tags()).mkString(":")
        .compareTo(Array(t._1.group(), t._1.name(), t._1.tags()).mkString(":")) < 0
    ).map {
      case (key, value) =>
        val mergedKeyName = Array(key.group(), key.name(), key.tags()).mkString(":")
        if (maxLengthOfDisplayName < mergedKeyName.length) {
          maxLengthOfDisplayName = mergedKeyName.length
        }
        (mergedKeyName, value.metricValue)
    }
    println(s"\n%-${maxLengthOfDisplayName}s   %s".format("Metric Name", "Value"))
    sortedMap.foreach {
      case (metricName, value) =>
        val specifier = value match {
          case _ @ (_: java.lang.Float | _: java.lang.Double) => "%.3f"
          case _ => "%s"
        }
        println(s"%-${maxLengthOfDisplayName}s : $specifier".format(metricName, value))
    }
  }

  /**
   * This is a simple wrapper around `CommandLineUtils.printUsageAndExit`.
   * It is needed for tools migration (KAFKA-14525), as there is no Java equivalent for return type `Nothing`.
   * Can be removed once [[kafka.admin.ConsumerGroupCommand]], [[kafka.tools.ConsoleConsumer]]
   * and [[kafka.tools.ConsoleProducer]] are migrated.
   *
   * @param parser Command line options parser.
   * @param message Error message.
   */
  def printUsageAndExit(parser: OptionParser, message: String): Nothing = {
    CommandLineUtils.printUsageAndExit(parser, message)
    throw new AssertionError("printUsageAndExit should not return, but it did.")
  }

  def formatCommand(stream: PrintStream,
                    directories: Seq[String],
                    metaProperties: MetaProperties,
                    metadataVersion: MetadataVersion,
                    ignoreFormatted: Boolean): Int = {
    if (directories.isEmpty) {
      throw new TerseFailure("No log directories found in the configuration.")
    }
    val unformattedDirectories = directories.filter(directory => {
      if (!Files.isDirectory(Paths.get(directory)) || !Files.exists(Paths.get(directory, "meta.properties"))) {
        true
      } else if (!ignoreFormatted) {
        throw new TerseFailure(s"Log directory $directory is already formatted. " +
          "Use --ignore-formatted to ignore this directory and format the others.")
      } else {
        false
      }
    })
    if (unformattedDirectories.isEmpty) {
      stream.println("All of the log directories are already formatted.")
    }
    unformattedDirectories.foreach(directory => {
      try {
        Files.createDirectories(Paths.get(directory))
      } catch {
        case e: Throwable => throw new TerseFailure(s"Unable to create storage " +
          s"directory $directory: ${e.getMessage}")
      }
      val metaPropertiesPath = Paths.get(directory, "meta.properties")
      val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesPath.toFile)
      checkpoint.write(metaProperties.toProperties)

      val bootstrapMetadata = BootstrapMetadata.fromVersion(metadataVersion, "format command")
      val bootstrapDirectory = new BootstrapDirectory(directory, Optional.empty())
      bootstrapDirectory.writeBinaryFile(bootstrapMetadata)

      stream.println(s"Formatting ${directory} with metadata.version ${metadataVersion}.")
    })
    0
  }
}
