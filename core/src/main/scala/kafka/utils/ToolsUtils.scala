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
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{Metric, MetricName}

import scala.collection.mutable

object ToolsUtils {

  def validatePortOrDie(parser: OptionParser, hostPort: String) = {
    val hostPorts: Array[String] = if(hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
    val validHostPort = hostPorts.filter { hostPortData =>
      org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null
    }
    val isValid = !validHostPort.isEmpty && validHostPort.size == hostPorts.length
    if(!isValid)
      CommandLineUtils.printUsageAndDie(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")
  }

  def maybeResonseCallback(responseCallbackOpt: Option[Errors => Unit],
                           errors: Errors): Unit = {
    if (responseCallbackOpt.isDefined) {
      val responseErrors = errors match {
        case errors@(Errors.NONE | Errors.NOT_COORDINATOR | Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
          errors
        case _ =>
          Errors.UNKNOWN_SERVER_ERROR
      }
      responseCallbackOpt.get(responseErrors)
    }
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
}
