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

import org.apache.kafka.common.utils.Utils
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.{Level, LogManager}

import java.util
import java.util.Locale
import scala.jdk.CollectionConverters._


object Log4jController {

  /**
   * Note: In log4j, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
   *
   * The root logger's name is changed in log4j2 to empty string (see: [[LogManager.ROOT_LOGGER_NAME]]) but for backward-
   * compatibility. Kafka keeps its original root logger name. It is why here is a dedicated definition for the root logger name.
   */
  val ROOT_LOGGER = "root"

  /**
   * Returns a map of the log4j loggers and their assigned log level.
   * If a logger does not have a log level assigned, we return the log level of the first ancestor with a level configured.
   */
  def loggers: Map[String, String] = {
    val logContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val rootLoggerLevel = logContext.getRootLogger.getLevel.toString

    // Loggers defined in the configuration
    val configured = logContext.getConfiguration.getLoggers.asScala
      .values
      .filterNot(_.getName.equals(LogManager.ROOT_LOGGER_NAME))
      .map { logger =>
        logger.getName -> logger.getLevel.toString
      }.toMap

    // Loggers actually running
    val actual = logContext.getLoggers.asScala
      .filterNot(_.getName.equals(LogManager.ROOT_LOGGER_NAME))
      .map { logger =>
        logger.getName -> logger.getLevel.toString
      }.toMap

    (configured ++ actual) + (ROOT_LOGGER -> rootLoggerLevel)
  }

  /**
   * Sets the log level of a particular logger. If the given logLevel is not an available log4j level
   * (i.e., one of OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL) it falls back to DEBUG.
   *
   * @see [[Level.toLevel]]
   */
  def logLevel(loggerName: String, logLevel: String): Boolean = {
    if (Utils.isBlank(loggerName) || Utils.isBlank(logLevel))
      return false

    val level = Level.toLevel(logLevel.toUpperCase(Locale.ROOT))

    if (loggerName == ROOT_LOGGER) {
      Configurator.setAllLevels(LogManager.ROOT_LOGGER_NAME, level)
      true
    } else {
      if (loggerExists(loggerName) && level != null) {
        Configurator.setAllLevels(loggerName, level)
        true
      }
      else false
    }
  }

  def unsetLogLevel(loggerName: String): Boolean = {
    if (loggerName == ROOT_LOGGER) {
      Configurator.setAllLevels(LogManager.ROOT_LOGGER_NAME, null)
      true
    } else {
      if (loggerExists(loggerName)) {
        Configurator.setAllLevels(loggerName, null)
        true
      }
      else false
    }
  }

  def loggerExists(loggerName: String): Boolean = loggers.contains(loggerName)
}

/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 * The companion object contains the singleton instance of this class and
 * registers the MBean. The [[kafka.utils.Logging]] trait forces initialization
 * of the companion object.
 */
class Log4jController extends Log4jControllerMBean {

  def getLoggers: util.List[String] = {
    // we replace scala collection by java collection so mbean client is able to deserialize it without scala library.
    new util.ArrayList[String](Log4jController.loggers.map {
      case (logger, level) => s"$logger=$level"
    }.toSeq.asJava)
  }


  def getLogLevel(loggerName: String): String = {
    Log4jController.loggers.getOrElse(loggerName, "No such logger.")
  }

  def setLogLevel(loggerName: String, level: String): Boolean = Log4jController.logLevel(loggerName, level)
}


trait Log4jControllerMBean {
  def getLoggers: java.util.List[String]
  def getLogLevel(logger: String): String
  def setLogLevel(logger: String, level: String): Boolean
}
