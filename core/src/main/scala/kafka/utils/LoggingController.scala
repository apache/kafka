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

import com.typesafe.scalalogging.Logger
import kafka.utils.LoggingController.ROOT_LOGGER
import org.apache.kafka.common.utils.Utils
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.{Level, LogManager}

import java.util
import java.util.Locale
import scala.jdk.CollectionConverters._


object LoggingController {

  private val logger = Logger[LoggingController]

  /**
   * Note: In Log4j 1, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
   *
   * The root logger's name is changed in log4j2 to empty string (see: [[LogManager.ROOT_LOGGER_NAME]]) but for backward-
   * compatibility. Kafka keeps its original root logger name. It is why here is a dedicated definition for the root logger name.
   */
  val ROOT_LOGGER = "root"

  private[this] val delegate: LoggingControllerDelegate = {
    try {
      new Log4jCoreController
    } catch {
      case _: ClassCastException | _: LinkageError =>
        logger.info("No supported logging implementation found. Logging configuration endpoint will be disabled.")
        new NoOpController
      case e: Exception =>
        logger.warn("A problem occurred, while initializing the logging controller. Logging configuration endpoint will be disabled.", e)
        new NoOpController
    }
  }

  /**
   * Returns a map of the log4j loggers and their assigned log level.
   * If a logger does not have a log level assigned, we return the log level of the first ancestor with a level configured.
   */
  def loggers: Map[String, String] = delegate.loggers

  /**
   * Sets the log level of a particular logger. If the given logLevel is not an available level
   * (i.e., one of OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL) it falls back to DEBUG.
   *
   * @see [[Level.toLevel]]
   */
  def logLevel(loggerName: String, logLevel: String): Boolean = delegate.logLevel(loggerName, logLevel)

  def unsetLogLevel(loggerName: String): Boolean = delegate.unsetLogLevel(loggerName)

  def loggerExists(loggerName: String): Boolean = delegate.loggerExists(loggerName)
}

private class NoOpController extends LoggingControllerDelegate {
  override def loggers: Map[String, String] = Map.empty

  override def logLevel(loggerName: String, logLevel: String): Boolean = false

  override def unsetLogLevel(loggerName: String): Boolean = false
}

private class Log4jCoreController extends LoggingControllerDelegate {
  private[this] val logContext = LogManager.getContext(false).asInstanceOf[LoggerContext]

  override def loggers: Map[String, String] = {
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

  override def logLevel(loggerName: String, logLevel: String): Boolean = {
    if (Utils.isBlank(loggerName) || Utils.isBlank(logLevel))
      return false

    val level = Level.toLevel(logLevel.toUpperCase(Locale.ROOT))

    if (loggerName == ROOT_LOGGER) {
      Configurator.setLevel(LogManager.ROOT_LOGGER_NAME, level)
      true
    } else {
      if (loggerExists(loggerName) && level != null) {
        Configurator.setLevel(loggerName, level)
        true
      }
      else false
    }
  }

  override def unsetLogLevel(loggerName: String): Boolean = {
    val nullLevel: Level = null
    if (loggerName == ROOT_LOGGER) {
      Configurator.setLevel(LogManager.ROOT_LOGGER_NAME, nullLevel)
      true
    } else {
      if (loggerExists(loggerName)) {
        Configurator.setLevel(loggerName, nullLevel)
        true
      }
      else false
    }
  }
}

private abstract class LoggingControllerDelegate {
  def loggers: Map[String, String]
  def logLevel(loggerName: String, logLevel: String): Boolean
  def unsetLogLevel(loggerName: String): Boolean
  def loggerExists(loggerName: String): Boolean = loggers.contains(loggerName)
}

/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 * The companion object contains the singleton instance of this class and
 * registers the MBean. The [[kafka.utils.Logging]] trait forces initialization
 * of the companion object.
 */
class LoggingController extends LoggingControllerMBean {

  def getLoggers: util.List[String] = {
    // we replace scala collection by java collection so mbean client is able to deserialize it without scala library.
    new util.ArrayList[String](LoggingController.loggers.map {
      case (logger, level) => s"$logger=$level"
    }.toSeq.asJava)
  }

  def getLogLevel(loggerName: String): String = {
    LoggingController.loggers.getOrElse(loggerName, "No such logger.")
  }

  def setLogLevel(loggerName: String, level: String): Boolean = LoggingController.logLevel(loggerName, level)
}

trait LoggingControllerMBean {
  def getLoggers: java.util.List[String]
  def getLogLevel(logger: String): String
  def setLogLevel(logger: String, level: String): Boolean
}
