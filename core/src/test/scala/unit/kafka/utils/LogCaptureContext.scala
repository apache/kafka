/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.utils

import org.apache.kafka.test.TestUtils

import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.test.appender.ListAppender

import scala.jdk.CollectionConverters._

class LogCaptureContext(listAppender: ListAppender, prevLevelMap: Map[String, Level]) extends AutoCloseable {

  def setLatch(size: Int): Unit = {
    this.listAppender.countDownLatch = new CountDownLatch(size)
  }

  @throws[InterruptedException]
  def await(l: Long, timeUnit: TimeUnit): Unit = {
    this.listAppender.countDownLatch.await(l, timeUnit)
  }

  def getMessages: Seq[LogEvent] = listAppender.getEvents.asScala.toSeq

  override def close(): Unit = {
    val loggerContext = LoggerContext.getContext(false)
    loggerContext.getRootLogger.removeAppender(listAppender)
    listAppender.stop()

    // Restore previous logger levels
    prevLevelMap.foreach { e =>
      val loggerName = e._1
      val level = e._2
      loggerContext.getLogger(loggerName).setLevel(level)
    }
  }
}

/**
 * This class provides an isolated logging context for logging tests. You can also set the logging
 * level of the loggers for a given context differently.
 *
 * By default, the context uses the definition in src/test/resources/log4j2.properties:
 *
 * {{{
 *     // Creates a logging context with default configurations
 *     val logCaptureContext = LogCaptureContext(Map(classOf[AppInfoParser].getName -> "WARN"))
 *     try {
 *         ...
 *     } finally {
 *         logCaptureContext.close
 *     }
 * }}}
 *
 * You can override the default logging levels by passing a map from the logger name to the desired level, like:
 *
 * {{{
 *     // A logging context with default configuration, but 'foo.bar' logger's level is set to WARN.
 *     val logCaptureContext = LogCaptureContext(Map("foo.bar" -> "WARN"))
 *     try {
 *         ...
 *     } finally {
 *         logCaptureContext.close
 *     }
 * }}}
 *
 * Since the logging messages are appended asynchronously, you should wait until the appender process
 * the given messages with [[LogCaptureContext.setLatch(Int)]] and [[LogCaptureContext.await(Long, TimeUnit)]] methods, like:
 *
 * {{{
 *     // A logging context with default configuration, but 'foo.bar' logger's level is set to WARN.
 *     val logCaptureContext = LogCaptureContext()
 *     try {
 *         // We expect there will be at least 5 logging messages.
 *         logCaptureContext.setLatch(5);
 *
 *         // The routine to test ...
 *
 *         // Wait for the appender to finish processing the logging messages, 10 seconds in maximum.
 *         logCaptureContext.await(10, TimeUnit.SECONDS)
 *         val event = logCaptureContext.getMessages.find(...)
 *     } finally {
 *         logCaptureContext.close
 *     }
 * }}}
 *
 * Note: The tests may hang up if you set the messages count too high.
 */
object LogCaptureContext {
  def apply(levelMap: Map[String, String] = Map()): LogCaptureContext = {
    val loggerContext = LoggerContext.getContext(false)
    val listAppender = ListAppender.createAppender("logger-context-" + TestUtils.randomString(8),
      false, false, null, null)
    listAppender.start
    loggerContext.getConfiguration.addAppender(listAppender)
    loggerContext.getRootLogger.addAppender(listAppender)

    // Store the previous logger levels
    val preLevelMap = levelMap.keys.map { loggerName =>
      (loggerName, loggerContext.getLogger(loggerName).getLevel)
    }.toMap

    // Change the logger levels
    levelMap.foreach { e =>
      val loggerName = e._1
      val level = e._2
      loggerContext.getLogger(loggerName).setLevel(Level.getLevel(level))
    }

    new LogCaptureContext(listAppender, preLevelMap)
  }
}
