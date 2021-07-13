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

object LogCaptureContext {
  def apply(name: String, levelMap: Map[String, String] = Map()): LogCaptureContext = {
    val loggerContext = LoggerContext.getContext(false)
    val listAppender = ListAppender.createAppender(name,
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
