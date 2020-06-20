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

package kafka.utils

import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent

import scala.collection.mutable.ListBuffer

class LogCaptureAppender extends AppenderSkeleton {
  private val events: ListBuffer[LoggingEvent] = ListBuffer.empty

  override protected def append(event: LoggingEvent): Unit = {
    events.synchronized {
      events += event
    }
  }

  def getMessages: ListBuffer[LoggingEvent] = {
    events.synchronized {
      return events.clone()
    }
  }

  override def close(): Unit = {
    events.synchronized {
      events.clear()
    }
  }

  override def requiresLayout: Boolean = false
}

object LogCaptureAppender {
  def createAndRegister(): LogCaptureAppender = {
    val logCaptureAppender: LogCaptureAppender = new LogCaptureAppender
    Logger.getRootLogger.addAppender(logCaptureAppender)
    logCaptureAppender
  }

  def setClassLoggerLevel(clazz: Class[_], logLevel: Level): Level = {
    val logger = Logger.getLogger(clazz)
    val previousLevel = logger.getLevel
    Logger.getLogger(clazz).setLevel(logLevel)
    previousLevel
  }

  def unregister(logCaptureAppender: LogCaptureAppender): Unit = {
    Logger.getRootLogger.removeAppender(logCaptureAppender)
  }
}
