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
import org.slf4j.{LoggerFactory, Marker, MarkerFactory}


object Log4jControllerRegistration {
  private val logger = Logger(this.getClass.getName)

  private val loggingMBean = new LoggingController
  registerMBean(loggingMBean, "kafka.Log4jController")

  private def registerMBean(mbean: LoggingController, typeAttr: String): Unit = {
    try {
      CoreUtils.registerMBean(mbean, s"kafka:type=$typeAttr")
      logger.info("Registered `kafka:type={}` MBean", typeAttr)
    } catch {
      case e: Exception => logger.warn("Couldn't register `kafka:type={}` MBean", typeAttr, e)
    }
  }
}

private object Logging {
  private val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")
}

trait Logging {

  protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(loggerName))

  protected var logIdent: String = _

  Log4jControllerRegistration

  protected def loggerName: String = getClass.getName

  protected def msgWithLogIdent(msg: String): String =
    if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = logger.trace(msgWithLogIdent(msg))

  def trace(msg: => String, e: => Throwable): Unit = logger.trace(msgWithLogIdent(msg),e)

  def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

  def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

  def debug(msg: => String): Unit = logger.debug(msgWithLogIdent(msg))

  def debug(msg: => String, e: => Throwable): Unit = logger.debug(msgWithLogIdent(msg),e)

  def info(msg: => String): Unit = logger.info(msgWithLogIdent(msg))

  def info(msg: => String,e: => Throwable): Unit = logger.info(msgWithLogIdent(msg),e)

  def warn(msg: => String): Unit = logger.warn(msgWithLogIdent(msg))

  def warn(msg: => String, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg),e)

  def error(msg: => String): Unit = logger.error(msgWithLogIdent(msg))

  def error(msg: => String, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg),e)

  def fatal(msg: => String): Unit =
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg))

  def fatal(msg: => String, e: => Throwable): Unit =
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg), e)
}
