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

  try {
    val log4jController = Class.forName("kafka.utils.Log4jController").asInstanceOf[Class[Object]]
    val instance = log4jController.getDeclaredConstructor().newInstance()
    CoreUtils.registerMBean(instance, "kafka:type=kafka.Log4jController")
    logger.info("Registered kafka:type=kafka.Log4jController MBean")
  } catch {
    case _: Exception => logger.info("Couldn't register kafka:type=kafka.Log4jController MBean")
  }
}

private object Logging {
  private val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")
}

/** Used as an implicit parameter to format log messages */
case class LogIdent(prefix : String)

trait Logging {

  protected lazy val logger = Logger(LoggerFactory.getLogger(loggerName))

  Log4jControllerRegistration

  protected def loggerName: String = getClass.getName

  protected def msgWithLogIdent(msg: String)(implicit logIndent : Option[LogIdent]): String =
    logIndent match {
      case None => msg
      case Some(LogIdent(prefix)) => prefix + msg
    }

  def trace(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.trace(msgWithLogIdent(msg)) else logger.trace(msgWithLogIdent(msg),e)

  def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

  def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

  def debug(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.debug(msgWithLogIdent(msg)) else logger.debug(msgWithLogIdent(msg), e)

  def info(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.info(msgWithLogIdent(msg)) else logger.info(msgWithLogIdent(msg), e)

  def warn(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.warn(msgWithLogIdent(msg)) else logger.warn(msgWithLogIdent(msg),e)

  def error(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.error(msgWithLogIdent(msg)) else logger.error(msgWithLogIdent(msg),e)

  def fatal(msg: => String, e: => Throwable = null)(implicit logIndent : Option[LogIdent] = None): Unit =
    if (e == null) logger.error(Logging.FatalMarker, msgWithLogIdent(msg)) else logger.error(Logging.FatalMarker, msgWithLogIdent(msg), e)
}
