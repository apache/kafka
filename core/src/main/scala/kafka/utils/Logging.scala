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

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.{Marker, MarkerFactory}

import scala.util.{Failure, Success, Try}

object Log4jControllerRegistration {
  private val logger = Logger(this.getClass.getName)

  val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")

  val instance = Try {
    val log4jController = Class.forName("kafka.utils.Log4jController").asInstanceOf[Class[Object]]
    val instance = log4jController.newInstance()
    CoreUtils.registerMBean(instance, "kafka:type=kafka.Log4jController")
    instance
  }

  instance match {
    case Success(_) => logger.info("Registered kafka:type=kafka.Log4jController MBean")
    case Failure(_) => logger.info("Couldn't register kafka:type=kafka.Log4jController MBean")
  }
}

trait Logging extends LazyLogging {
  def loggerName: String = this.getClass.getName

  protected var logIdent: String = _

  Log4jControllerRegistration

  protected def msgWithLogIdent(msg: String): String =
    if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = logger.trace(msgWithLogIdent(msg))

  // The return type here should be Unit as in any other logging function in this class.
  // However making it so will cause the compiler to believe that the signature of this
  // and the trace above will be the same as the parameters are generic Function objects
  // subject to type erasure. After the erasure the signature of the two function would
  // be the same.
  def trace(e: => Throwable): Any = logger.trace(logIdent,e)

  def trace(msg: => String, e: => Throwable): Unit = logger.trace(msgWithLogIdent(msg),e)

  def swallowTrace(action: => Unit): Unit = CoreUtils.swallow(logger.underlying.trace, action)

  def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

  def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

  def debug(msg: => String): Unit = logger.debug(msgWithLogIdent(msg))

  def debug(e: => Throwable): Any = logger.debug(logIdent,e)

  def debug(msg: => String, e: => Throwable): Unit = logger.debug(msgWithLogIdent(msg),e)

  def swallowDebug(action: => Unit): Unit = CoreUtils.swallow(logger.underlying.debug, action)

  def info(msg: => String): Unit = logger.info(msgWithLogIdent(msg))

  def info(e: => Throwable): Any = logger.info(logIdent,e)

  def info(msg: => String,e: => Throwable): Unit = logger.info(msgWithLogIdent(msg),e)

  def swallowInfo(action: => Unit): Unit = CoreUtils.swallow(logger.underlying.info, action)

  def warn(msg: => String): Unit = logger.warn(msgWithLogIdent(msg))

  def warn(e: => Throwable): Any = logger.warn(logIdent,e)

  def warn(msg: => String, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg),e)

  def swallowWarn(action: => Unit): Unit = CoreUtils.swallow(logger.underlying.warn, action)

  def swallow(action: => Unit): Unit = swallowWarn(action)

  def error(msg: => String): Unit = logger.error(msgWithLogIdent(msg))

  def error(e: => Throwable): Any = logger.error(logIdent,e)

  def error(msg: => String, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg),e)

  def swallowError(action: => Unit): Unit = CoreUtils.swallow(logger.underlying.error, action)

  def fatal(msg: => String): Unit =
    logger.error(Log4jControllerRegistration.FatalMarker, msgWithLogIdent(msg))

  def fatal(e: => Throwable): Any =
    logger.error(Log4jControllerRegistration.FatalMarker, logIdent, e)

  def fatal(msg: => String, e: => Throwable): Unit =
    logger.error(Log4jControllerRegistration.FatalMarker, msgWithLogIdent(msg), e)
}
