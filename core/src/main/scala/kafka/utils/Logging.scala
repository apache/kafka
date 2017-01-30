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

import org.apache.log4j.spi.{LocationInfo, LoggerFactory, LoggingEvent}
import org.apache.log4j.{Logger, Priority}

trait Logging {
  protected val loggerName: String = this.getClass.getName
  protected lazy val logger: Logger =
    Logger.getLogger(loggerName, new TraitAwareLocationInfoProvidingLoggerFactory)
  private[this] val traitImplFqcn = classOf[Logging].getName + "$class"

  protected var logIdent: String = _

  // Force initialization to register Log4jControllerMBean
  private val log4jController = Log4jController

  /**
    * A [[LoggerFactory]] to create a [[Logger]] which does little tricky interpolation to correct
    * logging location of logging which happens through calling [[Logging]] trait method.
    *
    * When a class extends Logging trait, Scala crates dedicated methods of those in [[Logging]] trait in
    * each implementation classes, in order to implement mixin on java which inhibit multiple-inheritance.
    * Hence, if an instance of class invokes [[Logging#warn]] method, the call stack would look like:
    * {{{
    * at org.apache.log4j.Category.log(Category.java:856)
    * at kafka.utils.Logging$class.warn(Logging.scala:154) <-- an exact method defined in trait
    * at kafka.utils.LoggingTest$TestLogger.warn(LoggingTest.scala:42) <-- a method grown in concrete class by mixin
    * at kafka.utils.LoggingTest$TestLogger.logWarnByTraitMethod(LoggingTest.scala:44) <-- this is the practical logging location
    * }}}
    * In order to find a exact location of which the logging performed, we need to dig into call stack and find
    * the first location where the method of [[Logging]] trait called so we can find the practical position
    * of the logging by finding +2 more lower stack element.
    */
  private[this] class TraitAwareLocationInfoProvidingLoggerFactory extends LoggerFactory {
    override def makeNewLoggerInstance(name: String): Logger = new Logger(name) {
      override def forcedLog(fqcn: String, level: Priority, message: Any, t: Throwable): Unit = {
        callAppenders(new LoggingEvent(fqcn, this, level, message, t) {
          private[this] var locationInfoCache: LocationInfo = _

          private[this] def isInvokedThroughTraitMethod(traitElem: StackTraceElement,
                                                        implElem: StackTraceElement): Boolean = {
            try {
              // Scala trait mixin creates a dedicated method in concrete class which implements a trait.
              // If implElem points a method which had defined through processing trait mixin, the class must be
              // an implementation of Logging trait(interface in byte code level) and the method name should
              // corresponds to the one's in Logging trait.
              classOf[Logging].isAssignableFrom(Class.forName(implElem.getClassName)) &&
                traitElem.getMethodName == implElem.getMethodName
            } catch {
              case _: Throwable => false
            }
          }

          private[this] def findPracticalLoggingLocation(): LocationInfo = {
            val callStack = (new Throwable).getStackTrace
            callStack.view.dropWhile { elem =>
              elem.getClassName != traitImplFqcn
            }.take(3).toList match {
              // If this logging invoked through trait method, the call stack should have exactly the expected
              // pattern. See isInvokedThroughTraitMethod for the detail.
              case traitElem :: implElem :: callerElem :: Nil
                if isInvokedThroughTraitMethod(traitElem, implElem) =>
                new LocationInfo(callerElem.getFileName, callerElem.getClassName, callerElem.getMethodName,
                  String.valueOf(callerElem.getLineNumber))
              // Otherwise this logging might be invoked through directly calling logger fields method.
              // just fallback to the original implementation.
              case _ => super.getLocationInformation
            }
          }

          override def getLocationInformation: LocationInfo = {
            if (locationInfoCache == null) {
              locationInfoCache = findPracticalLoggingLocation()
            }
            locationInfoCache
          }
        })
      }
    }
  }

  private def msgWithLogIdent(msg: String) =
    if(logIdent == null) msg else logIdent + msg

  final def trace(msg: => String): Unit = {
    if (logger.isTraceEnabled)
      logger.trace(msgWithLogIdent(msg))
  }
  final def trace(e: => Throwable): Any = {
    if (logger.isTraceEnabled)
      logger.trace(logIdent, e)
  }
  final def trace(msg: => String, e: => Throwable): Unit = {
    if (logger.isTraceEnabled)
      logger.trace(msgWithLogIdent(msg), e)
  }
  final def swallowTrace(action: => Unit) {
    CoreUtils.swallow(logger.trace, action)
  }

  def isDebugEnabled: Boolean = logger.isDebugEnabled

  final def debug(msg: => String): Unit = {
    if (logger.isDebugEnabled)
      logger.debug(msgWithLogIdent(msg))
  }
  final def debug(e: => Throwable): Any = {
    if (logger.isDebugEnabled)
      logger.debug(logIdent, e)
  }
  final def debug(msg: => String, e: => Throwable): Unit = {
    if (logger.isDebugEnabled)
      logger.debug(msgWithLogIdent(msg), e)
  }
  final def swallowDebug(action: => Unit) {
    CoreUtils.swallow(logger.debug, action)
  }

  final def info(msg: => String): Unit = {
    if (logger.isInfoEnabled)
      logger.info(msgWithLogIdent(msg))
  }
  final def info(e: => Throwable): Any = {
    if (logger.isInfoEnabled)
      logger.info(logIdent, e)
  }
  final def info(msg: => String, e: => Throwable): Unit = {
    if (logger.isInfoEnabled)
      logger.info(msgWithLogIdent(msg), e)
  }
  final def swallowInfo(action: => Unit) {
    CoreUtils.swallow(logger.info, action)
  }

  final def warn(msg: => String): Unit = {
    logger.warn(msgWithLogIdent(msg))
  }
  final def warn(e: => Throwable): Any = {
    logger.warn(logIdent, e)
  }
  final def warn(msg: => String, e: => Throwable): Unit = {
    logger.warn(msgWithLogIdent(msg), e)
  }
  final def swallowWarn(action: => Unit) {
    CoreUtils.swallow(logger.warn, action)
  }
  final def swallow(action: => Unit): Unit = swallowWarn(action)

  final def error(msg: => String): Unit = {
    logger.error(msgWithLogIdent(msg))
  }
  final def error(e: => Throwable): Any = {
    logger.error(logIdent, e)
  }
  final def error(msg: => String, e: => Throwable): Unit = {
    logger.error(msgWithLogIdent(msg), e)
  }
  final def swallowError(action: => Unit) {
    CoreUtils.swallow(logger.error, action)
  }

  final def fatal(msg: => String): Unit = {
    logger.fatal(msgWithLogIdent(msg))
  }
  final def fatal(e: => Throwable): Any = {
    logger.fatal(logIdent, e)
  }
  final def fatal(msg: => String, e: => Throwable): Unit = {
    logger.fatal(msgWithLogIdent(msg), e)
  }
}
