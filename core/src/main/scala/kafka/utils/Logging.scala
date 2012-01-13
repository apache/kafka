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

import org.apache.log4j.Logger

trait Logging {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def trace(msg: => String): Unit = {
    if (logger.isTraceEnabled())
      logger.trace(msg)	
  }
  def trace(e: => Throwable): Any = {
    if (logger.isTraceEnabled())
      logger.trace("",e)	
  }
  def trace(msg: => String, e: => Throwable) = {
    if (logger.isTraceEnabled())
      logger.trace(msg,e)
  }
  def swallowTrace(action: => Unit) {
    Utils.swallow(logger.trace, action)
  }

  def debug(msg: => String): Unit = {
    if (logger.isDebugEnabled())
      logger.debug(msg)
  }
  def debug(e: => Throwable): Any = {
    if (logger.isDebugEnabled())
      logger.debug("",e)	
  }
  def debug(msg: => String, e: => Throwable) = {
    if (logger.isDebugEnabled())
      logger.debug(msg,e)
  }
  def swallowDebug(action: => Unit) {
    Utils.swallow(logger.debug, action)
  }

  def info(msg: => String): Unit = {
    if (logger.isInfoEnabled())
      logger.info(msg)
  }
  def info(e: => Throwable): Any = {
    if (logger.isInfoEnabled())
      logger.info("",e)
  }
  def info(msg: => String,e: => Throwable) = {
    if (logger.isInfoEnabled())
      logger.info(msg,e)
  }
  def swallowInfo(action: => Unit) {
    Utils.swallow(logger.info, action)
  }

  def warn(msg: => String): Unit = {
    logger.warn(msg)
  }
  def warn(e: => Throwable): Any = {
    logger.warn("",e)
  }
  def warn(msg: => String, e: => Throwable) = {
    logger.warn(msg,e)
  }
  def swallowWarn(action: => Unit) {
    Utils.swallow(logger.warn, action)
  }
  def swallow(action: => Unit) = swallowWarn(action)

  def error(msg: => String):Unit = {
    logger.error(msg)
  }		
  def error(e: => Throwable): Any = {
    logger.error("",e)
  }
  def error(msg: => String, e: => Throwable) = {
    logger.error(msg,e)
  }
  def swallowError(action: => Unit) {
    Utils.swallow(logger.error, action)
  }

  def fatal(msg: => String): Unit = {
    logger.fatal(msg)
  }
  def fatal(e: => Throwable): Any = {
    logger.fatal("",e)
  }	
  def fatal(msg: => String, e: => Throwable) = {
    logger.fatal(msg,e)
  }
 
}