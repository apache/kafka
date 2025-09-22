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

import java.io.File
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import java.lang.management.ManagementFactory
import com.typesafe.scalalogging.Logger

import javax.management.ObjectName
import org.apache.kafka.common.utils.Utils
import org.slf4j.event.Level

/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object CoreUtils {
  private val logger = Logger(getClass)

  /**
    * Do the given action and log any exceptions thrown without rethrowing them.
    *
    * @param action The action to execute.
    * @param logging The logging instance to use for logging the thrown exception.
    * @param logLevel The log level to use for logging.
    */
  @noinline // inlining this method is not typically useful and it triggers spurious spotbugs warnings
  def swallow(action: => Unit, logging: Logging, logLevel: Level = Level.WARN): Unit = {
    try {
      action
    } catch {
      case e: Throwable => logLevel match {
        case Level.ERROR => logging.error(e.getMessage, e)
        case Level.WARN => logging.warn(e.getMessage, e)
        case Level.INFO => logging.info(e.getMessage, e)
        case Level.DEBUG => logging.debug(e.getMessage, e)
        case Level.TRACE => logging.trace(e.getMessage, e)
      }
    }
  }

  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param files list of files to be deleted
   */
  def delete(files: java.util.List[String]): Unit = files.forEach(f => Utils.delete(new File(f)))

  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer
      mbs synchronized {
        val objName = new ObjectName(name)
        if (mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register Mbean $name", e)
        false
    }
  }

  /**
   * Execute the given function inside the lock
   */
  def inLock[T](lock: Lock)(fun: => T): T = {
    lock.lock()
    try {
      fun
    } finally {
      lock.unlock()
    }
  }

  def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.readLock)(fun)

  def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)

}
