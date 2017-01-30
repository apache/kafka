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

import org.apache.log4j._
import org.apache.log4j.spi._
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable.ListBuffer

class LoggingTest {

  private[this] class MockAppender extends AppenderSkeleton {
    val eventLocations = new ListBuffer[LocationInfo]()

    override def append(event: LoggingEvent): Unit = {
      eventLocations += event.getLocationInformation
    }

    override def requiresLayout(): Boolean = false

    override def close(): Unit = {}
  }

  private[this] class TestLogger extends Logging {
    logger.isTraceEnabled // Instantiate lazy-val logger

    def logWarnByTraitMethod(): Throwable = {
      warn("warn through trait method"); new Throwable // intentionally placing on the same line to get exactly same line number
    }

    def logWarnBySwallowingLogMethod(): Throwable = {
      swallowWarn(throw new RuntimeException("exception to be swallowed")); new Throwable // intentionally placing on the same line to get exactly same line number
    }

    def logWarnThroughLogger(): Throwable = {
      logger.warn("warn through logger"); new Throwable // intentionally placing on the same line to get exactly same line number
    }
  }

  private[this] class TestLoggerSubclass extends TestLogger {
    def logWarnThroughSubclass(): Throwable = {
      warn("warn by subclass"); new Throwable // intentionally placing on the same line to get exactly same line number
    }
  }


  private[this] val appender = new MockAppender
  private[this] val testLogger = new TestLogger

  @Before
  def setUp(): Unit = {
    val logger = Logger.getLogger(classOf[TestLogger])
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)
  }

  private[this] def callerInfo(throwable: Throwable): LocationInfo = {
    val firstElem = throwable.getStackTrace.view.head
    new LocationInfo(firstElem.getFileName, firstElem.getClassName, firstElem.getMethodName,
      String.valueOf(firstElem.getLineNumber))
  }

  private[this] def verifyLocationInfo(actual: LocationInfo, got: LocationInfo): Unit = {
    assertNotEquals(actual.getFileName, LocationInfo.NA)
    assertEquals(actual.getFileName, got.getFileName)
    assertNotEquals(actual.getClassName, LocationInfo.NA)
    assertEquals(actual.getClassName, got.getClassName)
    assertNotEquals(actual.getMethodName, LocationInfo.NA)
    assertEquals(actual.getMethodName, got.getMethodName)
    assertNotEquals(actual.getLineNumber, LocationInfo.NA)
    assertEquals(actual.getLineNumber, got.getLineNumber)
  }

  @Test
  def testLocationInfoWithTraitMethod(): Unit = {
    val caller = callerInfo(testLogger.logWarnByTraitMethod())

    assertEquals(1, appender.eventLocations.size)
    verifyLocationInfo(caller, appender.eventLocations.head)
  }

  @Test
  def testLocationInfoWithSwallowingLogMethod(): Unit = {
    val caller = callerInfo(testLogger.logWarnBySwallowingLogMethod())

    assertEquals(1, appender.eventLocations.size)
    verifyLocationInfo(caller, appender.eventLocations.head)
  }

  @Test
  def testLocationInfoWithDirectLoggerAccess(): Unit = {
    val caller = callerInfo(testLogger.logWarnThroughLogger())

    assertEquals(1, appender.eventLocations.size)
    verifyLocationInfo(caller, appender.eventLocations.head)
  }

  @Test
  def testLocationInfoWithTraitMethodBySubclass(): Unit = {
    val subclassLogger = new TestLoggerSubclass
    val logger = Logger.getLogger(classOf[TestLoggerSubclass])
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)

    val caller = callerInfo(subclassLogger.logWarnThroughSubclass())

    assertEquals(1, appender.eventLocations.size)
    verifyLocationInfo(caller, appender.eventLocations.head)
  }

  @Test
  def testLocationInfoWithTraitMethodByExternal(): Unit = {
    testLogger.warn("warn from external"); val caller = callerInfo(new Throwable) // intentionally placing on the same line to get exactly same line number

    assertEquals(1, appender.eventLocations.size)
    verifyLocationInfo(caller, appender.eventLocations.head)
  }
}
