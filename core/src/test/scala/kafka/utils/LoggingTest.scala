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

import java.lang.management.ManagementFactory

import javax.management.ObjectName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.slf4j.LoggerFactory


class LoggingTest extends Logging {

  @Test
  def testTypeOfGetLoggers(): Unit = {
    val log4jController = new Log4jController
    // the return object of getLoggers must be a collection instance from java standard library.
    // That enables mbean client to deserialize it without extra libraries.
    assertEquals(classOf[java.util.ArrayList[String]], log4jController.getLoggers.getClass)
  }

  @Test
  def testLog4jControllerIsRegistered(): Unit = {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    val log4jControllerName = ObjectName.getInstance("kafka:type=kafka.Log4jController")
    assertTrue(mbs.isRegistered(log4jControllerName), "kafka.utils.Log4jController is not registered")
    val instance = mbs.getObjectInstance(log4jControllerName)
    assertEquals("kafka.utils.Log4jController", instance.getClassName)
  }

  @Test
  def testLogNameOverride(): Unit = {
    class TestLogging(overriddenLogName: String) extends Logging {
      // Expose logger
      def log = logger
      override def loggerName = overriddenLogName
    }
    val overriddenLogName = "OverriddenLogName"
    val logging = new TestLogging(overriddenLogName)

    assertEquals(overriddenLogName, logging.log.underlying.getName)
  }

  @Test
  def testLogName(): Unit = {
    class TestLogging extends Logging {
      // Expose logger
      def log = logger
    }
    val logging = new TestLogging

    assertEquals(logging.getClass.getName, logging.log.underlying.getName)
  }

  @Test
  def testLoggerLevelIsResolved(): Unit = {
    val controller = new Log4jController()
    val previousLevel = controller.getLogLevel("kafka")
    try {
      controller.setLogLevel("kafka", "TRACE")
      // Do some logging so that the Logger is created within the hierarchy
      // (until loggers are used only loggers in the config file exist)
      LoggerFactory.getLogger("kafka.utils.Log4jControllerTest").trace("test")
      assertEquals("TRACE", controller.getLogLevel("kafka"))
      assertEquals("TRACE", controller.getLogLevel("kafka.utils.Log4jControllerTest"))
      assertTrue(controller.getLoggers.contains("kafka=TRACE"))
      assertTrue(controller.getLoggers.contains("kafka.utils.Log4jControllerTest=TRACE"))
    } finally {
      controller.setLogLevel("kafka", previousLevel)
    }
  }
}
