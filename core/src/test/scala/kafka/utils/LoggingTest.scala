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

import org.junit.Test
import org.junit.Assert.{assertEquals, assertTrue}


class LoggingTest extends Logging {

  @Test
  def testLog4jControllerIsRegistered(): Unit = {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    val log4jControllerName = ObjectName.getInstance("kafka:type=kafka.Log4jController")
    assertTrue("kafka.utils.Log4jController is not registered", mbs.isRegistered(log4jControllerName))
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
}
