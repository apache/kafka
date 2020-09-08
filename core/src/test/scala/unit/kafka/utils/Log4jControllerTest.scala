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

import com.typesafe.scalalogging.Logger
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory


class Log4jControllerTest {

  @Test
  def testLoggerLevelIsResolved(): Unit = {
    val controller = new Log4jController()
    val previousLevel = controller.getLogLevel("kafka")
    try {
      controller.setLogLevel("kafka", "TRACE")
      Logger(LoggerFactory.getLogger("kafka.utils.Log4jControllerTest")).trace("test")
      Assert.assertEquals("TRACE", controller.getLogLevel("kafka"))
      Assert.assertEquals("TRACE", controller.getLogLevel("kafka.utils.Log4jControllerTest"))
      Assert.assertTrue(controller.getLoggers.contains("kafka=TRACE"))
      Assert.assertTrue(controller.getLoggers.contains("kafka.utils.Log4jControllerTest=TRACE"))
    } finally {
      controller.setLogLevel("kafka", previousLevel)
    }

  }
}
