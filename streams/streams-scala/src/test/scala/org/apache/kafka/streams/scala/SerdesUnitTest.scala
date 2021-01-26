/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala

import kafka.utils.LogCaptureAppender
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde
import org.junit.Assert.assertTrue
import org.junit.Test

class SerdesUnitTest {

  @Test
  def shouldLogMessageWhenTimeWindowedSerdeIsUsed(): Unit = {

    Serdes.timeWindowedSerde(new TimeWindowedSerde[String]())
    val appender = LogCaptureAppender.createAndRegister()
    val message = appender.getMessages.find(e => e.getLevel == Level.WARN && e.getThrowableInformation != null)
    assertTrue("There should be a warning about TimeWindowedDeserializer", message.isDefined)
  }

}
