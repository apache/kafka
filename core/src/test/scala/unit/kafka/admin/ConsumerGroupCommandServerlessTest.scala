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

package kafka.admin

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import org.junit.Assert.fail
import org.junit.Test

class ConsumerGroupCommandServerlessTest {

  @Test
  def testDateTimeFormats(): Unit = {
    //check valid formats
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    //check some invalid formats
    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }

    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }
  }

  private def invokeGetDateTimeMethod(format: SimpleDateFormat): Unit = {
    val checkpoint = new Date()
    val timestampString = format.format(checkpoint)
    ConsumerGroupCommand.convertTimestamp(timestampString)
  }
}
