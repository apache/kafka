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

package kafka.tools

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat

import org.junit.Assert.assertEquals
import org.junit.Test

class ConsumerPerformanceTest {

  private val outContent = new ByteArrayOutputStream()

  @Test
  def testHeaderMatchBody(): Unit = {
    Console.withOut(outContent) {
      ConsumerPerformance.printHeader(true)
      ConsumerPerformance.printProgressMessage(1, 1024 * 1024, 0, 1, 0, 0, 1,
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      )
    }

    val contents = outContent.toString.split("\n")
    assertEquals(2, contents.length)
    val header = contents(0)
    val body = contents(1)

    assertEquals(header.split(",").length, body.split(",").length)
  }
}
