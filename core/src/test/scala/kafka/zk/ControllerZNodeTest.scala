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

package kafka.zk

import java.nio.charset.StandardCharsets

import org.junit.Assert.assertEquals
import org.junit.Test

class ControllerZNodeTest {

  @Test
  def testEncodeDecode(): Unit = {
    val brokerId = 1
    val timestamp = 1500000000000L
    assertEquals(brokerId, ControllerZNode.decode(ControllerZNode.encode(brokerId, timestamp)).get)
  }

  @Test
  def testDecodeFailed(): Unit = {
    assertEquals("malformed json", None,
      ControllerZNode.decode("""{"brokerid":1, "timestamp":"1",}""".getBytes(StandardCharsets.UTF_8)))
    assertEquals("no version", None,
      ControllerZNode.decode("""{"brokerid":1, "timestamp":"1"}""".getBytes(StandardCharsets.UTF_8)))
  }
}
