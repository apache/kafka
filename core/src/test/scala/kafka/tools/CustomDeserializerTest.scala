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

import java.io.PrintStream

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

class CustomDeserializer extends Deserializer[String] {

  override def deserialize(topic: String, data: Array[Byte]): String = {
    assertNotNull(topic, "topic must not be null")
    new String(data)
  }

  override def deserialize(topic: String, headers: Headers, data: Array[Byte]): String = {
    println("WITH HEADERS")
    new String(data)
  }
}

class CustomDeserializerTest {

  @Test
  def checkFormatterCallDeserializerWithHeaders(): Unit = {
    val formatter = new DefaultMessageFormatter()
    formatter.valueDeserializer = Some(new CustomDeserializer)
    val output = TestUtils.grabConsoleOutput(formatter.writeTo(
      new ConsumerRecord("topic_test", 1, 1L, "key".getBytes, "value".getBytes), mock(classOf[PrintStream])))
    assertTrue(output.contains("WITH HEADERS"), "DefaultMessageFormatter should call `deserialize` method with headers.")
    formatter.close()
  }

  @Test
  def checkDeserializerTopicIsNotNull(): Unit = {
    val formatter = new DefaultMessageFormatter()
    formatter.keyDeserializer = Some(new CustomDeserializer)

    formatter.writeTo(new ConsumerRecord("topic_test", 1, 1L, "key".getBytes, "value".getBytes),
      mock(classOf[PrintStream]))

    formatter.close()
  }
}
