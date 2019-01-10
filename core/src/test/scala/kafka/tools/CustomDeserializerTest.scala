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

package kafka.tools

import java.io.PrintStream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.hamcrest.CoreMatchers
import org.junit.Test
import org.junit.Assert.assertThat
import org.scalatest.mockito.MockitoSugar

class CustomDeserializer extends Deserializer[String] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def deserialize(topic: String, data: Array[Byte]): String = {
    assertThat("topic must not be null", topic, CoreMatchers.notNullValue())
    new String(data)
  }

  override def close(): Unit = {
  }
}

class CustomDeserializerTest extends MockitoSugar {

  @Test
  def checkDeserializerTopicIsNotNull(): Unit = {
    val formatter = new DefaultMessageFormatter()
    formatter.keyDeserializer = Some(new CustomDeserializer)

    formatter.writeTo(new ConsumerRecord("topic_test", 1, 1l, "key".getBytes, "value".getBytes), mock[PrintStream])

    formatter.close()
  }
}
