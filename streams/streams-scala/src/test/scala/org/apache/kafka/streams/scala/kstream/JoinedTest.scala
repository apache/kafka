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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JoinedTest {

  @Test
  def testCreateJoined(): Unit = {
    val joined: Joined[String, Long, Int] = Joined.`with`[String, Long, Int]

    assertEquals(joined.keySerde.getClass, Serdes.stringSerde.getClass)
    assertEquals(joined.valueSerde.getClass, Serdes.longSerde.getClass)
    assertEquals(joined.otherValueSerde.getClass, Serdes.intSerde.getClass)
  }

  @Test
  def testCreateJoinedWithSerdesAndRepartitionTopicName(): Unit = {
    val repartitionTopicName = "repartition-topic"
    val joined: Joined[String, Long, Int] = Joined.`with`(repartitionTopicName)

    assertEquals(joined.keySerde.getClass, Serdes.stringSerde.getClass)
    assertEquals(joined.valueSerde.getClass, Serdes.longSerde.getClass)
    assertEquals(joined.otherValueSerde.getClass, Serdes.intSerde.getClass)
  }
}
