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

import org.apache.kafka.streams.kstream.internals.StreamJoinedInternal
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.Stores
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.time.Duration

class StreamJoinedTest {

  @Test
  def testCreateStreamJoinedWithSerdes(): Unit = {
    val streamJoined: StreamJoined[String, String, Long] = StreamJoined.`with`[String, String, Long]

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.keySerde().getClass)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.valueSerde().getClass)
    assertEquals(Serdes.longSerde.getClass, streamJoinedInternal.otherValueSerde().getClass)
  }

  @Test
  def testCreateStreamJoinedWithSerdesAndStoreSuppliers(): Unit = {
    val storeSupplier = Stores.inMemoryWindowStore("myStore", Duration.ofMillis(500), Duration.ofMillis(250), false)

    val otherStoreSupplier =
      Stores.inMemoryWindowStore("otherStore", Duration.ofMillis(500), Duration.ofMillis(250), false)

    val streamJoined: StreamJoined[String, String, Long] =
      StreamJoined.`with`[String, String, Long](storeSupplier, otherStoreSupplier)

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.keySerde().getClass)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.valueSerde().getClass)
    assertEquals(Serdes.longSerde.getClass, streamJoinedInternal.otherValueSerde().getClass)
    assertEquals(otherStoreSupplier, streamJoinedInternal.otherStoreSupplier())
    assertEquals(storeSupplier, streamJoinedInternal.thisStoreSupplier())
  }

  @Test
  def testCreateStreamJoinedWithSerdesAndStateStoreName(): Unit = {
    val streamJoined: StreamJoined[String, String, Long] = StreamJoined.as[String, String, Long]("myStoreName")

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.keySerde().getClass)
    assertEquals(Serdes.stringSerde.getClass, streamJoinedInternal.valueSerde().getClass)
    assertEquals(Serdes.longSerde.getClass, streamJoinedInternal.otherValueSerde().getClass)
    assertEquals("myStoreName", streamJoinedInternal.storeName())
  }

}
