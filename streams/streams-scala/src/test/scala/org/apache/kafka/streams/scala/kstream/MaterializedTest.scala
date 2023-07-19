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

import org.apache.kafka.streams.kstream.internals.MaterializedInternal
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.Stores
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.time.Duration

class MaterializedTest {

  @Test
  def testCreateMaterializedWithSerdes(): Unit = {
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.`with`[String, Long, ByteArrayKeyValueStore]

    val internalMaterialized = new MaterializedInternal(materialized)
    assertEquals(Serdes.stringSerde.getClass, internalMaterialized.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalMaterialized.valueSerde.getClass)
  }

  @Test
  def testCreateMaterializedWithSerdesAndStoreName(): Unit = {
    val storeName = "store"
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.as[String, Long, ByteArrayKeyValueStore](storeName)

    val internalMaterialized = new MaterializedInternal(materialized)
    assertEquals(Serdes.stringSerde.getClass, internalMaterialized.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalMaterialized.valueSerde.getClass)
    assertEquals(storeName, internalMaterialized.storeName)
  }

  @Test
  def testCreateMaterializedWithSerdesAndWindowStoreSupplier(): Unit = {
    val storeSupplier = Stores.persistentWindowStore("store", Duration.ofMillis(1), Duration.ofMillis(1), true)
    val materialized: Materialized[String, Long, ByteArrayWindowStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    assertEquals(Serdes.stringSerde.getClass, internalMaterialized.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalMaterialized.valueSerde.getClass)
    assertEquals(storeSupplier, internalMaterialized.storeSupplier)
  }

  @Test
  def testCreateMaterializedWithSerdesAndKeyValueStoreSupplier(): Unit = {
    val storeSupplier = Stores.persistentKeyValueStore("store")
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    assertEquals(Serdes.stringSerde.getClass, internalMaterialized.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalMaterialized.valueSerde.getClass)
    assertEquals(storeSupplier, internalMaterialized.storeSupplier)
  }

  @Test
  def testCreateMaterializedWithSerdesAndSessionStoreSupplier(): Unit = {
    val storeSupplier = Stores.persistentSessionStore("store", Duration.ofMillis(1))
    val materialized: Materialized[String, Long, ByteArraySessionStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    assertEquals(Serdes.stringSerde.getClass, internalMaterialized.keySerde.getClass)
    assertEquals(Serdes.longSerde.getClass, internalMaterialized.valueSerde.getClass)
    assertEquals(storeSupplier, internalMaterialized.storeSupplier)
  }
}
