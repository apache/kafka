/*
 * Copyright (C) 2018 Joan Goyeau.
 *
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

import java.time.Duration

import org.apache.kafka.streams.kstream.internals.MaterializedInternal
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.Stores
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MaterializedTest extends FlatSpec with Matchers {

  "Create a Materialized" should "create a Materialized with Serdes" in {
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.`with`[String, Long, ByteArrayKeyValueStore]

    val internalMaterialized = new MaterializedInternal(materialized)
    internalMaterialized.keySerde.getClass shouldBe Serdes.String.getClass
    internalMaterialized.valueSerde.getClass shouldBe Serdes.Long.getClass
  }

  "Create a Materialize with a store name" should "create a Materialized with Serdes and a store name" in {
    val storeName = "store"
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.as[String, Long, ByteArrayKeyValueStore](storeName)

    val internalMaterialized = new MaterializedInternal(materialized)
    internalMaterialized.keySerde.getClass shouldBe Serdes.String.getClass
    internalMaterialized.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalMaterialized.storeName shouldBe storeName
  }

  "Create a Materialize with a window store supplier" should "create a Materialized with Serdes and a store supplier" in {
    val storeSupplier = Stores.persistentWindowStore("store", Duration.ofMillis(1), Duration.ofMillis(1), true)
    val materialized: Materialized[String, Long, ByteArrayWindowStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    internalMaterialized.keySerde.getClass shouldBe Serdes.String.getClass
    internalMaterialized.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalMaterialized.storeSupplier shouldBe storeSupplier
  }

  "Create a Materialize with a key value store supplier" should "create a Materialized with Serdes and a store supplier" in {
    val storeSupplier = Stores.persistentKeyValueStore("store")
    val materialized: Materialized[String, Long, ByteArrayKeyValueStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    internalMaterialized.keySerde.getClass shouldBe Serdes.String.getClass
    internalMaterialized.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalMaterialized.storeSupplier shouldBe storeSupplier
  }

  "Create a Materialize with a session store supplier" should "create a Materialized with Serdes and a store supplier" in {
    val storeSupplier = Stores.persistentSessionStore("store", Duration.ofMillis(1))
    val materialized: Materialized[String, Long, ByteArraySessionStore] =
      Materialized.as[String, Long](storeSupplier)

    val internalMaterialized = new MaterializedInternal(materialized)
    internalMaterialized.keySerde.getClass shouldBe Serdes.String.getClass
    internalMaterialized.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalMaterialized.storeSupplier shouldBe storeSupplier
  }
}
