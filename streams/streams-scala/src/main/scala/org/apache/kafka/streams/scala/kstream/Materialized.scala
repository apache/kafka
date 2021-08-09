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

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Materialized => MaterializedJ}
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArraySessionStore, ByteArrayWindowStore}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, SessionBytesStoreSupplier, WindowBytesStoreSupplier}

object Materialized {

  /**
   * Materialize a [[StateStore]] with the provided key and value [[Serde]]s.
   * An internal name will be used for the store.
   *
   * @tparam K         key type
   * @tparam V         value type
   * @tparam S         store type
   * @param keySerde   the key [[Serde]] to use.
   * @param valueSerde the value [[Serde]] to use.
   * @return a new [[Materialized]] instance with the given key and value serdes
   */
  def `with`[K, V, S <: StateStore](implicit keySerde: Serde[K], valueSerde: Serde[V]): MaterializedJ[K, V, S] =
    MaterializedJ.`with`(keySerde, valueSerde)

  /**
   * Materialize a [[StateStore]] with the given name.
   *
   * @tparam K         key type of the store
   * @tparam V         value type of the store
   * @tparam S         type of the [[StateStore]]
   * @param storeName  the name of the underlying [[org.apache.kafka.streams.scala.kstream.KTable]] state store;
   *                   valid characters are ASCII alphanumerics, '.', '_' and '-'.
   * @param keySerde   the key serde to use.
   * @param valueSerde the value serde to use.
   * @return a new [[Materialized]] instance with the given storeName
   */
  def as[K, V, S <: StateStore](
    storeName: String
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): MaterializedJ[K, V, S] =
    MaterializedJ.as(storeName).withKeySerde(keySerde).withValueSerde(valueSerde)

  /**
   * Materialize a [[org.apache.kafka.streams.state.WindowStore]] using the provided [[WindowBytesStoreSupplier]].
   *
   * Important: Custom subclasses are allowed here, but they should respect the retention contract:
   * Window stores are required to retain windows at least as long as (window size + window grace period).
   * Stores constructed via [[org.apache.kafka.streams.state.Stores]] already satisfy this contract.
   *
   * @tparam K         key type of the store
   * @tparam V         value type of the store
   * @param supplier   the [[WindowBytesStoreSupplier]] used to materialize the store
   * @param keySerde   the key serde to use.
   * @param valueSerde the value serde to use.
   * @return a new [[Materialized]] instance with the given supplier
   */
  def as[K, V](
    supplier: WindowBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): MaterializedJ[K, V, ByteArrayWindowStore] =
    MaterializedJ.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)

  /**
   * Materialize a [[org.apache.kafka.streams.state.SessionStore]] using the provided [[SessionBytesStoreSupplier]].
   *
   * Important: Custom subclasses are allowed here, but they should respect the retention contract:
   * Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
   * Stores constructed via [[org.apache.kafka.streams.state.Stores]] already satisfy this contract.
   *
   * @tparam K         key type of the store
   * @tparam V         value type of the store
   * @param supplier   the [[SessionBytesStoreSupplier]] used to materialize the store
   * @param keySerde   the key serde to use.
   * @param valueSerde the value serde to use.
   * @return a new [[Materialized]] instance with the given supplier
   */
  def as[K, V](
    supplier: SessionBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): MaterializedJ[K, V, ByteArraySessionStore] =
    MaterializedJ.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)

  /**
   * Materialize a [[org.apache.kafka.streams.state.KeyValueStore]] using the provided [[KeyValueBytesStoreSupplier]].
   *
   * @tparam K         key type of the store
   * @tparam V         value type of the store
   * @param supplier   the [[KeyValueBytesStoreSupplier]] used to materialize the store
   * @param keySerde   the key serde to use.
   * @param valueSerde the value serde to use.
   * @return a new [[Materialized]] instance with the given supplier
   */
  def as[K, V](
    supplier: KeyValueBytesStoreSupplier
  )(implicit keySerde: Serde[K], valueSerde: Serde[V]): MaterializedJ[K, V, ByteArrayKeyValueStore] =
    MaterializedJ.as(supplier).withKeySerde(keySerde).withValueSerde(valueSerde)
}
