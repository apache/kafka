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
package org.apache.kafka.streams.scala
package kstream

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{
  GlobalKTable,
  JoinWindows,
  Printed,
  TransformerSupplier,
  ValueTransformerSupplier,
  ValueTransformerWithKeySupplier,
  KStream => KStreamJ
}
import org.apache.kafka.streams.processor.TopicNameExtractor
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.scala.FunctionsCompatConversions.{
  FlatValueMapperFromFunction,
  FlatValueMapperWithKeyFromFunction,
  ForeachActionFromFunction,
  KeyValueMapperFromFunction,
  MapperFromFunction,
  PredicateFromFunction,
  TransformerSupplierAsJava,
  ValueMapperFromFunction,
  ValueMapperWithKeyFromFunction,
  ValueTransformerSupplierAsJava,
  ValueTransformerSupplierWithKeyAsJava
}

import scala.jdk.CollectionConverters._

/**
 * Wraps the Java class [[org.apache.kafka.streams.kstream.KStream KStream]] and delegates method calls to the
 * underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for KStream
 * @see `org.apache.kafka.streams.kstream.KStream`
 */
//noinspection ScalaDeprecation
class KStream[K, V](val inner: KStreamJ[K, V]) {

  /**
   * Create a new [[KStream]] that consists all records of this stream which satisfies the given predicate.
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KStream]] that contains only those records that satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filter`
   */
  def filter(predicate: (K, V) => Boolean): KStream[K, V] =
    new KStream(inner.filter(predicate.asPredicate))

  /**
   * Create a new [[KStream]] that consists all records of this stream which satisfies the given predicate.
   *
   * @param predicate a filter that is applied to each record
   * @param named     a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains only those records that satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filter`
   */
  def filter(predicate: (K, V) => Boolean, named: Named): KStream[K, V] =
    new KStream(inner.filter(predicate.asPredicate, named))

  /**
   * Create a new [[KStream]] that consists all records of this stream which do <em>not</em> satisfy the given
   * predicate.
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KStream]] that contains only those records that do <em>not</em> satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filterNot`
   */
  def filterNot(predicate: (K, V) => Boolean): KStream[K, V] =
    new KStream(inner.filterNot(predicate.asPredicate))

  /**
   * Create a new [[KStream]] that consists all records of this stream which do <em>not</em> satisfy the given
   * predicate.
   *
   * @param predicate a filter that is applied to each record
   * @param named     a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains only those records that do <em>not</em> satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filterNot`
   */
  def filterNot(predicate: (K, V) => Boolean, named: Named): KStream[K, V] =
    new KStream(inner.filterNot(predicate.asPredicate, named))

  /**
   * Set a new key (with possibly new type) for each input record.
   * <p>
   * The function `mapper` passed is applied to every record and results in the generation of a new
   * key `KR`. The function outputs a new [[KStream]] where each record has this new key.
   *
   * @param mapper a function `(K, V) => KR` that computes a new key for each record
   * @return a [[KStream]] that contains records with new key (possibly of different type) and unmodified value
   * @see `org.apache.kafka.streams.kstream.KStream#selectKey`
   */
  def selectKey[KR](mapper: (K, V) => KR): KStream[KR, V] =
    new KStream(inner.selectKey[KR](mapper.asKeyValueMapper))

  /**
   * Set a new key (with possibly new type) for each input record.
   * <p>
   * The function `mapper` passed is applied to every record and results in the generation of a new
   * key `KR`. The function outputs a new [[KStream]] where each record has this new key.
   *
   * @param mapper a function `(K, V) => KR` that computes a new key for each record
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains records with new key (possibly of different type) and unmodified value
   * @see `org.apache.kafka.streams.kstream.KStream#selectKey`
   */
  def selectKey[KR](mapper: (K, V) => KR, named: Named): KStream[KR, V] =
    new KStream(inner.selectKey[KR](mapper.asKeyValueMapper, named))

  /**
   * Transform each record of the input stream into a new record in the output stream (both key and value type can be
   * altered arbitrarily).
   * <p>
   * The provided `mapper`, a function `(K, V) => (KR, VR)` is applied to each input record and computes a new output record.
   *
   * @param mapper a function `(K, V) => (KR, VR)` that computes a new output record
   * @return a [[KStream]] that contains records with new key and value (possibly both of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#map`
   */
  def map[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] =
    new KStream(inner.map[KR, VR](mapper.asKeyValueMapper))

  /**
   * Transform each record of the input stream into a new record in the output stream (both key and value type can be
   * altered arbitrarily).
   * <p>
   * The provided `mapper`, a function `(K, V) => (KR, VR)` is applied to each input record and computes a new output record.
   *
   * @param mapper a function `(K, V) => (KR, VR)` that computes a new output record
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains records with new key and value (possibly both of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#map`
   */
  def map[KR, VR](mapper: (K, V) => (KR, VR), named: Named): KStream[KR, VR] =
    new KStream(inner.map[KR, VR](mapper.asKeyValueMapper, named))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `V => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper , a function `V => VR` that computes a new output value
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: V => VR): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapper))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `V => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper , a function `V => VR` that computes a new output value
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: V => VR, named: Named): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapper, named))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `(K, V) => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper , a function `(K, V) => VR` that computes a new output value
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: (K, V) => VR): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapperWithKey))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `(K, V) => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper , a function `(K, V) => VR` that computes a new output value
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: (K, V) => VR, named: Named): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapperWithKey, named))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * <p>
   * The provided `mapper`, function `(K, V) => Iterable[(KR, VR)]` is applied to each input record and computes zero or more output records.
   *
   * @param mapper function `(K, V) => Iterable[(KR, VR)]` that computes the new output records
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#flatMap`
   */
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStream[KR, VR] = {
    val kvMapper = mapper.tupled.andThen(_.map(ImplicitConversions.tuple2ToKeyValue).asJava)
    new KStream(inner.flatMap[KR, VR](((k: K, v: V) => kvMapper(k, v)).asKeyValueMapper))
  }

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * <p>
   * The provided `mapper`, function `(K, V) => Iterable[(KR, VR)]` is applied to each input record and computes zero or more output records.
   *
   * @param mapper function `(K, V) => Iterable[(KR, VR)]` that computes the new output records
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#flatMap`
   */
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)], named: Named): KStream[KR, VR] = {
    val kvMapper = mapper.tupled.andThen(_.map(ImplicitConversions.tuple2ToKeyValue).asJava)
    new KStream(inner.flatMap[KR, VR](((k: K, v: V) => kvMapper(k, v)).asKeyValueMapper, named))
  }

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `V => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `V => Iterable[VR]` that computes the new output values
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: V => Iterable[VR]): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapper))

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `V => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `V => Iterable[VR]` that computes the new output values
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: V => Iterable[VR], named: Named): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapper, named))

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `(K, V) => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `(K, V) => Iterable[VR]` that computes the new output values
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: (K, V) => Iterable[VR]): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapperWithKey))

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `(K, V) => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `(K, V) => Iterable[VR]` that computes the new output values
   * @param named  a [[Named]] config used to name the processor in the topology
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: (K, V) => Iterable[VR], named: Named): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapperWithKey, named))

  /**
   * Print the records of this KStream using the options provided by `Printed`
   *
   * @param printed options for printing
   * @see `org.apache.kafka.streams.kstream.KStream#print`
   */
  def print(printed: Printed[K, V]): Unit = inner.print(printed)

  /**
   * Perform an action on each record of `KStream`
   *
   * @param action an action to perform on each record
   * @see `org.apache.kafka.streams.kstream.KStream#foreach`
   */
  def foreach(action: (K, V) => Unit): Unit =
    inner.foreach(action.asForeachAction)

  /**
   * Perform an action on each record of `KStream`
   *
   * @param action an action to perform on each record
   * @param named  a [[Named]] config used to name the processor in the topology
   * @see `org.apache.kafka.streams.kstream.KStream#foreach`
   */
  def foreach(action: (K, V) => Unit, named: Named): Unit =
    inner.foreach(action.asForeachAction, named)

  /**
   * Creates an array of `KStream` from this stream by branching the records in the original stream based on
   * the supplied predicates.
   *
   * @param predicates the ordered list of functions that return a Boolean
   * @return multiple distinct substreams of this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#branch`
   * @deprecated since 2.8. Use `split` instead.
   */
  //noinspection ScalaUnnecessaryParentheses
  @deprecated("use `split()` instead", "2.8")
  def branch(predicates: ((K, V) => Boolean)*): Array[KStream[K, V]] =
    inner.branch(predicates.map(_.asPredicate): _*).map(kstream => new KStream(kstream))

  /**
   * Split this stream. [[BranchedKStream]] can be used for routing the records to different branches depending
   * on evaluation against the supplied predicates.
   * Stream branching is a stateless record-by-record operation.
   *
   * @return [[BranchedKStream]] that provides methods for routing the records to different branches.
   * @see `org.apache.kafka.streams.kstream.KStream#split`
   */
  def split(): BranchedKStream[K, V] =
    new BranchedKStream(inner.split())

  /**
   * Split this stream. [[BranchedKStream]] can be used for routing the records to different branches depending
   * on evaluation against the supplied predicates.
   * Stream branching is a stateless record-by-record operation.
   *
   * @param named a [[Named]] config used to name the processor in the topology and also to set the name prefix
   *              for the resulting branches (see [[BranchedKStream]])
   * @return [[BranchedKStream]] that provides methods for routing the records to different branches.
   * @see `org.apache.kafka.streams.kstream.KStream#split`
   */
  def split(named: Named): BranchedKStream[K, V] =
    new BranchedKStream(inner.split(named))

  /**
   * Materialize this stream to a topic and creates a new [[KStream]] from the topic using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or they can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KStream[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.through(topic)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param topic    the topic name
   * @param produced the instance of Produced that gives the serdes and `StreamPartitioner`
   * @return a [[KStream]] that contains the exact same (and potentially repartitioned) records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#through`
   * @deprecated use `repartition()` instead
   */
  @deprecated("use `repartition()` instead", "2.6.0")
  def through(topic: String)(implicit produced: Produced[K, V]): KStream[K, V] =
    new KStream(inner.through(topic, produced))

  /**
   * Materialize this stream to a topic and creates a new [[KStream]] from the topic using the `Repartitioned` instance
   * for configuration of the `Serde key serde`, `Serde value serde`, `StreamPartitioner`, number of partitions, and
   * topic name part.
   * <p>
   * The created topic is considered as an internal topic and is meant to be used only by the current Kafka Streams instance.
   * Similar to auto-repartitioning, the topic will be created with infinite retention time and data will be automatically purged by Kafka Streams.
   * The topic will be named as "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
   * `StreamsConfig` via parameter `APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG`,
   * "&lt;name&gt;" is either provided via `Repartitioned#as(String)` or an internally
   * generated name, and "-repartition" is a fixed suffix.
   * <p>
   * The user can either supply the `Repartitioned` instance as an implicit in scope or they can also provide implicit
   * key and value serdes that will be converted to a `Repartitioned` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KStream[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.repartition
   *
   * // Similarly you can create an implicit Repartitioned and it will be passed implicitly
   * // to the repartition call
   * }}}
   *
   * @param repartitioned the `Repartitioned` instance used to specify `Serdes`, `StreamPartitioner` which determines
   *                      how records are distributed among partitions of the topic,
   *                      part of the topic name, and number of partitions for a repartition topic.
   * @return a [[KStream]] that contains the exact same repartitioned records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#repartition`
   */
  def repartition(implicit repartitioned: Repartitioned[K, V]): KStream[K, V] =
    new KStream(inner.repartition(repartitioned))

  /**
   * Materialize this stream to a topic using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or they can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KTable[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.to(topic)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param topic    the topic name
   * @param produced the instance of Produced that gives the serdes and `StreamPartitioner`
   * @see `org.apache.kafka.streams.kstream.KStream#to`
   */
  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic, produced)

  /**
   * Dynamically materialize this stream to topics using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`.
   * The topic names for each record to send to is dynamically determined based on the given mapper.
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or they can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KTable[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.to(topicChooser)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param extractor the extractor to determine the name of the Kafka topic to write to for reach record
   * @param produced  the instance of Produced that gives the serdes and `StreamPartitioner`
   * @see `org.apache.kafka.streams.kstream.KStream#to`
   */
  def to(extractor: TopicNameExtractor[K, V])(implicit produced: Produced[K, V]): Unit =
    inner.to(extractor, produced)

  /**
   * Convert this stream to a [[KTable]].
   *
   * @return a [[KTable]] that contains the same records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#toTable`
   */
  def toTable: KTable[K, V] =
    new KTable(inner.toTable)

  /**
   * Convert this stream to a [[KTable]].
   *
   * @param named a [[Named]] config used to name the processor in the topology
   * @return a [[KTable]] that contains the same records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#toTable`
   */
  def toTable(named: Named): KTable[K, V] =
    new KTable(inner.toTable(named))

  /**
   * Convert this stream to a [[KTable]].
   *
   * @param materialized a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                     should be materialized.
   * @return a [[KTable]] that contains the same records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#toTable`
   */
  def toTable(materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    new KTable(inner.toTable(materialized))

  /**
   * Convert this stream to a [[KTable]].
   *
   * @param named        a [[Named]] config used to name the processor in the topology
   * @param materialized a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                     should be materialized.
   * @return a [[KTable]] that contains the same records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#toTable`
   */
  def toTable(named: Named, materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    new KTable(inner.toTable(named, materialized))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def transform[K1, V1](
    transformerSupplier: TransformerSupplier[K, V, KeyValue[K1, V1]],
    stateStoreNames: String*
  ): KStream[K1, V1] =
    new KStream(inner.transform(transformerSupplier, stateStoreNames: _*))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param named               a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def transform[K1, V1](
    transformerSupplier: TransformerSupplier[K, V, KeyValue[K1, V1]],
    named: Named,
    stateStoreNames: String*
  ): KStream[K1, V1] =
    new KStream(inner.transform(transformerSupplier, named, stateStoreNames: _*))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def flatTransform[K1, V1](
    transformerSupplier: TransformerSupplier[K, V, Iterable[KeyValue[K1, V1]]],
    stateStoreNames: String*
  ): KStream[K1, V1] =
    new KStream(inner.flatTransform(transformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param named               a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def flatTransform[K1, V1](
    transformerSupplier: TransformerSupplier[K, V, Iterable[KeyValue[K1, V1]]],
    named: Named,
    stateStoreNames: String*
  ): KStream[K1, V1] =
    new KStream(inner.flatTransform(transformerSupplier.asJava, named, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](
    valueTransformerSupplier: ValueTransformerSupplier[V, Iterable[VR]],
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param named                    a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](
    valueTransformerSupplier: ValueTransformerSupplier[V, Iterable[VR]],
    named: Named,
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, named, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](
    valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, Iterable[VR]],
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param named                    a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](
    valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, Iterable[VR]],
    named: Named,
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, named, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](
    valueTransformerSupplier: ValueTransformerSupplier[V, VR],
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param named                    a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](
    valueTransformerSupplier: ValueTransformerSupplier[V, VR],
    named: Named,
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, named, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](
    valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, VR],
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param named                    a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](
    valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, VR],
    named: Named,
    stateStoreNames: String*
  ): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, named, stateStoreNames: _*))

  /**
   * Process all records in this stream, one record at a time, by applying a `Processor` (provided by the given
   * `processorSupplier`).
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Processor`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param processorSupplier a function that generates a [[org.apache.kafka.streams.processor.Processor]]
   * @param stateStoreNames   the names of the state store used by the processor
   * @see `org.apache.kafka.streams.kstream.KStream#process`
   */
  @deprecated(since = "3.0", message = "Use process(ProcessorSupplier, String*) instead.")
  def process(
    processorSupplier: () => org.apache.kafka.streams.processor.Processor[K, V],
    stateStoreNames: String*
  ): Unit = {
    val processorSupplierJ: org.apache.kafka.streams.processor.ProcessorSupplier[K, V] = () => processorSupplier()
    inner.process(processorSupplierJ, stateStoreNames: _*)
  }

  /**
   * Process all records in this stream, one record at a time, by applying a `Processor` (provided by the given
   * `processorSupplier`).
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Processor`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * Note that this overload takes a ProcessorSupplier instead of a Function to avoid post-erasure ambiguity with
   * the older (deprecated) overload.
   *
   * @param processorSupplier a supplier for [[org.apache.kafka.streams.processor.api.Processor]]
   * @param stateStoreNames   the names of the state store used by the processor
   * @see `org.apache.kafka.streams.kstream.KStream#process`
   */
  def process(processorSupplier: ProcessorSupplier[K, V, Void, Void], stateStoreNames: String*): Unit =
    inner.process(processorSupplier, stateStoreNames: _*)

  /**
   * Process all records in this stream, one record at a time, by applying a `Processor` (provided by the given
   * `processorSupplier`).
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Processor`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param processorSupplier a function that generates a [[org.apache.kafka.streams.processor.Processor]]
   * @param named             a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames   the names of the state store used by the processor
   * @see `org.apache.kafka.streams.kstream.KStream#process`
   */
  @deprecated(since = "3.0", message = "Use process(ProcessorSupplier, String*) instead.")
  def process(
    processorSupplier: () => org.apache.kafka.streams.processor.Processor[K, V],
    named: Named,
    stateStoreNames: String*
  ): Unit = {
    val processorSupplierJ: org.apache.kafka.streams.processor.ProcessorSupplier[K, V] = () => processorSupplier()
    inner.process(processorSupplierJ, named, stateStoreNames: _*)
  }

  /**
   * Process all records in this stream, one record at a time, by applying a `Processor` (provided by the given
   * `processorSupplier`).
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Processor`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * Note that this overload takes a ProcessorSupplier instead of a Function to avoid post-erasure ambiguity with
   * the older (deprecated) overload.
   *
   * @param processorSupplier a supplier for [[org.apache.kafka.streams.processor.api.Processor]]
   * @param named             a [[Named]] config used to name the processor in the topology
   * @param stateStoreNames   the names of the state store used by the processor
   * @see `org.apache.kafka.streams.kstream.KStream#process`
   */
  def process(processorSupplier: ProcessorSupplier[K, V, Void, Void], named: Named, stateStoreNames: String*): Unit =
    inner.process(processorSupplier, named, stateStoreNames: _*)

  /**
   * Group the records by their current key into a [[KGroupedStream]]
   * <p>
   * The user can either supply the `Grouped` instance as an implicit in scope or they can also provide an implicit
   * serdes that will be converted to a `Grouped` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * val clicksPerRegion: KTable[String, Long] =
   *   userClicksStream
   *     .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
   *     .map((_, regionWithClicks) => regionWithClicks)
   *
   *     // the groupByKey gets the Grouped instance through an implicit conversion of the
   *     // serdes brought into scope through the import Serdes._ above
   *     .groupByKey
   *     .reduce(_ + _)
   *
   * // Similarly you can create an implicit Grouped and it will be passed implicitly
   * // to the groupByKey call
   * }}}
   *
   * @param grouped the instance of Grouped that gives the serdes
   * @return a [[KGroupedStream]] that contains the grouped records of the original [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#groupByKey`
   */
  def groupByKey(implicit grouped: Grouped[K, V]): KGroupedStream[K, V] =
    new KGroupedStream(inner.groupByKey(grouped))

  /**
   * Group the records of this [[KStream]] on a new key that is selected using the provided key transformation function
   * and the `Grouped` instance.
   * <p>
   * The user can either supply the `Grouped` instance as an implicit in scope or they can also provide an implicit
   * serdes that will be converted to a `Grouped` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * val textLines = streamBuilder.stream[String, String](inputTopic)
   *
   * val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)
   *
   * val wordCounts: KTable[String, Long] =
   *   textLines.flatMapValues(v => pattern.split(v.toLowerCase))
   *
   *     // the groupBy gets the Grouped instance through an implicit conversion of the
   *     // serdes brought into scope through the import Serdes._ above
   *     .groupBy((k, v) => v)
   *
   *     .count()
   * }}}
   *
   * @param selector a function that computes a new key for grouping
   * @return a [[KGroupedStream]] that contains the grouped records of the original [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#groupBy`
   */
  def groupBy[KR](selector: (K, V) => KR)(implicit grouped: Grouped[KR, V]): KGroupedStream[KR, V] =
    new KGroupedStream(inner.groupBy(selector.asKeyValueMapper, grouped))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed inner equi join with
   * serializers and deserializers supplied by the implicit `StreamJoined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.join[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed left equi join with
   * serializers and deserializers supplied by the implicit `StreamJoined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.leftJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed outer equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#outerJoin`
   */
  def outerJoin[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.outerJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KTable]]'s records using inner equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param table  the [[KTable]] to be joined with this stream
   * @param joiner a function that computes the join result for a pair of matching records
   * @param joined an implicit `Joined` instance that defines the serdes to be used to serialize/deserialize
   *               inputs and outputs of the joined streams. Instead of `Joined`, the user can also supply
   *               key serde, value serde and other value serde in implicit scope and they will be
   *               converted to the instance of `Joined` through implicit conversion
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[VT, VR](table: KTable[K, VT])(joiner: (V, VT) => VR)(implicit joined: Joined[K, V, VT]): KStream[K, VR] =
    new KStream(inner.join[VT, VR](table.inner, joiner.asValueJoiner, joined))

  /**
   * Join records of this stream with another [[KTable]]'s records using left equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param table  the [[KTable]] to be joined with this stream
   * @param joiner a function that computes the join result for a pair of matching records
   * @param joined an implicit `Joined` instance that defines the serdes to be used to serialize/deserialize
   *               inputs and outputs of the joined streams. Instead of `Joined`, the user can also supply
   *               key serde, value serde and other value serde in implicit scope and they will be
   *               converted to the instance of `Joined` through implicit conversion
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[VT, VR](table: KTable[K, VT])(joiner: (V, VT) => VR)(implicit joined: Joined[K, V, VT]): KStream[K, VR] =
    new KStream(inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner, joined))

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed inner equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV])(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(
      inner.join[GK, GV, RV](
        globalKTable,
        ((k: K, v: V) => keyValueMapper(k, v)).asKeyValueMapper,
        ((v: V, gv: GV) => joiner(v, gv)).asValueJoiner
      )
    )

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed inner equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param named          a [[Named]] config used to name the processor in the topology
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV], named: Named)(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(
      inner.join[GK, GV, RV](
        globalKTable,
        ((k: K, v: V) => keyValueMapper(k, v)).asKeyValueMapper,
        ((v: V, gv: GV) => joiner(v, gv)).asValueJoiner,
        named
      )
    )

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed left equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV])(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapper.asKeyValueMapper, joiner.asValueJoiner))

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed left equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param named          a [[Named]] config used to name the processor in the topology
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *         one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV], named: Named)(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapper.asKeyValueMapper, joiner.asValueJoiner, named))

  /**
   * Merge this stream and the given stream into one larger stream.
   * <p>
   * There is no ordering guarantee between records from this `KStream` and records from the provided `KStream`
   * in the merged stream. Relative order is preserved within each input stream though (ie, records within
   * one input stream are processed in order).
   *
   * @param stream a stream which is to be merged into this stream
   * @return a merged stream containing all records from this and the provided [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#merge`
   */
  def merge(stream: KStream[K, V]): KStream[K, V] =
    new KStream(inner.merge(stream.inner))

  /**
   * Merge this stream and the given stream into one larger stream.
   * <p>
   * There is no ordering guarantee between records from this `KStream` and records from the provided `KStream`
   * in the merged stream. Relative order is preserved within each input stream though (ie, records within
   * one input stream are processed in order).
   *
   * @param named  a [[Named]] config used to name the processor in the topology
   * @param stream a stream which is to be merged into this stream
   * @return a merged stream containing all records from this and the provided [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#merge`
   */
  def merge(stream: KStream[K, V], named: Named): KStream[K, V] =
    new KStream(inner.merge(stream.inner, named))

  /**
   * Perform an action on each record of `KStream`.
   * <p>
   * Peek is a non-terminal operation that triggers a side effect (such as logging or statistics collection)
   * and returns an unchanged stream.
   *
   * @param action an action to perform on each record
   * @see `org.apache.kafka.streams.kstream.KStream#peek`
   */
  def peek(action: (K, V) => Unit): KStream[K, V] =
    new KStream(inner.peek(action.asForeachAction))

  /**
   * Perform an action on each record of `KStream`.
   * <p>
   * Peek is a non-terminal operation that triggers a side effect (such as logging or statistics collection)
   * and returns an unchanged stream.
   *
   * @param action an action to perform on each record
   * @param named  a [[Named]] config used to name the processor in the topology
   * @see `org.apache.kafka.streams.kstream.KStream#peek`
   */
  def peek(action: (K, V) => Unit, named: Named): KStream[K, V] =
    new KStream(inner.peek(action.asForeachAction, named))
}
