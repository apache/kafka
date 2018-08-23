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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import scala.collection.JavaConverters._
import java.lang.{Iterable => JIterable}

/**
 * Implicit classes that offer conversions of Scala function literals to
 * SAM (Single Abstract Method) objects in Java. These make the Scala APIs much
 * more expressive, with less boilerplate and more succinct.
 * <p>
 * For Scala 2.11, most of these conversions need to be invoked explicitly, as Scala 2.11 does not
 * have full support for SAM types.
 */
private[scala] object FunctionsCompatConversions {

  implicit class ForeachActionFromFunction[K, V](val p: (K, V) => Unit) extends AnyVal {
    def asForeachAction: ForeachAction[K, V] = new ForeachAction[K, V] {
      override def apply(key: K, value: V): Unit = p(key, value)
    }
  }

  implicit class PredicateFromFunction[K, V](val p: (K, V) => Boolean) extends AnyVal {
    def asPredicate: Predicate[K, V] = new Predicate[K, V] {
      override def test(key: K, value: V): Boolean = p(key, value)
    }
  }

  implicit class MapperFromFunction[T, U, VR](val f: (T, U) => VR) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[T, U, VR] = new KeyValueMapper[T, U, VR] {
      override def apply(key: T, value: U): VR = f(key, value)
    }
    def asValueJoiner: ValueJoiner[T, U, VR] = new ValueJoiner[T, U, VR] {
      override def apply(value1: T, value2: U): VR = f(value1, value2)
    }
  }

  implicit class KeyValueMapperFromFunction[K, V, KR, VR](val f: (K, V) => (KR, VR)) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = new KeyValueMapper[K, V, KeyValue[KR, VR]] {
      override def apply(key: K, value: V): KeyValue[KR, VR] = {
        val (kr, vr) = f(key, value)
        KeyValue.pair(kr, vr)
      }
    }
  }

  implicit class ValueMapperFromFunction[V, VR](val f: V => VR) extends AnyVal {
    def asValueMapper: ValueMapper[V, VR] = new ValueMapper[V, VR] {
      override def apply(value: V): VR = f(value)
    }
  }

  implicit class FlatValueMapperFromFunction[V, VR](val f: V => Iterable[VR]) extends AnyVal {
    def asValueMapper: ValueMapper[V, JIterable[VR]] = new ValueMapper[V, JIterable[VR]] {
      override def apply(value: V): JIterable[VR] = f(value).asJava
    }
  }

  implicit class ValueMapperWithKeyFromFunction[K, V, VR](val f: (K, V) => VR) extends AnyVal {
    def asValueMapperWithKey: ValueMapperWithKey[K, V, VR] = new ValueMapperWithKey[K, V, VR] {
      override def apply(readOnlyKey: K, value: V): VR = f(readOnlyKey, value)
    }
  }

  implicit class FlatValueMapperWithKeyFromFunction[K, V, VR](val f: (K, V) => Iterable[VR]) extends AnyVal {
    def asValueMapperWithKey: ValueMapperWithKey[K, V, JIterable[VR]] = new ValueMapperWithKey[K, V, JIterable[VR]] {
      override def apply(readOnlyKey: K, value: V): JIterable[VR] = f(readOnlyKey, value).asJava
    }
  }

  implicit class AggregatorFromFunction[K, V, VA](val f: (K, V, VA) => VA) extends AnyVal {
    def asAggregator: Aggregator[K, V, VA] = new Aggregator[K, V, VA] {
      override def apply(key: K, value: V, aggregate: VA): VA = f(key, value, aggregate)
    }
  }

  implicit class MergerFromFunction[K, VR](val f: (K, VR, VR) => VR) extends AnyVal {
    def asMerger: Merger[K, VR] = new Merger[K, VR] {
      override def apply(aggKey: K, aggOne: VR, aggTwo: VR): VR = f(aggKey, aggOne, aggTwo)
    }
  }

  implicit class ReducerFromFunction[V](val f: (V, V) => V) extends AnyVal {
    def asReducer: Reducer[V] = new Reducer[V] {
      override def apply(value1: V, value2: V): V = f(value1, value2)
    }
  }

  implicit class InitializerFromFunction[VA](val f: () => VA) extends AnyVal {
    def asInitializer: Initializer[VA] = new Initializer[VA] {
      override def apply(): VA = f()
    }
  }

  implicit class TransformerSupplierFromFunction[K, V, VO](val f: () => Transformer[K, V, VO]) extends AnyVal {
    def asTransformerSupplier: TransformerSupplier[K, V, VO] = new TransformerSupplier[K, V, VO] {
      override def get(): Transformer[K, V, VO] = f()
    }
  }
}
