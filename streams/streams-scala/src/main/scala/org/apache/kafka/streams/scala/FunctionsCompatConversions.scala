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
import scala.jdk.CollectionConverters._
import java.lang.{Iterable => JIterable}

import org.apache.kafka.streams.processor.ProcessorContext

/**
 * Implicit classes that offer conversions of Scala function literals to
 * SAM (Single Abstract Method) objects in Java. These make the Scala APIs much
 * more expressive, with less boilerplate and more succinct.
 */
private[scala] object FunctionsCompatConversions {

  implicit class ForeachActionFromFunction[K, V](val p: (K, V) => Unit) extends AnyVal {
    def asForeachAction: ForeachAction[K, V] = (key: K, value: V) => p(key, value)
  }

  implicit class PredicateFromFunction[K, V](val p: (K, V) => Boolean) extends AnyVal {
    def asPredicate: Predicate[K, V] = (key: K, value: V) => p(key, value)
  }

  implicit class MapperFromFunction[T, U, VR](val f: (T, U) => VR) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[T, U, VR] = (key: T, value: U) => f(key, value)
    def asValueJoiner: ValueJoiner[T, U, VR] = (value1: T, value2: U) => f(value1, value2)
  }

  implicit class KeyValueMapperFromFunction[K, V, KR, VR](val f: (K, V) => (KR, VR)) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = (key: K, value: V) => {
      val (kr, vr) = f(key, value)
      KeyValue.pair(kr, vr)
    }
  }

  implicit class FunctionFromFunction[V, VR](val f: V => VR) extends AnyVal {
    def asJavaFunction: java.util.function.Function[V, VR] = (value: V) => f(value)
  }

  implicit class ValueMapperFromFunction[V, VR](val f: V => VR) extends AnyVal {
    def asValueMapper: ValueMapper[V, VR] = (value: V) => f(value)
  }

  implicit class FlatValueMapperFromFunction[V, VR](val f: V => Iterable[VR]) extends AnyVal {
    def asValueMapper: ValueMapper[V, JIterable[VR]] = (value: V) => f(value).asJava
  }

  implicit class ValueMapperWithKeyFromFunction[K, V, VR](val f: (K, V) => VR) extends AnyVal {
    def asValueMapperWithKey: ValueMapperWithKey[K, V, VR] = (readOnlyKey: K, value: V) => f(readOnlyKey, value)
  }

  implicit class FlatValueMapperWithKeyFromFunction[K, V, VR](val f: (K, V) => Iterable[VR]) extends AnyVal {
    def asValueMapperWithKey: ValueMapperWithKey[K, V, JIterable[VR]] =
      (readOnlyKey: K, value: V) => f(readOnlyKey, value).asJava
  }

  implicit class AggregatorFromFunction[K, V, VA](val f: (K, V, VA) => VA) extends AnyVal {
    def asAggregator: Aggregator[K, V, VA] = (key: K, value: V, aggregate: VA) => f(key, value, aggregate)
  }

  implicit class MergerFromFunction[K, VR](val f: (K, VR, VR) => VR) extends AnyVal {
    def asMerger: Merger[K, VR] = (aggKey: K, aggOne: VR, aggTwo: VR) => f(aggKey, aggOne, aggTwo)
  }

  implicit class ReducerFromFunction[V](val f: (V, V) => V) extends AnyVal {
    def asReducer: Reducer[V] = (value1: V, value2: V) => f(value1, value2)
  }

  implicit class InitializerFromFunction[VA](val f: () => VA) extends AnyVal {
    def asInitializer: Initializer[VA] = () => f()
  }

  implicit class TransformerSupplierFromFunction[K, V, VO](val f: () => Transformer[K, V, VO]) extends AnyVal {
    def asTransformerSupplier: TransformerSupplier[K, V, VO] = () => f()
  }

  implicit class TransformerSupplierAsJava[K, V, VO](val supplier: TransformerSupplier[K, V, Iterable[VO]])
      extends AnyVal {
    def asJava: TransformerSupplier[K, V, JIterable[VO]] = () => {
      val innerTransformer = supplier.get()
      new Transformer[K, V, JIterable[VO]] {
        override def transform(key: K, value: V): JIterable[VO] = innerTransformer.transform(key, value).asJava
        override def init(context: ProcessorContext): Unit = innerTransformer.init(context)
        override def close(): Unit = innerTransformer.close()
      }
    }
  }
  implicit class ValueTransformerSupplierAsJava[V, VO](val supplier: ValueTransformerSupplier[V, Iterable[VO]])
      extends AnyVal {
    def asJava: ValueTransformerSupplier[V, JIterable[VO]] = () => {
      val innerTransformer = supplier.get()
      new ValueTransformer[V, JIterable[VO]] {
        override def transform(value: V): JIterable[VO] = innerTransformer.transform(value).asJava
        override def init(context: ProcessorContext): Unit = innerTransformer.init(context)
        override def close(): Unit = innerTransformer.close()
      }
    }
  }
  implicit class ValueTransformerSupplierWithKeyAsJava[K, V, VO](
    val supplier: ValueTransformerWithKeySupplier[K, V, Iterable[VO]]
  ) extends AnyVal {
    def asJava: ValueTransformerWithKeySupplier[K, V, JIterable[VO]] = () => {
      val innerTransformer = supplier.get()
      new ValueTransformerWithKey[K, V, JIterable[VO]] {
        override def transform(key: K, value: V): JIterable[VO] = innerTransformer.transform(key, value).asJava
        override def init(context: ProcessorContext): Unit = innerTransformer.init(context)
        override def close(): Unit = innerTransformer.close()
      }
    }
  }
}
