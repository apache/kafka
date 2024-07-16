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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.KeyValue;

import java.util.Objects;

/**
 * The {@code Predicate} interface represents a predicate (boolean-valued function) of a {@link KeyValue} pair.
 * This is a stateless record-by-record operation, i.e, {@link #test(Object, Object)} is invoked individually for each
 * record of a stream.
 *
 * @param <K> key type
 * @param <V> value type
 * @see KStream#filter(Predicate)
 * @see KStream#filterNot(Predicate)
 * @see BranchedKStream#branch(Predicate)
 * @see KTable#filter(Predicate)
 * @see KTable#filterNot(Predicate)
 */
public interface Predicate<K, V> {

    /**
     * Test if the record with the given key and value satisfies the predicate.
     *
     * @param key   the key of the record
     * @param value the value of the record
     * @return {@code true} if the {@link KeyValue} pair satisfies the predicate&mdash;{@code false} otherwise
     */
    boolean test(final K key, final V value);

    /**
     * Returns a composed predicate that represents a short-circuiting logical AND of this predicate and another.
     * When evaluating the composed predicate, if this predicate is false, then the other predicate is not evaluated.
     * Any exceptions thrown during evaluation of either predicate are relayed to the caller; if evaluation of this
     * predicate throws an exception, the other predicate will not be evaluated.
     * @param other a predicate that will be logically-ANDed with this predicate
     * @return a composed predicate that represents the short-circuiting logical AND of this predicate and the other predicate
     * @throws NullPointerException if other is null
     */
    default Predicate<K, V> and(Predicate<? super K, ? super V> other) {
        Objects.requireNonNull(other);
        return (k, v) -> this.test(k, v) && other.test(k, v);
    }

    /**
     * Returns a predicate that represents the logical negation of this predicate.
     * @return a predicate that represents the logical negation of this predicate
     */
    default Predicate<K, V> negate() {
        return (k, v) -> !this.test(k, v);
    }

    /**
     * Returns a composed predicate that represents a short-circuiting logical OR of this predicate and another. When
     * evaluating the composed predicate, if this predicate is true, then the other predicate is not evaluated.
     * Any exceptions thrown during evaluation of either predicate are relayed to the caller; if evaluation of this
     * predicate throws an exception, the other predicate will not be evaluated.
     * @param other a predicate that will be logically-ORed with this predicate
     * @return a composed predicate that represents the short-circuiting logical OR of this predicate and the other predicate
     * @throws NullPointerException if other is null
     */
    default Predicate<K, V> or(Predicate<? super K, ? super V> other) {
        Objects.requireNonNull(other);
        return (k, v) -> this.test(k, v) || other.test(k, v);
    }

    /**
     * Returns a predicate that is the negation of the supplied predicate. This is accomplished by returning result
     * of the calling target.negate().
     * @param target predicate to negate
     * @param <K> key type
     * @param <V> value type
     * @return a predicate that negates the results of the supplied predicate
     * @throws NullPointerException if target is null
     */
    static <K, V> Predicate<K, V> not(Predicate<K, V> target) {
        Objects.requireNonNull(target);
        return target.negate();
    }
}
