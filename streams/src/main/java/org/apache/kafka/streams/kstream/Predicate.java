/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * The {@link Predicate} interface represents a predicate (boolean-valued function) of a key-value pair.
 *
 * @param <K>   key type
 * @param <V>   value type
 */
public interface Predicate<K, V> {

    /**
     * Test if the record with the given key and value satisfies the predicate.
     *
     * @param key    the key of the record
     * @param value  the value of the record
     * @return       return {@code true} if the key-value pair satisfies the predicate&mdash;{@code false} otherwise
     */
    boolean test(K key, V value);
}
