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
 * The {@link Aggregator} interface for aggregating values of the given key.
 *
 * @param <K>   key type
 * @param <V>   original value type
 * @param <T>   aggregate value type
 */
public interface Aggregator<K, V, T> {

    /**
     * Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
     *
     * @param aggKey     the key of the record
     * @param value      the value of the record
     * @param aggregate  the current aggregate value
     * @return           the new aggregate value
     */
    T apply(K aggKey, V value, T aggregate);
}
