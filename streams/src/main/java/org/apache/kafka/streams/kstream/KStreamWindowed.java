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
 * KStreamWindowed is an abstraction of a stream of key-value pairs with a window.
 */
public interface KStreamWindowed<K, V> extends KStream<K, V> {

    /**
     * Creates a new stream by joining this windowed stream with the other windowed stream.
     * Each element arrived from either of the streams is joined with elements in a window of each other.
     * The resulting values are computed by applying a joiner.
     *
     * @param other  the other windowed stream
     * @param joiner ValueJoiner
     * @param <V1>   the value type of the other stream
     * @param <V2>   the value type of the new stream
     * @return KStream
     */
    <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V, V1, V2> joiner);

}
