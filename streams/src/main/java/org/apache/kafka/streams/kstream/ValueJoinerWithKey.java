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


/**
 * The {@code ValueJoinerWithKey} interface for joining two values into a new value of arbitrary type.
 * This interface provides access to a read-only key that the user should not modify as this would lead to
 * undefined behavior
 * This is a stateless operation, i.e, {@link #apply(Object, Object, Object)} is invoked individually for each joining
 * record-pair of a {@link KStream}-{@link KStream}, {@link KStream}-{@link KTable}, or {@link KTable}-{@link KTable}
 * join.
 *
 * @param <K1> key value type
 * @param <V1> first value type
 * @param <V2> second value type
 * @param <VR> joined value type
 * @see KStream#join(KStream, ValueJoinerWithKey, JoinWindows)
 * @see KStream#join(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
 * @see KStream#leftJoin(KStream, ValueJoinerWithKey, JoinWindows)
 * @see KStream#leftJoin(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
 * @see KStream#outerJoin(KStream, ValueJoinerWithKey, JoinWindows)
 * @see KStream#outerJoin(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
 * @see KStream#join(KTable, ValueJoinerWithKey)
 * @see KStream#join(KTable, ValueJoinerWithKey, Joined)
 * @see KStream#leftJoin(KTable, ValueJoinerWithKey)
 * @see KStream#leftJoin(KTable, ValueJoinerWithKey, Joined)
 * @see KStream#join(GlobalKTable, KeyValueMapper, ValueJoinerWithKey) 
 * @see KStream#join(GlobalKTable, KeyValueMapper, ValueJoinerWithKey, Named)
 * @see KStream#leftJoin(GlobalKTable, KeyValueMapper, ValueJoinerWithKey)
 * @see KStream#leftJoin(GlobalKTable, KeyValueMapper, ValueJoinerWithKey, Named)
 */
public interface ValueJoinerWithKey<K1, V1, V2, VR> {

    /**
     * Return a joined value consisting of {@code readOnlyKey}, {@code value1} and {@code value2}.
     *
     * @param readOnlyKey the key
     * @param value1 the first value for joining
     * @param value2 the second value for joining
     * @return the joined value
     */
    VR apply(final K1 readOnlyKey, final V1 value1, final V2 value2);
}
