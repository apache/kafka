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

import org.apache.kafka.common.annotation.InterfaceStability;

@InterfaceStability.Unstable
public interface ValueJoinerWithKey<K, V1, V2, VR> {

    /**
     * Return a joined value consisting of {@code value1} and {@code value2} with read-only key.
     *
     * @param key the join key
     * @param value1 the first value for joining
     * @param value2 the second value for joining
     * @return the joined value
     */
    VR apply(final K key, final V1 value1, final V2 value2);
}
