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
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The Converter interface provides support for translating between Copycat's runtime data format
 * and the "native" runtime format used by the serialization layer. This is used to translate
 * two types of data: records and offsets. The (de)serialization is performed by a separate
 * component -- the producer or consumer serializer or deserializer for records or a Copycat
 * serializer or deserializer for offsets.
 */
@InterfaceStability.Unstable
public interface Converter<T> {

    /**
     * Convert a Copycat data object to a native object for serialization.
     * @param value
     * @return
     */
    T fromCopycatData(Object value);

    /**
     * Convert a native object to a Copycat data object.
     * @param value
     * @return
     */
    Object toCopycatData(T value);
}
