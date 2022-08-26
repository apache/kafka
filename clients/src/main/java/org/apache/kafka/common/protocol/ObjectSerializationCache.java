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

package org.apache.kafka.common.protocol;

import java.util.IdentityHashMap;

/**
 * The ObjectSerializationCache stores sizes and values computed during the
 * first serialization pass.  This avoids recalculating and recomputing the same
 * values during the second pass.
 *
 * It is intended to be used as part of a two-pass serialization process like:
 * ObjectSerializationCache cache = new ObjectSerializationCache();
 * message.size(version, cache);
 * message.write(version, cache);
 */
public final class ObjectSerializationCache {
    private final IdentityHashMap<Object, Object> map;

    public ObjectSerializationCache() {
        this.map = new IdentityHashMap<>();
    }

    public void setArraySizeInBytes(Object o, Integer size) {
        map.put(o, size);
    }

    public Integer getArraySizeInBytes(Object o) {
        return (Integer) map.get(o);
    }

    public void cacheSerializedValue(Object o, byte[] val) {
        map.put(o, val);
    }

    public byte[] getSerializedValue(Object o) {
        Object value = map.get(o);
        return (byte[]) value;
    }
}
