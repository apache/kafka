/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.kstream.type.internal;

import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.type.TypeException;
import org.apache.kafka.streams.kstream.type.Types;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;

import static org.junit.Assert.assertEquals;

public class ResolverTest {

    @Test
    public void testResolveReturnType() {
        Type returnType;

        KeyValueMapper kvMapper = new KeyValueMapper<Integer, Long, String>() {
            @Override
            public String apply(Integer key, Long value) {
                return "a";
            }
        };

        try {
            returnType = Resolver.resolveReturnType(KeyValueMapper.class, kvMapper.getClass());
        } catch (TypeException ex) {
            returnType = null;
        }

        assertEquals((Type) String.class, returnType);

        ValueMapper vMapper = new ValueMapper<Long, String>() {
            @Override
            public String apply(Long value) {
                return "a";
            }
        };

        try {
            returnType = Resolver.resolveReturnType(ValueMapper.class, vMapper.getClass());
        } catch (TypeException ex) {
            returnType = null;
        }

        assertEquals((Type) String.class, returnType);


        ValueJoiner vJoiner = new ValueJoiner<Integer, Integer, Long>() {
            @Override
            public Long apply(Integer val1, Integer val2) {
                return 0L;
            }
        };

        try {
            returnType = Resolver.resolveReturnType(ValueJoiner.class, vJoiner.getClass());
        } catch (TypeException ex) {
            returnType = null;
        }

        assertEquals((Type) Long.class, returnType);
    }

    @Test
    public void testKeyValueTypeFromKeyValueMapper() throws Exception {
        Type returnType;

        KeyValueMapper kvMapper = new KeyValueMapper<Integer, Long, KeyValue<String, Short>>() {
            @Override
            public KeyValue<String, Short> apply(Integer key, Long value) {
                return null;
            }
        };

        try {
            returnType = Resolver.resolveReturnType(KeyValueMapper.class, kvMapper.getClass());
        } catch (TypeException ex) {
            returnType = null;
        }

        Assert.assertEquals(Types.type(KeyValue.class, String.class, Short.class), returnType);

        Type keyType = Resolver.getKeyTypeFromKeyValueType(returnType);

        assertEquals((Type) String.class, keyType);

        Type valueType = Resolver.getValueTypeFromKeyValueType(returnType);

        assertEquals((Type) Short.class, valueType);
    }

}
