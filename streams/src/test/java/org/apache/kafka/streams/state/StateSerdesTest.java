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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StateSerdesTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfTopicNameIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes(null, byte[].class, byte[].class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfKeyClassIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", null, byte[].class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfValueClassIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", byte[].class, null);
    }

    @Test
    public void shouldReturnSerdesForBuiltInKeyAndValueTypesForBuiltinTypes() {
        final Class[] supportedBuildInTypes = new Class[] {
            String.class,
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            byte[].class,
            ByteBuffer.class,
            Bytes.class
        };

        for (final Class keyClass : supportedBuildInTypes) {
            for (final Class valueClass : supportedBuildInTypes) {
                Assert.assertNotNull(StateSerdes.withBuiltinTypes("anyName", keyClass, valueClass));
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownKeyTypeForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", Class.class, byte[].class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownValueTypeForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", byte[].class, Class.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfTopicNameIsNull() {
        new StateSerdes<>(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfKeyClassIsNull() {
        new StateSerdes<>("anyName", null, Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfValueClassIsNull() {
        new StateSerdes<>("anyName", Serdes.ByteArray(), null);
    }

}
