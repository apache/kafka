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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.type.Types;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Test;

import java.lang.reflect.Type;

import static org.junit.Assert.assertEquals;

public class AbstractStreamTest {

    private static class TestTransformer implements Transformer<Integer, Long, KeyValue<String, Short>> {
        @Override
        public void init(ProcessorContext context){
        }

        @Override
        public KeyValue<String, Short> transform(Integer key, Long value) {
            return null;
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
        }
    }

    private static class TestTransformerSupplier implements TransformerSupplier<Integer, Long, KeyValue<String, Short>> {
        public TestTransformer get() {
            return new TestTransformer();
        }
    };

    @Test
    public void testResolveReturnType() {
        Type returnType;

        KeyValueMapper kvMapper = new KeyValueMapper<Integer, Long, String>() {
            @Override
            public String apply(Integer key, Long value) {
                return "a";
            }
        };

        returnType = AbstractStream.resolveReturnType(kvMapper);

        assertEquals((Type) String.class, returnType);

        ValueMapper vMapper = new ValueMapper<Long, String>() {
            @Override
            public String apply(Long value) {
                return "a";
            }
        };

        returnType = AbstractStream.resolveReturnType(vMapper);

        assertEquals((Type) String.class, returnType);

        ValueJoiner vJoiner = new ValueJoiner<Integer, Integer, Long>() {
            @Override
            public Long apply(Integer val1, Integer val2) {
                return 0L;
            }
        };

        returnType = AbstractStream.resolveReturnType(vJoiner);

        assertEquals((Type) Long.class, returnType);
    }

    @Test
    public void testResolveReturnTypeFromSupplier() throws Exception {
        Type returnType;

        TestTransformerSupplier transformerSupplier = new TestTransformerSupplier();

        returnType = AbstractStream.resolveReturnType(Transformer.class, "transform", transformerSupplier);

        assertEquals(Types.type(KeyValue.class, String.class, Short.class), returnType);
    }

}
