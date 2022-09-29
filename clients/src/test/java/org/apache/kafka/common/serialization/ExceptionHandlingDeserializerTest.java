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
package org.apache.kafka.common.serialization;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionHandlingDeserializerTest {

    public static final IllegalStateException EXCEPTION = new IllegalStateException();

    @Test
    public void ok() {
        ExceptionHandlingDeserializer<String> sut = new ExceptionHandlingDeserializer<>();
        Map<String, String> configs = new HashMap<>();
        configs.put("exception.handling.deserializer.delegate", OKDeserializer.class.getName());
        sut.configure(configs, false);
        assertEquals(new ExceptionHandlingDeserializer.Result<>("ok", null), sut.deserialize(null, null));
        assertEquals(new ExceptionHandlingDeserializer.Result<>("ok", null), sut.deserialize(null, null, null));
    }

    @Test
    public void boom() {
        ExceptionHandlingDeserializer<String> sut = new ExceptionHandlingDeserializer<>();
        Map<String, String> configs = new HashMap<>();
        configs.put("exception.handling.deserializer.delegate", OKDeserializer.class.getName());
        assertEquals(new ExceptionHandlingDeserializer.Result<String>(null, EXCEPTION), sut.deserialize(null, null));
        assertEquals(new ExceptionHandlingDeserializer.Result<String>(null, EXCEPTION), sut.deserialize(null, null, null));
    }

    static class OKDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(String topic, byte[] data) {
            return "ok";
        }
    }

    static class BoomDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(String topic, byte[] data) {
            throw EXCEPTION;
        }
    }
}