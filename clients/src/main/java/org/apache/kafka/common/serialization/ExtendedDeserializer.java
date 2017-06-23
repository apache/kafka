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

import java.util.Map;

import org.apache.kafka.common.header.Headers;

public interface ExtendedDeserializer<T> extends Deserializer<T> {
    
    T deserialize(String topic, Headers headers, byte[] data);

    class Wrapper<T> implements ExtendedDeserializer<T> {

        private final Deserializer<T> deserializer;

        public Wrapper(Deserializer<T> deserializer) {
            this.deserializer = deserializer;
        }


        @Override
        public T deserialize(String topic, Headers headers, byte[] data) {
            return deserialize(topic, data);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            return deserializer.deserialize(topic, data);
        }

        @Override
        public void close() {
            deserializer.close();
        }
    }
}
