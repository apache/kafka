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

package org.apache.kafka.test;

import org.apache.kafka.clients.processor.KafkaSource;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;

public class MockSource<K, V> extends KafkaSource<K, V> {

    public Deserializer<? extends K> keyDeserializer;
    public Deserializer<? extends V> valDeserializer;

    public int numReceived = 0;
    public ArrayList<K> keys = new ArrayList<>();
    public ArrayList<V> values = new ArrayList<>();

    public MockSource(Deserializer<? extends K> keyDeserializer, Deserializer<? extends V> valDeserializer) {
        super(keyDeserializer, valDeserializer);

        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
    }

    @Override
    public void process(K key, V value) {
        this.numReceived++;
        this.keys.add(key);
        this.values.add(value);
    }
}
