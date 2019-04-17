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

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ListSerde<T> implements Serde<List<T>> {

    private final Serde<List<T>> inner;

    public ListSerde(Serde<T> serde, Comparator<T> comparator) {
        inner = Serdes.serdeFrom(new ListSerializer<T>(serde.serializer()),
                new ListDeserializer<T>(serde.deserializer(), comparator));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

    @Override
    public Serializer<List<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<List<T>> deserializer() {
        return inner.deserializer();
    }

}
