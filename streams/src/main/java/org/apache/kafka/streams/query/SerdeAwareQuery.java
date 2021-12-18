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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * Marker interface that interactive queries (see {@link
 *  * org.apache.kafka.streams.KafkaStreams#query(StateQueryRequest)})
 *  may implement if they need access to serdes for their execution. Kafka Streams
 *  will inject the serdes before passing the query into the state store.
 * <p>
 * See also {@link Query}
 *
 * @param <R> The type of the result returned by this query.
 */
public interface SerdeAwareQuery<K, V, R> extends Query<R> {
    public static final class QuerySerdes<K, V> {

        private final String topic;
        private final Serializer<K> keySerializer;
        private final Deserializer<V> valueDeserializer;

        public QuerySerdes(final String topic,
                           final Serializer<K> keySerializer,
                           final Deserializer<V> valueDeserializer) {
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valueDeserializer = valueDeserializer;
        }

        public String getTopic() {
            return topic;
        }

        public Serializer<K> getKeySerializer() {
            return keySerializer;
        }

        public Deserializer<V> getValueDeserializer() {
            return valueDeserializer;
        }
    }

    void setSerdes(QuerySerdes<K, V> serdes);
    QuerySerdes<K, V> getSerdes();
}
