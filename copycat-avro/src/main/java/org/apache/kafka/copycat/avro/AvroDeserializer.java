/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.storage.OffsetDeserializer;

import java.util.Map;

public class AvroDeserializer extends AbstractKafkaAvroDeserializer implements OffsetDeserializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public Object deserializeOffset(String connector, byte[] data) {
        // TODO: Support schema projection
        return deserialize(data);
    }

    @Override
    public Object deserializeOffset(String connector, byte[] data, Schema schema) {
        // TODO: Support schema projection
        return deserialize(data);
    }

    @Override
    public void close() {

    }
}
