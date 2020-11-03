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
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.errors.SerializationException;

/**
 * Serializer/Deserializer interface for {@link KafkaPrincipal} for the the purpose of inter-broker forwarding.
 * Any serialization/deserialization failure should raise a {@link SerializationException} to be consistent.
 */
public interface KafkaPrincipalSerde {

    /**
     * Serialize a {@link KafkaPrincipal} into byte array.
     *
     * @param principal principal to be serialized
     * @return serialized bytes
     * @throws SerializationException
     */
    byte[] serialize(KafkaPrincipal principal) throws SerializationException;

    /**
     * Deserialize a {@link KafkaPrincipal} from byte array.
     * @param bytes byte array to be deserialized
     * @return the deserialized principal
     * @throws SerializationException
     */
    KafkaPrincipal deserialize(byte[] bytes) throws SerializationException;
}
