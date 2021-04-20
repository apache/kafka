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
package org.apache.kafka.server.log.remote.metadata.storage;

import java.util.Objects;

/**
 * The context associated with the record in remote log metadata topic. This contains api-key, and the payload object.
 * <br>
 * <p>
 * For example:
 * Remote log segment metadata record will have
 * <pre>
 * <ul>
 *     <li>api key as: {@link org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord#apiKey()} </li>
 *     <li>payload as: {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata} </li>
 * </ul>
 * </pre>
 * <p>
 *
 * You can read more details in <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage#KIP405:KafkaTieredStorage-MessageFormat">KIP-405</a>
 */
public class RemoteLogMetadataContext {
    private final short apiKey;
    private final Object payload;

    public RemoteLogMetadataContext(short apiKey, Object payload) {
        this.apiKey = apiKey;
        this.payload = payload;
    }

    public short apiKey() {
        return apiKey;
    }

    public Object payload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogMetadataContext that = (RemoteLogMetadataContext) o;
        return apiKey == that.apiKey && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiKey, payload);
    }

    @Override
    public String toString() {
        return "RemoteLogMetadataContext{" +
                "apiKey=" + apiKey +
                ", payload=" + payload +
                '}';
    }
}
