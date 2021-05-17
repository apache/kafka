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
package org.apache.kafka.server.log.remote.metadata.storage.serialization;

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;

/**
 * This interface is about transforming {@link RemoteLogMetadata} objects into the respective {@link ApiMessageAndVersion} or vice versa.
 * <p></p>
 * Those metadata objects can be {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata},
 * {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate}, or {@link org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata}.
 * <p>
 * @param <T> metadata type.
 *
 * @see RemoteLogSegmentMetadataTransform
 * @see RemoteLogSegmentMetadataUpdateTransform
 * @see RemotePartitionDeleteMetadataTransform
 */
public interface RemoteLogMetadataTransform<T extends RemoteLogMetadata> {

    /**
     * Transforms the given {@code metadata} object into the respective {@code ApiMessageAndVersion} object.
     *
     * @param metadata metadata object to be transformed.
     * @return transformed {@code ApiMessageAndVersion} object.
     */
    ApiMessageAndVersion toApiMessageAndVersion(T metadata);

    /**
     * Return the metadata object transformed from the given {@code apiMessageAndVersion}.
     *
     * @param apiMessageAndVersion ApiMessageAndVersion object to be transformed.
     * @return transformed {@code T} metadata object.
     */
    T fromApiMessageAndVersion(ApiMessageAndVersion apiMessageAndVersion);

}
