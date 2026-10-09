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
package org.apache.kafka.metadata;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.List;

public final class MetadataCacheFixtures {

    private MetadataCacheFixtures() {
    }

    public static MetadataCache createCache() {
        return new KRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_0);
    }

    public static void updateCache(MetadataCache cache, List<ApiMessage> records) {
        if (cache instanceof KRaftMetadataCache c) {
            MetadataImage image = c.currentImage();
            MetadataImage partialImage = new MetadataImage(
                new MetadataProvenance(100L, 10, 1000L, true),
                image.features(),
                image.cluster(),
                image.topics(),
                image.configs(),
                image.clientQuotas(),
                image.producerIds(),
                image.acls(),
                image.scram(),
                image.delegationTokens()
            );
            MetadataDelta delta = new MetadataDelta.Builder().setImage(partialImage).build();
            for (ApiMessage record : records) {
                delta.replay(record);
            }
            c.setImage(delta.apply(new MetadataProvenance(100L, 10, 1000L, true)));
        } else {
            throw new RuntimeException("Unsupported cache type");
        }
    }
}
