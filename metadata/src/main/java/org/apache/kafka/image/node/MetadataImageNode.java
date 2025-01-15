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

package org.apache.kafka.image.node;

import org.apache.kafka.image.MetadataImage;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;


public class MetadataImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "image";

    /**
     * The metadata image.
     */
    private final MetadataImage image;

    private static final Map<String, Function<MetadataImage, MetadataNode>> CHILDREN = Map.of(
        ProvenanceNode.NAME, image -> new ProvenanceNode(image.provenance()),
        FeaturesImageNode.NAME, image -> new FeaturesImageNode(image.features()),
        ClusterImageNode.NAME, image -> new ClusterImageNode(image.cluster()),
        TopicsImageNode.NAME, image -> new TopicsImageNode(image.topics()),
        ConfigurationsImageNode.NAME, image -> new ConfigurationsImageNode(image.configs()),
        ClientQuotasImageNode.NAME, image -> new ClientQuotasImageNode(image.clientQuotas()),
        ProducerIdsImageNode.NAME, image -> new ProducerIdsImageNode(image.producerIds()),
        AclsImageNode.NAME, image -> new AclsImageByIdNode(image.acls()),
        ScramImageNode.NAME, image -> new ScramImageNode(image.scram()),
        DelegationTokenImageNode.NAME, image -> new DelegationTokenImageNode(image.delegationTokens())
    );

    public MetadataImageNode(MetadataImage image) {
        this.image = image;
    }

    public MetadataImage image() {
        return image;
    }

    @Override
    public Collection<String> childNames() {
        return CHILDREN.keySet();
    }

    @Override
    public MetadataNode child(String name) {
        return CHILDREN.getOrDefault(name, __ -> null).apply(image);
    }
}
