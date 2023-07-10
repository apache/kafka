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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public class MetadataImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "image";

    /**
     * The metadata image.
     */
    private final MetadataImage image;

    private final static Map<String, Function<MetadataImage, MetadataNode>> CHILDREN;

    static {
        Map<String, Function<MetadataImage, MetadataNode>> children = new HashMap<>();
        children.put(ProvenanceNode.NAME, image -> new ProvenanceNode(image.provenance()));
        children.put(FeaturesImageNode.NAME, image -> new FeaturesImageNode(image.features()));
        children.put(ClusterImageNode.NAME, image -> new ClusterImageNode(image.cluster()));
        children.put(TopicsImageNode.NAME, image -> new TopicsImageNode(image.topics()));
        children.put(ConfigurationsImageNode.NAME, image -> new ConfigurationsImageNode(image.configs()));
        children.put(ClientQuotasImageNode.NAME, image -> new ClientQuotasImageNode(image.clientQuotas()));
        children.put(ProducerIdsImageNode.NAME, image -> new ProducerIdsImageNode(image.producerIds()));
        children.put(AclsImageNode.NAME, image -> new AclsImageByIdNode(image.acls()));
        children.put(ScramImageNode.NAME, image -> new ScramImageNode(image.scram()));
        children.put(DelegationTokenImageNode.NAME, image -> new DelegationTokenImageNode(image.delegationTokens()));
        CHILDREN = Collections.unmodifiableMap(children);
    }

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
