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

import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;


public final class TopicImageNode implements MetadataNode {
    /**
     * The topic image.
     */
    private final TopicImage image;

    public TopicImageNode(TopicImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        childNames.add("name");
        childNames.add("id");
        for (Integer partitionId : image.partitions().keySet()) {
            childNames.add(partitionId.toString());
        }
        if (image.mirrorName() != null) {
            childNames.add("mirrorName");
            childNames.add("mirrorDesiredState");
        }
        return childNames;
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals("name")) {
            return new MetadataLeafNode(image.name());
        } else if (name.equals("id")) {
            return new MetadataLeafNode(image.id().toString());
        } else if (name.equals("mirrorName")) {
            return Optional.ofNullable(image.mirrorName()).map(MetadataLeafNode::new).orElse(null);
        } else if (name.equals("mirrorDesiredState")) {
            return Optional.ofNullable(image.mirrorName())
                    .map(_unused -> new MetadataLeafNode(MirrorPartitionState.nameOf(image.desiredMirrorState())))
                    .orElse(null);
        } else {
            int partitionId;
            try {
                partitionId = Integer.parseInt(name);
            } catch (NumberFormatException e) {
                return null;
            }
            PartitionRegistration registration = image.partitions().get(partitionId);
            if (registration == null) return null;
            return new MetadataLeafNode(registration.toString());
        }
    }
}
