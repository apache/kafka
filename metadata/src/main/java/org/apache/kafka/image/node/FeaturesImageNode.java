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

import org.apache.kafka.image.FeaturesImage;

import java.util.ArrayList;
import java.util.Collection;


public class FeaturesImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "features";

    /**
     * The name of the metadata version child node.
     */
    public static final String METADATA_VERSION = "metadataVersion";

    /**
     * The name of the zk migration state child node.
     */
    public static final String ZK_MIGRATION_STATE = "zkMigrationState";

    /**
     * The prefix to put before finalized feature children.
     */
    public static final String FINALIZED_PREFIX = "finalized_";

    /**
     * The features image.
     */
    private final FeaturesImage image;

    public FeaturesImageNode(FeaturesImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        childNames.add(METADATA_VERSION);
        childNames.add(ZK_MIGRATION_STATE);
        for (String featureName : image.finalizedVersions().keySet()) {
            childNames.add(FINALIZED_PREFIX + featureName);
        }
        return childNames;
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals(METADATA_VERSION)) {
            return new MetadataLeafNode(image.metadataVersion().toString());
        } else if (name.startsWith(FINALIZED_PREFIX)) {
            String key = name.substring(FINALIZED_PREFIX.length());
            return new MetadataLeafNode(
                    image.finalizedVersions().getOrDefault(key, (short) 0).toString());
        } else {
            return null;
        }
    }
}
