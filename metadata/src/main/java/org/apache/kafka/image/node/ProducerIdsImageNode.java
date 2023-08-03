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

import org.apache.kafka.image.ProducerIdsImage;

import java.util.Collection;
import java.util.Collections;


public class ProducerIdsImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "producerIds";

    /**
     * The producer IDs image.
     */
    private final ProducerIdsImage image;

    public ProducerIdsImageNode(ProducerIdsImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return Collections.singletonList("nextProducerId");
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals("nextProducerId")) {
            return new MetadataLeafNode(image.nextProducerId() + "");
        } else {
            return null;
        }
    }
}
