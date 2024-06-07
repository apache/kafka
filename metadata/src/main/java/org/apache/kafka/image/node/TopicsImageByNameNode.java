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
import org.apache.kafka.image.TopicsImage;

import java.util.Collection;


public class TopicsImageByNameNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "byName";

    /**
     * The topics image.
     */
    private final TopicsImage image;

    public TopicsImageByNameNode(TopicsImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return image.topicsByName().keySet();
    }

    @Override
    public MetadataNode child(String name) {
        TopicImage topicImage = image.topicsByName().get(name);
        if (topicImage == null) return null;
        return new TopicImageNode(topicImage);
    }
}
